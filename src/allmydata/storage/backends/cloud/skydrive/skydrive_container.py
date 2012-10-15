
from os.path import basename, dirname, normpath, join
from datetime import datetime
import re

from zope.interface import implements
from twisted.internet import reactor, defer
from twisted.web import http
from twisted.python.failure import Failure

from allmydata.node import InvalidValueError, MissingConfigEntry
from allmydata.storage.backends.cloud.cloud_common import IContainer, \
     ContainerRetryMixin, ContainerListMixin, CloudError
from allmydata.util.hashutil import sha1
from allmydata.util import log


def configure_skydrive_container(storedir, config):
    from allmydata.storage.backends.cloud.skydrive.skydrive_container import SkyDriveContainer

    client_id = config.get_config("storage", "skydrive.client_id")
    client_secret = config.get_private_config("skydrive_client_secret")

    try: auth_code = config.get_private_config("skydrive_auth_code")
    except MissingConfigEntry:
        from txskydrive.api_v5 import txSkyDrive
        api = txSkyDrive(client_id=client_id, client_secret=client_secret)
        raise MissingConfigEntry(
            '\n\n'
            'Visit the following URL in any web browser (firefox, chrome, safari, etc),\n'
                '  authorize there, confirm access permissions, and paste URL of an empty page\n'
                '  (starting with "https://login.live.com/oauth20_desktop.srf") you will get\n'
                '  redirected to in the end into "private/skydrive_auth_code" file.\n\n'
            ' See "Authorization" section in "doc/cloud.rst" for details.\n\n'
            ' URL to visit: %s\n'%(api.auth_user_get_url()) )

    if re.search(r'^https?://', auth_code):
        from txskydrive.api_v5 import txSkyDrive
        api = txSkyDrive(client_id=client_id, client_secret=client_secret)
        api.auth_user_process_url(auth_code)
        config.write_private_config("skydrive_auth_code", api.auth_code)

    access_token = config.get_optional_private_config("skydrive_access_token")
    refresh_token = config.get_optional_private_config("skydrive_refresh_token")

    api_url = config.get_config("storage", "skydrive.api_url", "https://apis.live.net/v5.0/")
    api_debug = config.get_config("storage", "skydrive.api_debug", False, boolean=True)
    folder_id = config.get_config("storage", "skydrive.folder_id", None)
    folder_path = config.get_config("storage", "skydrive.folder_path", None)

    if not folder_id and not folder_path:
        raise InvalidValueError("Either skydrive.folder_id or skydrive.folder_path must be specified.")
    elif folder_id and folder_path:
        raise InvalidValueError( "Only one of skydrive.folder_id"
            " or skydrive.folder_path must be specified, not both." )
    elif not folder_id:
        folder_id = config.get_optional_private_config("skydrive_folder_id")
        folder_id_created = True
    else:
        folder_id_created = False

    def token_update_handler(auth_access_token, auth_refresh_token, **kwargs):
        config.write_private_config("skydrive_access_token", auth_access_token)
        config.write_private_config("skydrive_refresh_token", auth_refresh_token)
        if kwargs:
            log.msg( 'Received unhandled SkyDrive access'
                ' data, discarded: %s'%(', '.join(kwargs.keys())), level=log.WEIRD )

    def folder_id_update_handler(folder_id):
        config.write_private_config("skydrive_folder_id", folder_id)

    container = SkyDriveContainer( api_url, folder_id, folder_path,
        client_id, client_secret, auth_code,
        token_update_handler=token_update_handler,
        folder_id_update_handler=folder_id_update_handler,
        access_token=access_token, refresh_token=refresh_token,
        api_debug=api_debug )

    return container



def encode_object_name(name):
    return name.replace('_', '__').replace('/', '_')

def decode_object_name(name_enc):
    if isinstance(name_enc, unicode):
        name_enc = name_enc.encode('utf-8')
    return '__'.join(c.replace('_', '/') for c in name_enc.split('__'))


class DeferredCache(object):

    _empty = object()
    cache = _empty
    cache_clear_timer = None
    callback = None
    deferred = None

    def __init__(self, cache_ttl=None):
        self.listeners = set()
        self.cache_ttl = cache_ttl

    def _activate(self, res):
        self.deferred, self.cache = None, res
        listeners = list(self.listeners) # might get updated in callbacks
        self.listeners.clear()
        for d in listeners:
            if not d.called: d.callback(res)
        if not self.deferred and not isinstance(res, Failure):
            if self.cache_ttl is not None:
                self.cache = res
                if self.cache_ttl > 0:
                    self.cache_clear_timer = reactor.callLater(self.cache_ttl, self.clear)
            else: self.cache = self._empty

    def register_update_callback(self, func, *args, **kwargs):
        self.callback = func, args, kwargs

    def register_deferred(self, d=None):
        if self.cache is not self._empty:
            if not d: d = defer.succeed(self.cache)
            else: d.callback(self.cache)
            return d
        if not self.deferred: self.refresh()
        if not d: d = defer.Deferred()
        self.listeners.add(d)
        return d

    def refresh(self):
        if self.deferred: return self.deferred # already in progress
        self.clear()
        func, args, kwargs = self.callback
        d = self.deferred = func(*args, **kwargs)
        d.addBoth(self._activate)
        return self.register_deferred()

    def clear(self, ignored=None):
        self.cache = self._empty
        if self.cache_clear_timer:
            if self.cache_clear_timer.active():
                self.cache_clear_timer.cancel()
            self.cache_clear_timer = None
        return ignored



class SkyDriveItem(object):

    metadata_keys = 'key', 'modification_date', 'etag', 'size', 'storage_class', 'owner'

    storage_class = 'STANDARD'
    etag = None
    owner = None

    def __init__(self, info, key, backend_id):
        self.key = key or decode_object_name(info['name'])
        self.modification_date = info['updated_time']
        self.size = info['size']
        self.backend_id = backend_id

class SkyDriveListing(object):

    metadata_keys = 'name', 'prefix', 'marker',\
        'max_keys', 'is_truncated', 'contents', 'common_prefixes'

    prefix = ''
    marker = ''
    max_keys = 2**30
    is_truncated = 'false'

    def __init__(self, name, contents):
        self.name = name
        self.contents = contents


class SkyDriveMetadata(DeferredCache):

    def __init__(self, cache_ttl=None):
        self._idmap = dict()
        super(SkyDriveMetadata, self).__init__(cache_ttl=cache_ttl)


    def _add_object(self, cache, info, key=None):
        key = key or decode_object_name(info['name'])
        backend_id = info['id']
        self._idmap[key] = backend_id
        cache[backend_id] = SkyDriveItem(info, key=key, backend_id=backend_id)

    def _remove_object(self, cache, key=None, backend_id=None):
        assert not (key and backend_id)
        if backend_id: key = cache[backend_id].key
        else: backend_id = self._idmap[key]
        del self._idmap[key], cache[backend_id]

    def _update_listdir(self, lst):
        self._idmap.clear()
        contents = dict()
        for info in lst: self._add_object(contents, info)
        return contents

    def update_listdir(self):
        func, args, kwargs = self._callback
        return func(*args, **kwargs).addCallback(self._update_listdir)

    def register_update_callback(self, func, *args, **kwargs):
        self._callback = func, args, kwargs
        super(SkyDriveMetadata, self).register_update_callback(self.update_listdir)


    def enumerate(self, name, prefix):
        d = self.register_deferred()
        def _filter_contents(contents):
            contents = list( obj
                for obj in contents.viewvalues()
                if obj.key.startswith(prefix) )
            return SkyDriveListing(name, contents)
        d.addCallback(_filter_contents)
        return d

    def get(self, key):
        d = self.register_deferred()
        def _get_object(contents, try_refresh=True):
            try: backend_id = self._idmap[key]
            except KeyError:
                if not try_refresh: raise
                log.msg( 'Detected inconsistency between cached list of'
                    ' share chunks and requested ones (missing files in the'
                    ' cloud?), trying to refresh list of remote shares.', level=log.WEIRD )
                d = self.refresh()
                d.addCallback(_get_object, try_refresh=False)
                return d
            return contents[backend_id]
        d.addCallback(_get_object)
        return d

    def add(self, info, key=None):
        d = self.register_deferred()
        d.addCallback(self._add_object, info, key=key)
        return d

    def remove(self, key):
        d = self.register_deferred()
        d.addCallback(self._remove_object, key=key)
        return d



class SkyDriveContainer(ContainerRetryMixin):
    implements(IContainer)
    """
    I represent SkyDrive container (folder), accessed using the txskydrive module.
    """

    def __init__( self, api_url, folder_id, folder_path,
            client_id, client_secret, auth_code,
            token_update_handler=None,
            folder_id_update_handler=None,
            access_token=None, refresh_token=None,
            api_debug=False ):
        # Only depend on txskydrive when this class is actually instantiated.
        from txskydrive.api_v5 import txSkyDrivePluggableSync, ProtocolError

        self.client = txSkyDrivePluggableSync(
            client_id=client_id, client_secret=client_secret, auth_code=auth_code,
            auth_access_token=access_token, auth_refresh_token=refresh_token,
            config_update_callback=token_update_handler,
            api_url_base=api_url, debug_requests=api_debug )

        self.folder_path = normpath(folder_path).lstrip('/')
        self.folder_id = folder_id
        self.folder_id_update_handler = folder_id_update_handler
        self.folder_name = folder_path or folder_id

        # Cache TTL can be inf if nothing else touches the folder,
        #  but just to be safe, make sure to refresh it occasionally.
        self.folder = SkyDriveMetadata(cache_ttl=3600)
        self.folder.register_update_callback( self._do_request,
            'list objects', self.client.listdir, self.folder_id, type_filter='file' )

        self.ServiceError = ProtocolError

    def __repr__(self):
        return ("<%s %r>" % (self.__class__.__name__, self.folder_name,))


    def create_shares_folder(self, folder_path, folder_id):
        from txskydrive.api_v5 import DoesNotExists
        d = self._do_request('resolve storage path', self.client.resolve_path, folder_path)

        def _match_folder_id(path_info):
            folder_path_id = path_info['id']
            if folder_path_id != folder_id:
                self.folder_id_update_handler(folder_path_id)
                self.folder_id = folder_path_id
                self.folder.register_update_callback( self._do_request,
                    'list objects', self.client.listdir, self.folder_id, type_filter='file' )

        def _create_folder(parent_info, name):
            return self._do_request('mkdir', self.client.mkdir, name, parent_info['id'])

        def _folder_lookup_error(failure):
            failure.trap(DoesNotExists)
            parent_id, slugs = failure.value.args
            d = _create_folder(dict(id=parent_id), slugs[0])
            for slug in slugs[1:]:
                d.addCallback(_create_folder, slug)
            return d

        d.addCallback(_match_folder_id)
        d.addErrback(self.folder.clear)
        d.addErrback(_folder_lookup_error)
        d.addCallback(_match_folder_id)
        return d


    _create_combinator_v = None
    @property
    def _create_combinator(self):
        if not self._create_combinator_v:
            self._create_combinator_v = DeferredCache()
            def _create_folder():
                return self.create_shares_folder(self.folder_path, self.folder_id)
            self._create_combinator_v.register_update_callback(
                self._do_request, 'create/check folder', _create_folder )
        return self._create_combinator_v

    def create(self):
        return self._create_combinator.register_deferred()

    def delete(self):
        d = self._do_request('delete folder', self.client.delete, self.folder_id)
        d.addCallback(self.folder.clear)
        d.addCallback(self._list_cache.clear)
        return d


    def create_missing_folders(func):
        'Handle missing path errors by checking/creating root folder and trying again.'
        from txskydrive.api_v5 import ProtocolError
        def _enoent_handler(failure, self, args, kwargs):
            failure.trap(ProtocolError, CloudError)
            http_code = failure.value.code\
                if isinstance(failure.value, ProtocolError)\
                else failure.value.args[1]
            if http_code not in [http.NOT_FOUND, http.GONE]:
                return failure
            self._list_cache.clear()
            d = self.create()
            d.addCallback(lambda ignored: func(self, *args, **kwargs))
            return d
        def _wrapper(self, *args, **kwargs):
            if not self.folder_id:
                d = self.create()
                d.addCallback(lambda ignored: func(self, *args, **kwargs))
            else:
                d = func(self, *args, **kwargs)
                d.addErrback(_enoent_handler, self, args, kwargs)
            return d
        return _wrapper


    @create_missing_folders
    def list_objects(self, prefix=''):
        return self.folder.enumerate(self.folder_name, prefix)

    @create_missing_folders
    def put_object(self, object_name, data, content_type=None, metadata={}):
        assert content_type is None, content_type
        assert metadata == {}, metadata

        # Rearrange arguments, so (potentially large) data won't end up in the logs
        d = self._do_request( 'PUT object',
            lambda name, dst, data: self.client.put((name, data), dst),
            encode_object_name(object_name), self.folder_id, data )

        # Returned info dict lacks some metadata keys, used in SkyDriveItem,
        #  which can be acquired properly via folder.get(), but is simple enough
        #  to fill it in right here
        def _extend_info(info):
            info['size'] = len(data)
            info['updated_time'] = datetime.utcnow().isoformat()
            return info
        d.addCallback(_extend_info)

        d.addCallback(self.folder.add, object_name)
        return d


    def resolve_object_name(func):
        def _wrapper(self, key):
            d = self.folder.get(key)
            d.addCallback(lambda obj: func(self, key, obj.backend_id))
            return d
        return _wrapper

    @resolve_object_name
    def get_object(self, key, backend_id):
        return self._do_request('GET object', self.client.get, backend_id)

    @resolve_object_name
    def delete_object(self, key, backend_id):
        d = self._do_request('DELETE object', self.client.delete, backend_id)
        d.addCallback(lambda ignored: self.folder.remove(key))
        return d

    def head_object(self, key):
        return self.folder.get(key)
