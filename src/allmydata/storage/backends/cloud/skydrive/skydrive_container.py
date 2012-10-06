
from os.path import basename, dirname, normpath, join
import re

from zope.interface import implements
from twisted.internet import reactor, defer
from twisted.web import http

from allmydata.node import InvalidValueError, MissingConfigEntry
from allmydata.storage.backends.cloud.cloud_common import IContainer, \
     ContainerRetryMixin, ContainerListMixin
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

    mapping_path = storedir.child("skydrive_idmap.db")

    def token_update_handler(auth_access_token, auth_refresh_token, **kwargs):
        config.write_private_config("skydrive_access_token", auth_access_token)
        config.write_private_config("skydrive_refresh_token", auth_refresh_token)
        if kwargs:
            log.msg( 'Received unhandled SkyDrive access'
                ' data, discarded: %s'%(', '.join(kwargs.keys())), level=log.WEIRD )

    def folder_id_update_handler(folder_id):
        config.write_private_config("skydrive_folder_id", folder_id)

    container = SkyDriveContainer( api_url, folder_id, folder_path,
        client_id, client_secret, auth_code, mapping_path,
        token_update_handler=token_update_handler,
        folder_id_update_handler=folder_id_update_handler,
        access_token=access_token, refresh_token=refresh_token )

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
    cache_ttl_flush = None
    callback = None
    deferred = None

    def __init__(self, cache_ttl=None):
        self.listeners = set()
        self.cache_ttl = cache_ttl

    def _activate(self, res):
        assert self.deferred
        self.deferred = None
        for d in self.listeners: d.callback(res)
        if self.cache_ttl is not None:
            self.cache = res
            if self.cache_ttl > 0:
                self.cache_ttl_flush = reactor.callLater(self.cache_ttl, self.flush)
        self.listeners.clear()
        return res

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
        if self.deferred: # already in progress
            return self.deferred
        self.flush()
        func, args, kwargs = self.callback
        d = self.deferred = func(*args, **kwargs)
        d.addBoth(self._activate)
        return d

    def flush(self, ignored=None):
        self.cache = self._empty
        if self.cache_ttl_flush:
            if self.cache_ttl_flush.active():
                self.cache_ttl_flush.cancel()
            self.cache_ttl_flush = None
        return ignored


class SkyDriveItem(object):

    def __init__(self, key, modification_date, etag, size, storage_class, owner=None):
        self.key = key
        self.modification_date = modification_date
        self.etag = etag
        self.size = size
        self.storage_class = storage_class
        self.owner = owner

    @classmethod
    def from_info(cls, info, key=None):
        key = key or decode_object_name(info['name'])
        etag = sha1('\0'.join([info['id'], info['name'], info['updated_time']])).hexdigest()
        return cls(key, info['updated_time'], etag, info['size'], 'STANDARD', None)

class SkyDriveListing(object):

    def __init__( self, name, prefix, marker, max_keys,
            is_truncated, contents=None, common_prefixes=None ):
        self.name = name
        self.prefix = prefix
        self.marker = marker
        self.max_keys = max_keys
        self.is_truncated = is_truncated
        self.contents = contents
        self.common_prefixes = common_prefixes


class SkyDriveContainer(ContainerRetryMixin):
    implements(IContainer)
    """
    I represent SkyDrive container (folder), accessed using the txskydrive module.
    """

    def __init__( self, api_url, folder_id, folder_path,
            client_id, client_secret, auth_code, mapping_path,
            token_update_handler=None,
            folder_id_update_handler=None,
            access_token=None, refresh_token=None ):
        # Only depend on txskydrive when this class is actually instantiated.
        from txskydrive.api_v5 import txSkyDrivePluggableSync, ProtocolError
        import anydbm

        self.client = txSkyDrivePluggableSync(
            client_id=client_id, client_secret=client_secret, auth_code=auth_code,
            auth_access_token=access_token, auth_refresh_token=refresh_token,
            api_url_base=api_url, config_update_callback=token_update_handler )

        self.folder_path = normpath(folder_path).lstrip('/')
        self.folder_id = folder_id
        self.folder_id_update_handler = folder_id_update_handler
        self.folder_name = folder_path or folder_id

        self.chunk_idmap_path = mapping_path.path
        self.chunk_idmap = anydbm.open(mapping_path.path, 'c')

        self.ServiceError = ProtocolError

    def __repr__(self):
        return ("<%s %r>" % (self.__class__.__name__, self.folder_name,))


    def ensure_folder(self, folder_path, folder_id):
        from txskydrive.api_v5 import DoesNotExists
        import anydbm
        d = self._do_request('resolve storage path', self.client.resolve_path, folder_path)

        def _match_folder_id(folder_path_id):
            if folder_path_id != folder_id:
                self.folder_id_update_handler(folder_path_id)
                self.folder_id = folder_path_id

        def _create_folder(parent_info, name):
            return self._do_request('mkdir', self.client.mkdir, name, parent_info['id'])

        def _folder_lookup_error(failure):
            failure.trap(DoesNotExists)
            parent_id, slugs = failure.value.args
            d = _create_folder(dict(id=parent_id), slugs[0])
            for slug in slugs[1:]:
                d.addCallback(_create_folder, slug)
            return d

        def _flush_idmap(failure):
            failure.trap(DoesNotExists)
            self.chunk_idmap = anydbm.open(self.chunk_idmap_path, 'n')
            return failure

        d.addCallback(_match_folder_id)
        d.addErrback(_flush_idmap)
        d.addErrback(_folder_lookup_error)
        d.addCallback(_match_folder_id)
        return d


    _create_combinator_v = None
    @property
    def _create_combinator(self):
        if not self._create_combinator_v:
            self._create_combinator_v = DeferredCache()
            self._create_combinator_v.register_update_callback(
                self._do_request, 'create/check folder',
                self.ensure_folder, self.folder_path, self.folder_id )
        return self._create_combinator_v

    _list_cache_v = None
    _list_cache_ttl = 120
    @property
    def _list_cache(self):
        if not self._list_cache_v:
            self._list_cache_v = DeferredCache(cache_ttl=self._list_cache_ttl)
            self._list_cache_v.register_update_callback(
                self._do_request, 'list objects', self.client.listdir, self.folder_id, type_filter='file' )
        return self._list_cache_v


    def create(self):
        return self._create_combinator.register_deferred()

    def delete(self):
        d = self._do_request('delete folder', self.client.delete, self.folder_id)
        def _flush_idmap(ignored):
            self.chunk_idmap = anydbm.open(self.chunk_idmap_path, 'n')
        d.addCallback(_flush_idmap)
        d.addCallback(self._list_cache.flush)
        return d


    def handle_enoent(func):
        'Handle missing path errors by checking/creating root folder and trying again.'
        from txskydrive.api_v5 import ProtocolError
        def _enoent_handler(failure, self, args, kwargs):
            failure.trap(ProtocolError)
            if failure.value.code not in [http.NOT_FOUND, http.GONE]:
                return failure
            self._list_cache.flush()
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


    @handle_enoent
    def resolve_object_name(self, object_name):
        try:
            return defer.succeed(self.chunk_idmap[object_name])
        except KeyError: pass
        d = self._do_request( 'resolve object path', self.client.resolve_path,
            encode_object_name(object_name), root_id=self.folder_id )
        def _update_idmap(cid):
            self.chunk_idmap[object_name] = cid
            return cid
        d.addCallback(_update_idmap)
        return d


    @handle_enoent
    def list_objects(self, prefix=''):
        d = self._list_cache.register_deferred()
        def _filter_objects(lst):
            contents = list()
            for info in lst:
                # Use this chance to populate the chunk_idmap cache
                key = decode_object_name(info['name'])
                self.chunk_idmap[key] = info['id']
                if not key.startswith(prefix): continue
                contents.append(SkyDriveItem.from_info(info, key=key))
            return SkyDriveListing(self.folder_name, '', '', 1000, 'false', contents, None)
        d.addCallback(_filter_objects)
        return d

    @handle_enoent
    def put_object(self, object_name, data, content_type=None, metadata={}):
        assert content_type is None, content_type
        assert metadata == {}, metadata
        d = self._do_request( 'PUT object', self.client.put,
            (encode_object_name(object_name), data), self.folder_id )
        def _cache_object_id(info):
            self.chunk_idmap[object_name] = info['id']
        d.addCallback(_cache_object_id)
        d.addCallback(self._list_cache.flush)
        return d

    def get_object(self, object_name):
        d = self.resolve_object_name(object_name)
        d.addCallback(lambda cid: self._do_request('GET object', self.client.get, cid))
        return d

    def head_object(self, object_name):
        d = self.resolve_object_name(object_name)
        d.addCallback(lambda cid: self._do_request('HEAD object', self.client.info, cid))
        d.addCallback(SkyDriveItem.from_info)
        return d

    def delete_object(self, object_name):
        d = self.resolve_object_name(object_name)
        d.addCallback(lambda cid: self._do_request('DELETE object', self.client.delete, cid))
        d.addCallback(self._list_cache.flush)
        return d
