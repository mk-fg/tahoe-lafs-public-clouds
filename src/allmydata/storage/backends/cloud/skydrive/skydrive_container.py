
from os.path import basename, dirname, normpath, join
import re

from zope.interface import implements
from twisted.internet import defer

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

    def token_update_handler(self, auth_access_token, auth_refresh_token, **kwargs):
        config.write_private_config("skydrive_access_token", auth_access_token)
        config.write_private_config("skydrive_refresh_token", auth_refresh_token)

    def folder_id_update_handler(self, folder_id):
        config.write_private_config("skydrive_folder_id")

    container = SkyDriveContainer( api_url, folder_id, folder_path,
        client_id, client_secret, auth_code, mapping_path,
        token_update_handler=token_update_handler,
        folder_id_update_handler=folder_id_update_handler,
        access_token=access_token, refresh_token=refresh_token )

    return container


class SkyDriveItem(object):

    def __init__(self, key, modification_date, etag, size, storage_class, owner=None):
        self.key = key
        self.modification_date = modification_date
        self.etag = etag
        self.size = size
        self.storage_class = storage_class
        self.owner = owner

    @classmethod
    def from_info(cls, info):
        etag = sha1('\0'.join([info['id'], info['name'], info['updated_time']])).hexdigest()
        return cls(info['id'], info['updated_time'], etag, info['size'], 'STANDARD', None)


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
        from txskydrive.api_v5 import txSkyDrivePluggableSync,\
            SkyDriveInteractionError, DoesNotExists
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

        self.ServiceError = SkyDriveInteractionError

    def __repr__(self):
        return ("<%s %r>" % (self.__class__.__name__, self.folder_name,))


    def ensure_folder(self, folder_path, folder_id):
        d = self._do_request('resolve path', self.client.resolve_path, folder_path)

        def _match_folder_id(folder_path_id):
            if folder_path_id != folder_id:
                self.folder_id_update_handler(folder_path_id)
                self.folder_id = folder_path_id

        def _create_folder(parent_info, name):
            return self._do_request('mkdir', self.client.mkdir, name, parent_info['id'])

        def _folder_lookup_error(reason, path):
            reason.trap(DoesNotExists)
            path = path.rsplit('/', 1)
            if len(path) > 1:
                d = self._do_request('resolve path', self.client.resolve_path, path[0])
                d.addCallback(_create_folder, path[-1])
                d.addErrback(_folder_lookup_error, path[0]) # try to create parent path
                d.addCallback(_create_folder, path[-1]) # retry
            else:
                d = self._do_request('mkdir', self.client.mkdir, path[-1])
            return d

        def _flush_idmap(ignored):
            self.chunk_idmap = anydbm.open(self.chunk_idmap_path, 'n')
            return ignored

        d.addCallback(_match_folder_id)
        d.addErrback(_flush_idmap)
        d.addErrback(_folder_lookup_error, folder_path)
        d.addCallback(_match_folder_id)
        return d

    def create(self):
        return self._do_request( 'create/check folder',
            self.ensure_folder, self.folder_path, self.folder_id )

    def delete(self):
        d = self._do_request('delete folder', self.client.delete, self.folder_id)
        def _flush_idmap(ignored):
            self.chunk_idmap = anydbm.open(self.chunk_idmap_path, 'n')
        d.addCallback(_flush_idmap)
        return d


    def encode_object_name(self, name):
        return name.replace('_', '__').replace('/', '_')

    def decode_object_name(self, name_enc):
        return '__'.join(c.replace('_', '/') for c in name_enc.split('__'))

    def resolve_object_name(self, object_name):
        try:
            return defer.suceed(self.chunk_idmap[object_name])
        except KeyError: pass

        d = self._do_request( 'resolve path', self.client.resolve_path,
            join(self.folder_path, self.encode_object_name(object_name)) )
        def _update_idmap(cid):
            self.chunk_idmap[object_name] = cid
            return cid
        d.addCallback(_update_idmap)
        return d


    def list_objects(self, prefix=''):
        d = self._do_request('list objects', self.client.listdir, self.folder_id, type_filter='file')
        def _filter_objects(lst):
            contents = list(
                SkyDriveItem.from_info(info)
                for info in lst if info['name'].startswith(prefix) )
            return SkyDriveListing(self.folder_name, '', '', 1000, 'false', contents, None)
        d.addCallback(_filter_objects)
        return d

    def put_object(self, object_name, data, content_type=None, metadata={}):
        assert content_type is None, content_type
        assert metadata == {}, metadata
        d = self._do_request( 'PUT object', self.client.put,
            (self.encode_object_name(object_name), data), self.folder_id )
        def _cache_object_id(info):
            self.chunk_idmap[object_name] = info['id']
        d.addCallback(_cache_object_id)
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
        return d
