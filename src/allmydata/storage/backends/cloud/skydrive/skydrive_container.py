#-*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from datetime import datetime
from collections import deque
import re

from zope.interface import implements
from twisted.internet import reactor, defer, task
from twisted.web import http

from allmydata.node import InvalidValueError, MissingConfigEntry
from allmydata.storage.backends.cloud.cloud_common import\
    IContainer, ContainerRetryMixin, CloudError
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
    folder_buckets = int(config.get_config("storage", "skydrive.folder_buckets", 1))

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

    container = SkyDriveContainer( api_url,
        folder_id, folder_path, folder_buckets,
        client_id, client_secret, auth_code,
        token_update_handler=token_update_handler,
        folder_id_update_handler=folder_id_update_handler,
        access_token=access_token, refresh_token=refresh_token,
        api_debug=api_debug )

    return container



class RateLimitMixin(object):

    def __init__(self, interval, burst):
        self.bucket = defer.DeferredQueue(burst, backlog=None)
        def _put_token(bucket=self.bucket):
            try: bucket.put(None)
            except defer.QueueOverflow: pass
        task.LoopingCall(_put_token).start(interval, now=False)

    def _do_request(self, *args, **kwargs):
        return self.bucket.get().addCallback(
            lambda ignored: super(RateLimitMixin, self)._do_request(*args, **kwargs) )



def encode_key(key):
    return key.replace('_', '__').replace('/', '_')

def decode_key(key_enc):
    if isinstance(key_enc, unicode):
        key_enc = key_enc.encode('utf-8')
    return '__'.join(c.replace('_', '/') for c in key_enc.split('__'))


class SkyDriveItem(object):
    # 'key', 'modification_date', 'etag', 'size', 'storage_class', 'owner'

    storage_class = 'STANDARD'
    etag = None
    owner = None

    backend_id = None

    def __init__(self, info, **kwz):
        self.key = kwz.pop('key', None) or decode_key(info['name'])
        self.backend_id = kwz.pop('backend_id', None) or info['id']
        self.modification_date = kwz.pop('modification_date', None) or info['updated_time']
        self.size = kwz.pop('size', None) or info['size']
        for k, v in kwz.viewitems(): setattr(self, k, v)


class SkyDriveListing(object):
    # 'name', 'prefix', 'marker', 'max_keys',
    #  'is_truncated', 'contents', 'common_prefixes'

    marker = ''
    max_keys = 2**30
    is_truncated = 'false'

    def __init__(self, name, prefix, contents):
        self.name, self.prefix, self.contents = name, prefix, contents



class SkyDriveContainer(RateLimitMixin, ContainerRetryMixin):
    implements(IContainer)

    def __init__( self, api_url,
            folder_id, folder_path, folder_buckets,
            client_id, client_secret, auth_code,
            token_update_handler=None,
            folder_id_update_handler=None,
            access_token=None, refresh_token=None,
            api_debug=False ):
        from txskydrive.api_v5 import txSkyDrivePluggableSync, ProtocolError, DoesNotExists

        self.client = txSkyDrivePluggableSync(
            client_id=client_id, client_secret=client_secret, auth_code=auth_code,
            auth_access_token=access_token, auth_refresh_token=refresh_token,
            config_update_callback=token_update_handler,
            api_url_base=api_url, debug_requests=api_debug )

        self.folder_path = folder_path
        self.folder_id = folder_id
        self.folder_id_update_handler = folder_id_update_handler
        self.folder_name = folder_path or folder_id

        self.folder_buckets = folder_buckets
        self._key_hash = sha1
        self._key_hash_max = khm = 1 << (8 * sha1('').digest_size)
        self._key_hash_max = khm - (khm % folder_buckets) - 1
        self._bucket_format = '{{:0{}d}}'.format(len(str(folder_buckets)))

        self.ProtocolError, self.DoesNotExists = ProtocolError, DoesNotExists
        self.ServiceError = ProtocolError
        super(SkyDriveContainer, self).__init__(interval=10, burst=10)

    def __repr__(self):
        return '<{} {!r}>'.format(self.__class__.__name__, self.folder_name)


    def key_bucket(self, key, prefix=''):
        # Can return any string, which will be used as a subdir for key.
        # Subdir can have multiple components (subdirs) in it. Can also be empty.
        # It doesn't matter (i.e. whole thing will work) how deeply nested
        #  these subdirs are and whether nesting depth is consistent -
        #  whole shares dir will be scanned recursively and all file-id's recorded.
        # original key = shares/$PREFIX/$STORAGEINDEX/$SHNUM.$CHUNK
        if self.folder_buckets == 1: return prefix # don't make any subfolders
        hn, h = 0, self._key_hash(key).digest()
        for b in h: hn = (hn << 8) + ord(b)
        if hn > self._key_hash_max: # avoid bias by +1 hashing
            return self.key_bucket(h)
        return self.fjoin( prefix,
            self._bucket_format.format(hn % self.folder_buckets) )

    @staticmethod
    def fjoin(*slugs):
        return '/'.join(it.ifilter( None,
            it.chain.from_iterable(slug.split('/') for slug in slugs) ))

    @defer.inlineCallbacks
    def _mkdir(self, path=''):
        if path: kwz = dict(root_id=self.folder_id)
        else: kwz, path = dict(), self.folder_name
        try:
            defer.returnValue((yield self._do_request(
                'check path components', self.client.resolve_path, path, **kwz )))
        except self.DoesNotExists as err:
            parent_id, slugs = err.args
            for slug in slugs:
                try:
                    parent_id = (yield self._do_request(
                        'mkdir', self.client.mkdir, slug, parent_id ))['id']
                except CloudError as err:
                    if err.args[1] != http.BAD_REQUEST: raise
                    # Might be created in-parallel, try probing it
                    parent_id = yield self._do_request(
                        'check path', self.client.resolve_path, slug, root_id=parent_id )
            defer.returnValue(parent_id)

    @defer.inlineCallbacks
    def _mkdir_wrapper(self, func, path=''):
        try: defer.returnValue((yield defer.maybeDeferred(func)))
        except self.ProtocolError as err: http_code = err.code
        except CloudError as err: http_code = err.args[1]
        if http_code not in [http.NOT_FOUND, http.GONE]: raise
        self.folder_id = yield self._mkdir()
        self.folder_id_update_handler(self.folder_id)
        yield self._mkdir(path)
        defer.returnValue((yield defer.maybeDeferred(func)))


    def create(self):
        return self._mkdir()

    @defer.inlineCallbacks
    def delete(self):
        yield self._do_request(
            'delete root', self.client.delete, self.folder_id )
        self._chunks.clear()
        self._folds.clear()


    _chunks = None # {file_id1: info1, file_key1: info1, ...}
    _folds = None # {fold: folder_id, ...}

    @defer.inlineCallbacks
    def _crawl(self):
        chunks, folds = list(), {'': self.folder_id}
        lst = deque( ('', info) for info in (yield self._mkdir_wrapper(
            lambda: self._do_request('list root', self.client.listdir, self.folder_id) )) )
        while lst:
            fold, info = lst.popleft()
            if info['type'] == 'folder':
                folds[self.fjoin(fold, info['name'])] = info['id']
                lst.extend( (self.fjoin(fold, info['name'], ci['name']), ci)
                    for ci in (yield self._do_request('listdir', self.client.listdir, info['id'])) )
            else: chunks.append(info)
        defer.returnValue((chunks, folds))

    @defer.inlineCallbacks
    def list_objects(self, prefix=''):
        if not self._chunks:
            chunk_list, self._folds = yield self._crawl()
            chunks = dict()
            for info in chunk_list:
                key, cid = decode_key(info['name']), info['id']
                # Hopefully that will never happen, but check just in case
                assert key not in chunks and cid not in chunks,\
                    'Detected two uploaded shares with same identifiers: {} / {}'.format(key, cid)
                chunks[key] = chunks[cid] = SkyDriveItem(info, key=key)
            self._chunks = chunks
        defer.returnValue(
            SkyDriveListing(self.folder_name, prefix, list(
                item for key, item in self._chunks.viewitems()
                if prefix and key.startswith(prefix) )) )


    @defer.inlineCallbacks
    def put_object(self, key, data, content_type=None, metadata=None):
        assert not content_type, content_type
        assert not metadata, metadata

        fold = self.key_bucket(key)
        @defer.inlineCallbacks
        def _upload_chunk():
            for fail in xrange(2):
                try:
                    defer.returnValue((yield self._do_request( 'upload',
                        # Wrapper here is to omit the large chunk body from the logs
                        lambda dst: self.client.put((encode_key(key), data), dst), self._folds[fold] )))
                except KeyError:
                    if fail: raise
                    self._folds[fold] = yield self._mkdir(fold)
        info = yield self._mkdir_wrapper(_upload_chunk, fold)

        info['size'] = len(data)
        info['updated_time'] = datetime.utcnow().isoformat()
        self._chunks[key] = self._chunks[info['id']] = SkyDriveItem(info, key=key)

    @defer.inlineCallbacks
    def delete_object(self, key):
        chunk_id = self._chunks[key].backend_id
        yield self._do_request('delete', self.client.delete, chunk_id)
        del self._chunks[key], self._chunks[chunk_id]


    def get_object(self, key):
        return self._do_request('get', self.client.get, self._chunks[key].backend_id)

    def head_object(self, key):
        return self._chunks[key]
