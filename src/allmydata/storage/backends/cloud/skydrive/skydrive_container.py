#-*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from datetime import datetime
from time import time
from collections import deque
import re

from zope.interface import implements
from twisted.internet import reactor, defer, task
from twisted.web import http

from allmydata.node import InvalidValueError, MissingConfigEntry
from allmydata.storage.backends.cloud.cloud_common import (
    IContainer, ContainerRetryMixin, CloudError,
    CloudServiceError, ContainerItem, ContainerListing,
    CommonContainerMixin, HTTPClientMixin )
from allmydata.util.hashutil import sha1
from allmydata.util import log


def configure_skydrive_container(storedir, config):
    from allmydata.storage.backends.cloud.skydrive.skydrive_container import SkyDriveContainer
    from txskydrive.api_v5 import txSkyDrive

    client_id = config.get_config('storage', 'skydrive.client_id')
    client_secret = config.get_private_config('skydrive_client_secret')

    try: auth_code = config.get_private_config('skydrive_auth_code')
    except MissingConfigEntry:
        api = txSkyDrive(client_id=client_id, client_secret=client_secret)
        raise MissingConfigEntry(
            '\n\n'
            'Visit the following URL in any web browser (firefox, chrome, safari, etc),\n'
                '  authorize there, confirm access permissions, and paste URL of an empty page\n'
                '  (starting with "https://login.live.com/oauth20_desktop.srf") you will get\n'
                '  redirected to in the end into "private/skydrive_auth_code" file.\n\n'
            ' See "Authorization" section in "doc/cloud.rst" for details.\n\n'
            ' URL to visit: {}\n'.format(api.auth_user_get_url()) )

    if re.search(r'^https?://', auth_code):
        from txskydrive.api_v5 import txSkyDrive
        api = txSkyDrive(client_id=client_id, client_secret=client_secret)
        api.auth_user_process_url(auth_code)
        config.write_private_config('skydrive_auth_code', api.auth_code)

    access_token = config.get_optional_private_config('skydrive_access_token')
    refresh_token = config.get_optional_private_config('skydrive_refresh_token')

    api_timeouts = dict()
    for k in txSkyDrive.request_io_timeouts:
        k_conf = 'skydrive.api.timeout.{}'.format(k)
        v = config.get_config('storage', k_conf, None)
        if v is None:
            continue
        v = float(v)
        if v < 0:
            raise InvalidValueError('{} value must be a positive integer or zero.'.format(k_conf))
        api_timeouts[k] = v

    api_parameters = dict(
        url = config.get_config('storage', 'skydrive.api.url', 'https://apis.live.net/v5.0/'),
        debug = config.get_config('storage', 'skydrive.api.debug', False, boolean=True),
        tb_interval = float(config.get_config('storage', 'skydrive.api.ratelimit.interval', 0)),
        tb_burst = int(config.get_config('storage', 'skydrive.api.ratelimit.burst', 1)),
        timeouts=api_timeouts )
    if api_parameters['tb_interval'] < 0:
        raise InvalidValueError(
            'skydrive.api.ratelimit.interval value must be either positive or zero.' )
    if api_parameters['tb_burst'] <= 0:
        raise InvalidValueError('skydrive.api.ratelimit.burst value must be a positive integer.')

    folder_id = config.get_config('storage', 'skydrive.folder_id', None)
    folder_path = config.get_config('storage', 'skydrive.folder_path', None)

    folder_buckets = int(config.get_config('storage', 'skydrive.folder_buckets', 1))
    if folder_buckets < 1:
        raise InvalidValueError('skydrive.folder_buckets value must be a positive integer.')

    if not folder_id and not folder_path:
        raise InvalidValueError('Either skydrive.folder_id or skydrive.folder_path must be specified.')
    elif folder_id and folder_path:
        raise InvalidValueError( 'Only one of skydrive.folder_id'
            ' or skydrive.folder_path must be specified, not both.' )
    elif not folder_id:
        folder_id = config.get_optional_private_config('skydrive_folder_id')
        folder_id_created = True
    else:
        folder_id_created = False

    def token_update_handler(auth_access_token, auth_refresh_token, **kwargs):
        config.write_private_config('skydrive_access_token', auth_access_token)
        config.write_private_config('skydrive_refresh_token', auth_refresh_token)
        if kwargs:
            log.msg( 'Received unhandled SkyDrive access'
                ' data, discarded: {}'.format(', '.join(kwargs.keys())), level=log.WEIRD )

    def folder_id_update_handler(folder_id):
        config.write_private_config("skydrive_folder_id", folder_id)

    container = SkyDriveContainer(
        api_parameters, folder_id, folder_path,
        client_id, client_secret, auth_code,
        folder_buckets=folder_buckets,
        token_update_handler=token_update_handler,
        folder_id_update_handler=folder_id_update_handler,
        access_token=access_token, refresh_token=refresh_token, )

    return container



def token_bucket(interval, burst=1, borrow=True):
    tokens, rate, ts_sync = burst, interval**-1, time()
    val = yield
    while True:
        if val is None: val = 1
        ts = time()
        ts_sync, tokens = ts, min(burst, tokens + (ts - ts_sync) * rate)
        if tokens >= val: delay, tokens = None, tokens - val
        else:
            delay = (val - tokens) / rate
            if borrow: tokens -= val
        val = yield delay

class ContainerRateLimitMixin(object):
    # Uses/expects container._reactor attribute set for the object
    # TODO: generic IMixin with fixed _do_request method

    rate_errors = 420,
    retry_backoff = 60, 120, 120, 600

    def __init__(self, interval, burst):
        if interval <= 0 or burst <= 0: # no limit
            self.bucket = None
            return
        self.bucket = token_bucket(interval, burst)
        next(self.bucket)

    def _delay(self, seconds):
        d = defer.Deferred()
        self._reactor.callLater(seconds, d.callback, None)
        return d

    @defer.inlineCallbacks
    def _do_request(self, *argz, **kwz):
        func = ft.partial( self._rate_limit_retries,
            super(ContainerRateLimitMixin, self)._do_request, *argz, **kwz )
        if not self.bucket: defer.returnValue((yield func()))
        delay = next(self.bucket)
        if delay is None: defer.returnValue((yield func()))
        yield self._delay(delay)
        defer.returnValue((yield func()))

    @defer.inlineCallbacks
    def _rate_limit_retries(self, func, *argz, **kwz):
        # These are bound to happen with free-API's, unfortunately
        try: defer.returnValue((yield func(*argz, **kwz)))
        except CloudError as err:
            if err.args[1] not in self.rate_errors: raise
            log.msg( 'Got rate-limit error from the API ({} {}), retrying after'
                ' a delay'.format(err.args[1], err.args[2]), level=log.OPERATIONAL )
        # Do it the hard way
        for attempt, delay in enumerate(self.retry_backoff, 1):
            yield self._delay(delay)
            try: defer.returnValue((yield func(*argz, **kwz)))
            except CloudError as err:
                if err.args[1] not in self.rate_errors\
                    or attempt == len(self.retry_backoff): raise



def encode_key(key):
    return key.replace('_', '__').replace('/', '_')

def decode_key(key_enc):
    if isinstance(key_enc, unicode):
        key_enc = key_enc.encode('utf-8')
    return '__'.join(c.replace('_', '/') for c in key_enc.split('__'))


class SkyDriveItem(ContainerItem):
    # 'key', 'modification_date', 'etag', 'size', 'storage_class', 'owner'

    backend_id = None
    storage_class = 'STANDARD'

    def __init__(self, info, **kwz):
        self.backend_id = kwz.pop('backend_id', None) or info['id']
        super(SkyDriveItem, self).__init__(
            kwz.pop('key', None) or decode_key(info['name']), # key
            kwz.pop('modification_date', None) or info['updated_time'], # modification_date
            self.backend_id, # etag
            kwz.pop('size', None) or info['size'], # size
            self.storage_class ) # storage_class
        for k, v in kwz.viewitems(): setattr(self, k, v) # extras


class SkyDriveListing(ContainerListing):
    # 'name', 'prefix', 'marker', 'max_keys', 'is_truncated', 'contents', 'common_prefixes'

    max_keys = 2**30
    is_truncated = 'false'

    def __init__(self, name, prefix, contents):
        super(SkyDriveListing, self).__init__(
            name, prefix, None, self.max_keys, self.is_truncated, contents=contents )



class SkyDriveContainer(ContainerRateLimitMixin, ContainerRetryMixin):
    implements(IContainer)

    def __init__( self, api,
            folder_id, folder_path,
            client_id, client_secret, auth_code,
            folder_buckets=1,
            token_update_handler=None,
            folder_id_update_handler=None,
            access_token=None, refresh_token=None,
            override_reactor=None ):
        from txskydrive.api_v5 import txSkyDrivePluggableSync, ProtocolError, DoesNotExists

        self.client = txSkyDrivePluggableSync(
            client_id=client_id, client_secret=client_secret, auth_code=auth_code,
            auth_access_token=access_token, auth_refresh_token=refresh_token,
            config_update_callback=token_update_handler,
            api_url_base=api['url'], debug_requests=api['debug'],
            request_io_timeouts=api['timeouts'] )

        self.folder_path = folder_path
        self.folder_id = folder_id
        self.folder_id_update_handler = folder_id_update_handler
        self.folder_name = folder_path or folder_id

        self.folder_buckets = folder_buckets
        if folder_buckets != 1:
            self._key_hash = sha1
            self._key_hash_max = khm = 1 << (8 * sha1('').digest_size)
            self._key_hash_max = khm - (khm % folder_buckets) - 1
            self._bucket_format = '{{:0{}d}}'.format(len(str(folder_buckets)))

        self._reactor = override_reactor or reactor

        self.ProtocolError, self.DoesNotExists = ProtocolError, DoesNotExists
        self.ServiceError = ProtocolError
        super(SkyDriveContainer, self).__init__(
            interval=api['tb_interval'], burst=api['tb_burst'] )

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
    def fjoin(*slugs, **kwz):
        key = kwz.pop('key', None)
        assert not kwz, kwz
        if key is not None:
            slugs = list(it.chain(slugs, [encode_key(key)]))
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
    _chunks_misplaced = None # {key: file_id, ...}
    _folds = None # {fold: folder_id, ...}

    @defer.inlineCallbacks
    def _first_result(self, *deferreds):
        try:
            res, idx = yield defer.DeferredList(
                deferreds, fireOnOneCallback=True, fireOnOneErrback=True )
        except defer.FirstError as err: err.subFailure.raiseException()
        defer.returnValue(res)

    @defer.inlineCallbacks
    def _crawl_fold(self, fold, info):
        sublst = list( (fold, ci) for ci in
            (yield self._do_request('listdir', self.client.listdir, info['id'])) )
        if not sublst:
            log.msg( 'Pruning empty subdir: {} (id: {})'\
                .format(fold, info['id']), level=log.OPERATIONAL )
            yield self._do_request('delete empty subdir', self.client.delete, info['id'])
        defer.returnValue((fold, sublst))

    @defer.inlineCallbacks
    def _crawl(self):
        chunks, folds = list(), {'': self.folder_id}
        lst = deque( ('', info) for info in (yield self._mkdir_wrapper(
            lambda: self._do_request('list root', self.client.listdir, self.folder_id) )) )
        lst_futures = dict()

        while lst or lst_futures:
            try: fold, info = lst.popleft()
            except IndexError:
                fold, sublst = yield self._first_result(*lst_futures.viewvalues())
                lst.extend(sublst)
                del lst_futures[fold]
                continue

            if info['type'] == 'folder':
                fold = self.fjoin(fold, info['name'])
                folds[fold] = info['id']
                lst_futures[fold] = self._crawl_fold(fold, info)
            else: chunks.append((fold, info))

        defer.returnValue((chunks, folds))

    @defer.inlineCallbacks
    def list_objects(self, prefix=''):
        if not self._chunks:
            chunk_list, self._folds = yield self._crawl()
            self._chunks_misplaced = dict()
            duplicate_debug, chunks = dict(), dict()
            for fold, info in chunk_list:
                key, cid = decode_key(info['name']), info['id']

                # Detect various duplicates
                if key in chunks:
                    fold_dup, info_dup = duplicate_debug[key]
                    if info_dup['updated_time'] > info['updated_time']:
                        (fold, info, cid), (fold_dup, info_dup) =\
                            (fold_dup, info_dup, info_dup['id']), (fold, info)
                    path_dup, path = self.fjoin(fold_dup, key=key), self.fjoin(fold, key=key)
                    log.msg(( 'Detected two shares with the same key: {path_dup!r}'
                                ' (mtime={mtime_dup}) and {path!r} (mtime={mtime}).'
                            ' Using the latest one (by mtime): {path!r}.'
                            ' Remove the older one ({path_dup!r}, id: {id_dup}) manually'
                            ' to get rid of this message.' )\
                        .format(
                            path_dup=path_dup, mtime_dup=info_dup['updated_time'],
                            path=path, mtime=info['updated_time'], id_dup=info_dup['id'] ), level=log.WEIRD)
                if cid in chunks:
                    raise AssertionError( '(API?) Bug: encountered same file_id'
                        ' twice, should not be possible: {} (key: {})'.format(cid, key) )

                fold_expected = self.key_bucket(key)
                if fold_expected != fold:
                    ## These are somewhat important, but way too noisy on skydrive.folder_buckets update
                    # log.msg(( 'Detected share (key: {}) in an unexpected folder:'
                    # 	' {} (expected: {})' ).format(key, fold, fold_expected), level=log.UNUSUAL )
                    self._chunks_misplaced[key] = fold, cid

                duplicate_debug[key] = fold, info # kept here for debug messages only
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

        # Make sure there won't be two same-key chunks in different folders
        if key in self._chunks_misplaced:
            fold_dup, cid_dup = self._chunks_misplaced[key]
            assert fold != fold_dup, fold_dup
            yield self._do_request('delete duplicate chunk', self.client.delete, cid_dup)
            if fold_dup not in set(it.imap( op.itemgetter(0),
                self._chunks_misplaced.viewvalues() )): del self._folds[fold_dup]


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
