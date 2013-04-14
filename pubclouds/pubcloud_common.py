#-*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from time import time
from collections import deque
import re, json

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



class PubCloudItem(ContainerItem):
	# 'key', 'modification_date', 'etag', 'size', 'storage_class', 'owner'

	backend_id = None
	storage_class = 'STANDARD'

	modification_date_key = None

	def __init__(self, info, **kwz):
		assert self.modification_date_key, self.modification_date_key
		self.backend_id = kwz.pop('backend_id', None) or info['id']
		super(PubCloudItem, self).__init__(
			kwz.pop('key', None) or decode_key(info['name']), # key
			kwz.pop('modification_date', None) or info[self.modification_date_key], # modification_date
			self.backend_id, # etag
			kwz.pop('size', None) or info['size'], # size
			self.storage_class ) # storage_class
		for k, v in kwz.viewitems(): setattr(self, k, v) # extras


class PubCloudListing(ContainerListing):
	# 'name', 'prefix', 'marker', 'max_keys', 'is_truncated', 'contents', 'common_prefixes'

	max_keys = 2**30
	is_truncated = 'false'

	def __init__(self, name, prefix, contents):
		super(PubCloudListing, self).__init__(
			name, prefix, None, self.max_keys, self.is_truncated, contents=contents )




class PubCloudContainer(ContainerRateLimitMixin, ContainerRetryMixin):
	implements(IContainer)

	modification_date_key = None
	build_item = build_listing = None
	ProtocolError = DoesNotExists = ServiceError = None

	def __init__(self, tb_interval=None, tb_burst=None, override_reactor=None):
		self._reactor = override_reactor or reactor
		self._chunks_lock = defer.DeferredLock()
		self._chunks_flush()
		super(PubCloudContainer, self).__init__(interval=tb_interval, burst=tb_burst)

		attrs = self.ProtocolError, self.DoesNotExists, self.ServiceError
		assert all(attrs), attrs
		attrs = [self.build_item, self.build_listing]
		assert all(attrs), attrs

		if not self.modification_date_key: # try to get it from item type
			if not isinstance(self.build_item, type):
				self.modification_date_key = self.build_item.modification_date_key
			assert self.modification_date_key, self.modification_date_key


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
	def _mkdir_root(self):
		self.folder_id = yield self._mkdir()
		self.folder_id_update_handler(self.folder_id)

	@defer.inlineCallbacks
	def _mkdir_wrapper(self, func, path=''):
		try: defer.returnValue((yield defer.maybeDeferred(func)))
		except self.ProtocolError as err: http_code = err.code
		except CloudError as err: http_code = err.args[1]
		if http_code not in [http.NOT_FOUND, http.GONE]: raise
		yield self._mkdir_root()
		yield self._mkdir(path)
		defer.returnValue((yield defer.maybeDeferred(func)))


	def create(self):
		return self._mkdir()

	@defer.inlineCallbacks
	def delete(self):
		yield self._do_request( 'delete root',
			self.client.delete_folder, self.folder_id, recursive=True )
		self._chunks_flush()



	def _listdir(self, folder_id):
		raise NotImplementedError()

	def _rmdir(self, folder_id):
		raise NotImplementedError()


	_chunks = None # {file_id1: info1, file_key1: info1, ...}
	_chunks_misplaced = None # {key: file_id, ...}
	_folds = None # {fold: folder_id, ...}

	def _chunks_flush(self):
		self._chunks = self._chunks_misplaced = self._folds = None

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
			(yield self._do_request('listdir', self._listdir, info['id'])) )
		if not sublst:
			log.msg( 'Pruning empty subdir: {} (id: {})'\
				.format(fold, info['id']), level=log.OPERATIONAL )
			yield self._do_request('delete empty subdir', self._rmdir, info['id'])
		defer.returnValue((fold, sublst))

	@defer.inlineCallbacks
	def _crawl(self):
		chunks, folds = list(), {'': self.folder_id}
		lst = deque( ('', info) for info in (yield self._mkdir_wrapper(
			lambda: self._do_request('list root', self._listdir, self.folder_id) )) )
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
	def _chunks_find(self):
		chunk_list, self._folds = yield self._crawl()
		self._chunks_misplaced = dict()
		duplicate_debug, chunks = dict(), dict()
		for fold, info in chunk_list:
			key, cid = decode_key(info['name']), info['id']

			# Detect various duplicates
			if key in chunks:
				fold_dup, info_dup = duplicate_debug[key]
				if info_dup[self.modification_date_key] > info[self.modification_date_key]:
					(fold, info, cid), (fold_dup, info_dup) =\
						(fold_dup, info_dup, info_dup['id']), (fold, info)
				path_dup, path = self.fjoin(fold_dup, key=key), self.fjoin(fold, key=key)
				log.msg(( 'Detected two shares with the same key: {path_dup!r}'
							' (id={id_dup}, mtime={mtime_dup}) and {path!r} (id={id}, mtime={mtime}).'
						' Using the latest one (by mtime): {path!r}.'
						' Remove the older one ({path_dup!r}, id: {id_dup}) manually'
							' to get rid of this message.' ).format(
					id=info['id'], id_dup=info_dup['id'],
					path_dup=path_dup, mtime_dup=info_dup[self.modification_date_key],
					path=path, mtime=info[self.modification_date_key] ), level=log.WEIRD)
				if cid in chunks: continue # using already processed one
			elif cid in chunks: # in case of duplicates, might be already recorded
				raise AssertionError( '(API?) Bug: encountered same file_id'
					' twice, should not be possible: {} (key: {})'.format(cid, key) )

			fold_expected = self.key_bucket(key)
			if fold_expected != fold:
				## These are somewhat important, but way too noisy on *.folder_buckets update
				# log.msg(( 'Detected share (key: {}) in an unexpected folder:'
				# 	' {} (expected: {})' ).format(key, fold, fold_expected), level=log.UNUSUAL )
				self._chunks_misplaced[key] = fold, cid

			duplicate_debug[key] = fold, info # kept here for debug messages only
			chunks[key] = chunks[cid] = self.build_item(info, key=key)
		self._chunks = chunks


	@defer.inlineCallbacks
	def list_objects(self, prefix=''):
		yield self._chunks_lock.acquire()
		try:
			if self._chunks is None:
				yield self._chunks_find()
		finally: self._chunks_lock.release()
		defer.returnValue(
			self.build_listing(self.folder_name, prefix, list(
				item for key, item in self._chunks.viewitems()
				if key != item.backend_id\
					and (not prefix or key.startswith(prefix)) )) )


	def put_object(self, key, data, content_type=None, metadata=None):
		raise NotImplementedError()

	def delete_object(self, key):
		raise NotImplementedError()


	def get_object(self, key):
		return self._do_request('get', self.client.get, self._chunks[key].backend_id)

	def head_object(self, key):
		return self._chunks[key]
