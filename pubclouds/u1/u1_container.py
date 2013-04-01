#-*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from time import time

from zope.interface import implements
from twisted.internet import reactor, defer

from allmydata.node import InvalidValueError, MissingConfigEntry
from allmydata.storage.backends.cloud.cloud_common import (
	IContainer, ContainerRetryMixin, CloudError,
	CloudServiceError, ContainerItem, ContainerListing,
	CommonContainerMixin, HTTPClientMixin )
from allmydata.util import log


def configure_u1_container(storedir, config):
	from txu1.api_v1 import txU1

	try:
		u1_consumer = map(
			config.get_private_config,
			['u1_consumer_key', 'u1_consumer_secret'] )
		u1_token = map(
			config.get_private_config,
			['u1_token', 'u1_token_secret'] )
		if not all(u1_consumer + u1_token): raise MissingConfigEntry()
	except MissingConfigEntry:
		raise MissingConfigEntry(
			'\n\n'
			'Ubuntu One OAuth consumer/token data seem to be missing.\n'
			'Create following files:\n'
				'  private/u1_consumer_key\n'
				'  private/u1_consumer_secret\n'
				'  private/u1_token\n'
				'  private/u1_token_secret\n'
			'This auth data can be generated (from Ubuntu SSO email/password)'
			' by using example code in txu1 module.\n\n' )

	api_timeouts = dict()
	for k in txU1.request_io_timeouts:
		k_conf = 'u1.api.timeout.{}'.format(k)
		v = config.get_config('storage', k_conf, None)
		if v is None:
			continue
		v = float(v)
		if v < 0:
			raise InvalidValueError('{} value must be a positive integer or zero.'.format(k_conf))
		api_timeouts[k] = v

	api_parameters = dict(
		debug = config.get_config('storage', 'u1.api.debug', False, boolean=True),
		tb_interval = float(config.get_config('storage', 'u1.api.ratelimit.interval', 0)),
		tb_burst = int(config.get_config('storage', 'u1.api.ratelimit.burst', 1)),
		timeouts=api_timeouts )
	if api_parameters['tb_interval'] < 0:
		raise InvalidValueError(
			'u1.api.ratelimit.interval value must be either positive or zero.' )
	if api_parameters['tb_burst'] <= 0:
		raise InvalidValueError('u1.api.ratelimit.burst value must be a positive integer.')

	data_path = config.get_config('storage', 'u1.path')
	return U1Container(api_parameters, data_path, u1_consumer, u1_token)



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



class U1Item(ContainerItem):
	# 'key', 'modification_date', 'etag', 'size', 'storage_class', 'owner'

	backend_id = None
	storage_class = 'STANDARD'

	def __init__(self, info, **kwz):
		self.backend_id = kwz.pop('backend_id', None) or info['content_path']
		super(BoxItem, self).__init__(
			kwz.pop('key')
			kwz.pop('modification_date', None) or info['when_changed'], # modification_date
			self.backend_id, # etag
			kwz.pop('size', None) or info['size'], # size
			self.storage_class ) # storage_class
		for k, v in kwz.viewitems(): setattr(self, k, v) # extras


class U1Listing(ContainerListing):
	# 'name', 'prefix', 'marker', 'max_keys', 'is_truncated', 'contents', 'common_prefixes'

	max_keys = 2**30
	is_truncated = 'false'

	def __init__(self, name, prefix, contents):
		super(BoxListing, self).__init__(
			name, prefix, None, self.max_keys, self.is_truncated, contents=contents )



def join(*path_nodes):
	path = ''
	for node in path_nodes:
		if not node: continue
		if not path.endswith('/'): path += '/'
		path += node.lstrip('/')
	return path



class U1Container(ContainerRateLimitMixin, ContainerRetryMixin):
	implements(IContainer)

	def __init__( self, api, data_path,
			auth_consumer, auth_token,
			override_reactor=None ):
		from txu1.api_v1 import txU1, ProtocolError, DoesNotExists

		self.client = txU1(
			auth_consumer=auth_consumer, auth_token=auth_token,
			debug_requests=api['debug'], request_io_timeouts=api['timeouts'] )
		self.data_path = data_path

		self._reactor = override_reactor or reactor

		self.ProtocolError, self.DoesNotExists = ProtocolError, DoesNotExists
		self.ServiceError = ProtocolError
		super(U1Container, self).__init__(
			interval=api['tb_interval'], burst=api['tb_burst'] )

		self._listdir_cache = None # to be filled
		self._listdir_cache_dirs = None # used on put requests
		self._listdir_cache_lock = defer.DeferredLock()

	def __repr__(self):
		return '<{} {!r}>'.format(self.__class__.__name__, self.data_path)


	def create(self): pass # paths are auto-created here

	def delete(self):
		return self._do_request('delete root', self.client.node_delete, self.data_path)


	@defer.inlineCallbacks
	def _listdir_cache_build(self, path, top=False):
		if top:
			self._listdir_cache = dict()
			self._listdir_cache_dirs = dict()
		try: info = yield self._do_request('listdir', self.client.node_info, path, children=True)
		except DoesNotExists:
			if not top: raise
			defer.returnValue(None) # will be created

		deferreds, dp_len = list(), len(self.data_path)
		for node in info['children']:
			if node['kind'] == 'file':
				assert node['resource_path'].startswith(self.data_path)
				key = node['resource_path'][dp_len:].lstrip('/')
				log.msg('---------- Share key: {!r}'.format(key))
				self._listdir_cache[key] = U1Item(node, key=key)
			elif node['kind'] == 'directory':
				self._listdir_cache_dirs[node['resource_path']] = node['content_path']
				deferreds.append(
					self._listdir_cache_build(self, node['resource_path']) )
			else: raise ValueError('Unknown node type: {}'.format(node['kind']))
		yield defer.DeferredList(deferreds) # make sure all sub-listings finish

	@defer.inlineCallbacks
	def list_objects(self, prefix=''):
		log.msg('---------- List prefix: {!r}'.format(prefix))
		yield self._listdir_cache_lock.acquire()
		try:
			if self._listdir_cache is None:
				yield self._listdir_cache_build(self.data_path)
		finally: self._listdir_cache_lock.release()
		defer.returnValue(
			U1Listing(self.data_path, prefix, list(
				item for key, item in self._listdir_cache.viewitems()
				if not prefix or key.startswith(prefix) )) )


	@defer.inlineCallbacks
	def put_object(self, key, data, content_type=None, metadata=None):
		assert not content_type, content_type
		assert not metadata, metadata

		path_file = join(self.data_path, key)
		path_dir, path_file_name = dirname(path_file), basename(path_file)

		try: dst = self._listdir_cache_dirs[path_dir]
		except KeyError:
			log.msg( '---------- Dir cache miss: {!r}, random key: {!r}'\
				.format(path_dir, random.choose(list(self._listdir_cache_dirs))) )
			node = yield self._do_request('bucket mkdir', self.client.node_mkdir, path_dir)
			self._listdir_cache_dirs[node['resource_path']] = node['content_path']
			dst = self._listdir_cache_dirs[path_dir]

		node = yield self._do_request( 'put',
			self.client.file_put_into, content_path=dst, name=path_file_name, data=data )
		key = node['resource_path'][len(self.data_path):].lstrip('/')
		log.msg('---------- New share key: {!r}'.format(key))
		self._listdir_cache[key] = U1Item(node, key=key)

	@defer.inlineCallbacks
	def delete_object(self, key):
		yield self._do_request('delete', self.client.node_delete, join(self.data_path, key))
		del self._listdir_cache[key]


	def get_object(self, key):
		return self._do_request( 'get', self.client.file_get,
			content_path=self._listdir_cache[key].backend_id )

	def head_object(self, key):
		return self._listdir_cache[key]
