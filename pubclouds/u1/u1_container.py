#-*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from os.path import dirname, basename

from twisted.internet import reactor, defer

from allmydata.node import InvalidValueError, MissingConfigEntry
from allmydata.util import log

from .pubcloud_common import (
	encode_key, decode_key,
	PubCloudItem, PubCloudListing, PubCloudContainer )



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



def join(*path_nodes):
	path = ''
	for node in path_nodes:
		if not node: continue
		if not path.endswith('/'): path += '/'
		path += node.lstrip('/')
	return path


class U1Item(PubCloudItem):

	modification_date_key = 'when_changed'

	def __init__(self, info, **kwz):
		assert 'key' in kwz, kwz
		info['id'] = info['content_path']
		super(U1Item, self).__init__(info, **kwz)

class U1Listing(PubCloudListing): pass


class U1Container(PubCloudContainer):

	def __init__( self, api, data_path,
			auth_consumer, auth_token,
			override_reactor=None ):
		from txu1.api_v1 import txU1, ProtocolError, DoesNotExists

		self.client = txU1(
			auth_consumer=auth_consumer, auth_token=auth_token,
			debug_requests=api['debug'], request_io_timeouts=api['timeouts'] )
		self.data_path = data_path

		self.ProtocolError, self.DoesNotExists = ProtocolError, DoesNotExists
		self.ServiceError = ProtocolError
		self.build_item, self.build_listing = U1Item, U1Listing

		super(U1Container, self).__init__(
			tb_interval=api['tb_interval'], tb_burst=api['tb_burst'],
			override_reactor=override_reactor )

		self._listdir_cache = None # to be filled
		self._listdir_cache_dirs = None # used on put requests

	def __repr__(self):
		return '<{} {!r}>'.format(self.__class__.__name__, self.data_path)


	def _mkdir(self): pass # paths are auto-created here
	def _rmdir(self): raise NotImplementedError('Should not be used in U1 driver')
	def _chunks_flush(self): pass # custom caches here

	def delete(self):
		return self._do_request('delete root', self.client.node_delete, self.data_path)


	@defer.inlineCallbacks
	def _listdir_cache_build(self, path, top=False):
		if top:
			self._listdir_cache = dict()
			self._listdir_cache_dirs = dict()
		try:
			info = yield self._do_request( 'listdir',
				self.err503_wrapper, self.client.node_info, path, children=True )
		except self.DoesNotExists:
			if not top: raise
			defer.returnValue(None) # will be created

		deferreds, dp_len = list(), len(self.data_path)
		for node in info['children']:
			if node['kind'] == 'file':
				assert node['resource_path'].startswith(self.data_path)
				key = node['resource_path'][dp_len:].lstrip('/')
				self._listdir_cache[key] = self.build_item(node, key=key)
			elif node['kind'] == 'directory':
				self._listdir_cache_dirs[node['resource_path']] = node['content_path']
				deferreds.append(self._listdir_cache_build(node['resource_path']))
			else: raise ValueError('Unknown node type: {}'.format(node['kind']))
		yield defer.DeferredList(deferreds) # make sure all sub-listings finish

	@defer.inlineCallbacks
	def list_objects(self, prefix=''):
		yield self._chunks_lock.acquire()
		try:
			if self._listdir_cache is None:
				yield self._listdir_cache_build(self.data_path, top=True)
		finally: self._chunks_lock.release()
		defer.returnValue(
			self.build_listing(self.data_path, prefix, list(
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
			node = yield self._do_request('bucket mkdir', self.client.node_mkdir, path_dir)
			self._listdir_cache_dirs[node['resource_path']] = node['content_path']
			dst = self._listdir_cache_dirs[path_dir]

		node = yield self._do_request( 'put',
			self.client.file_put_into, content_path=dst, name=path_file_name, data=data )
		key = node['resource_path'][len(self.data_path):].lstrip('/')
		self._listdir_cache[key] = self.build_item(node, key=key)

	@defer.inlineCallbacks
	def delete_object(self, key):
		yield self._do_request('delete', self.client.node_delete, join(self.data_path, key))
		del self._listdir_cache[key]


	def get_object(self, key):
		return self._do_request( 'get', self.client.file_get,
			content_path=self._listdir_cache[key].backend_id )

	def head_object(self, key):
		return self._listdir_cache[key]
