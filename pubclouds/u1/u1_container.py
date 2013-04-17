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

	dir_buckets = int(config.get_config('storage', 'u1.dir_buckets', 1))
	if dir_buckets < 1:
		raise InvalidValueError('u1.dir_buckets value must be a positive integer.')

	data_path = config.get_config('storage', 'u1.path')
	return U1Container(api_parameters, data_path, dir_buckets, u1_consumer, u1_token)



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
		# path is used for delete and info, id for get
		if 'id' not in info: kwz.setdefault('backend_id', info['content_path'])
		if 'path' not in info: kwz.setdefault('path', info['resource_path'])
		if 'name' not in info: info['name'] = basename(info['resource_path'].rstrip('/'))
		super(U1Item, self).__init__(info, **kwz)

class U1Listing(PubCloudListing): pass


class U1Container(PubCloudContainer):

	def __init__( self, api, data_path,
			auth_consumer, auth_token,
			dir_buckets=1, override_reactor=None ):
		from txu1.api_v1 import txU1, ProtocolError, DoesNotExists

		self.client = txU1(
			auth_consumer=auth_consumer, auth_token=auth_token,
			debug_requests=api['debug'], request_io_timeouts=api['timeouts'] )
		self.data_path = data_path

		self.folder_id = self.folder_name = self.data_path
		self._key_buckets_init(dir_buckets)
		self._listdir_cache = dict() # {resource_path: content_path, ...} for one-req puts

		self.ProtocolError, self.DoesNotExists = ProtocolError, DoesNotExists
		self.ServiceError = ProtocolError
		self.build_item, self.build_listing = U1Item, U1Listing

		super(U1Container, self).__init__(
			tb_interval=api['tb_interval'], tb_burst=api['tb_burst'],
			override_reactor=override_reactor )


	# Paths are auto-created here
	@defer.inlineCallbacks
	def _mkdir_wrapper(self, func, path=''):
		try: defer.returnValue((yield defer.maybeDeferred(func)))
		except self.ProtocolError as err: http_code = err.code
		except CloudError as err: http_code = err.args[1]
		if http_code not in [http.NOT_FOUND, http.GONE]: raise
		defer.returnValue(list())
	def _mkdir(self): pass

	def _rmdir(self): raise NotImplementedError('Should not be used in U1 driver')
	def _chunks_flush(self): pass # custom caches here

	def delete(self):
		return self._do_request('delete root', self.client.node_delete, self.data_path)


	@defer.inlineCallbacks
	def _listdir(self, path):
		res = yield self.client.node_info(path, children=True)
		# Adapt to {name: encoded_share_path, type: ..., id: ...} convention other clouds use
		lst = list()
		for info in res['children']:
			info['type'] = 'folder' if info['kind'] == 'directory' else 'file'
			if info['type'] == 'folder':
				info['id'] = info['resource_path']
				self._listdir_cache[info['id']] = info['content_path']
			else:
				info['id'] = info['content_path']
				info['name'] = basename(info['resource_path'].rstrip('/'))
			lst.append(info)
		defer.returnValue(lst)


	@defer.inlineCallbacks
	def put_object(self, key, data, content_type=None, metadata=None):
		assert not content_type, content_type
		assert not metadata, metadata

		fold = self.key_bucket(key)
		dst_id = self._folds[fold]
		try: dst = self._listdir_cache[dst_id]
		except KeyError:
			info = yield self._do_request( 'bucket mkdir',
				self.client.node_mkdir, join(self.data_path, fold) )
			self._listdir_cache[dst_id] = info['content_path']
			dst = self._listdir_cache[dst_id]

		item = self.build_item((yield self._do_request( 'upload',
			self.client.file_put_into, content_path=dst, name=encode_key(key), data=data )))

		# Make sure there won't be two same-key chunks in different folders
		if key in self._chunks_misplaced:
			fold_dup, cid_dup = self._chunks_misplaced[key]
			assert fold != fold_dup, fold_dup
			info_dup = self._chunks[cid_dup]
			yield self._do_request('delete duplicate chunk', self.client.node_delete, info_dup['path'])
			del self._chunks[cid_dup], self._chunks_misplaced[key]

		self._chunks[key] = self._chunks[item.backend_id] = item

	@defer.inlineCallbacks
	def delete_object(self, key):
		item = self._chunks[key]
		yield self._do_request('delete', self.client.node_delete, item.path)
		del self._chunks[key], self._chunks[item.backend_id]


	def get_object(self, key):
		return self._do_request( 'get', self.client.file_get,
			content_path=self._chunks[key].backend_id )
