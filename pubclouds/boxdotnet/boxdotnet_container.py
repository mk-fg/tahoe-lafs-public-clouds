#-*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from datetime import datetime
import re, json

from twisted.internet import reactor, defer

from allmydata.node import InvalidValueError, MissingConfigEntry
from allmydata.util import log

from .pubcloud_common import (
	encode_key, decode_key,
	PubCloudItem, PubCloudListing, PubCloudContainer )



def configure_boxdotnet_container(storedir, config):
	from txboxdotnet.api_v2 import txBox

	client_id = config.get_config('storage', 'box.client_id')
	client_secret = config.get_private_config('box_client_secret')

	try: auth_code = config.get_private_config('box_auth_code')
	except MissingConfigEntry:
		api = txBox(client_id=client_id, client_secret=client_secret)
		raise MissingConfigEntry(
			'\n\n'
			'Visit the following URL in any web browser (firefox, chrome, safari, etc),\n'
				'  authorize there, confirm access permissions, and paste URL of an empty page\n'
				'  (starting with "https://success.box.com/") you will get\n'
				'  redirected to in the end into "private/box_auth_code" file.\n\n'
			' See "Authorization" section in "doc/cloud.rst" for details.\n\n'
			' URL to visit: {}\n'.format(api.auth_user_get_url()) )

	if re.search(r'^https?://', auth_code):
		api = txBox(client_id=client_id, client_secret=client_secret)
		api.auth_user_process_url(auth_code)
		auth_code = api.auth_code
		config.write_private_config('box_auth_code', api.auth_code)

	access_token = config.get_optional_private_config('box_access_token')
	refresh_token = config.get_optional_private_config('box_refresh_token')

	api_timeouts = dict()
	for k in txBox.request_io_timeouts:
		k_conf = 'box.api.timeout.{}'.format(k)
		v = config.get_config('storage', k_conf, None)
		if v is None:
			continue
		v = float(v)
		if v < 0:
			raise InvalidValueError('{} value must be a positive integer or zero.'.format(k_conf))
		api_timeouts[k] = v

	api_parameters = dict(
		debug = config.get_config('storage', 'box.api.debug', False, boolean=True),
		tb_interval = float(config.get_config('storage', 'box.api.ratelimit.interval', 0)),
		tb_burst = int(config.get_config('storage', 'box.api.ratelimit.burst', 1)),
		timeouts=api_timeouts )
	if api_parameters['tb_interval'] < 0:
		raise InvalidValueError(
			'box.api.ratelimit.interval value must be either positive or zero.' )
	if api_parameters['tb_burst'] <= 0:
		raise InvalidValueError('box.api.ratelimit.burst value must be a positive integer.')

	folder_id = config.get_config('storage', 'box.folder_id', None)
	folder_path = config.get_config('storage', 'box.folder_path', None)

	folder_buckets = int(config.get_config('storage', 'box.folder_buckets', 1))
	if folder_buckets < 1:
		raise InvalidValueError('box.folder_buckets value must be a positive integer.')

	if not folder_id and not folder_path:
		raise InvalidValueError('Either box.folder_id or box.folder_path must be specified.')
	elif folder_id and folder_path:
		raise InvalidValueError( 'Only one of box.folder_id'
			' or box.folder_path must be specified, not both.' )
	elif not folder_id:
		folder_id = config.get_optional_private_config('box_folder_id')
		folder_id_created = True
	else:
		folder_id_created = False

	def token_update_handler(auth_access_token, auth_refresh_token, **kwargs):
		config.write_private_config('box_access_token', auth_access_token)
		config.write_private_config('box_refresh_token', auth_refresh_token)
		if kwargs:
			log.msg( 'Received unhandled box.net access'
				' data, discarded: {}'.format(', '.join(kwargs.keys())), level=log.WEIRD )

	def folder_id_update_handler(folder_id):
		config.write_private_config('box_folder_id', folder_id)

	container = BoxContainer(
		api_parameters, folder_id, folder_path,
		client_id, client_secret, auth_code,
		folder_buckets=folder_buckets,
		token_update_handler=token_update_handler,
		folder_id_update_handler=folder_id_update_handler,
		access_token=access_token, refresh_token=refresh_token, )

	return container



class BoxItem(PubCloudItem):
	modification_date_key = 'content_modified_at'

class BoxListing(PubCloudListing): pass


class BoxContainer(PubCloudContainer):

	def __init__( self, api,
			folder_id, folder_path,
			client_id, client_secret, auth_code,
			folder_buckets=1,
			token_update_handler=None,
			folder_id_update_handler=None,
			access_token=None, refresh_token=None,
			override_reactor=None ):
		from txboxdotnet.api_v2 import txBoxPluggableSync, ProtocolError, DoesNotExists

		self.client = txBoxPluggableSync(
			client_id=client_id, client_secret=client_secret, auth_code=auth_code,
			auth_access_token=access_token, auth_refresh_token=refresh_token,
			config_update_callback=token_update_handler,
			debug_requests=api['debug'], request_io_timeouts=api['timeouts'] )

		self.folder_path = folder_path
		self.folder_id = folder_id
		self.folder_id_update_handler = folder_id_update_handler
		self.folder_name = folder_path or folder_id
		if self.folder_id is None: reactor.callLater(1, self._mkdir_root)

		self._key_buckets_init(folder_buckets)

		self.ProtocolError, self.DoesNotExists = ProtocolError, DoesNotExists
		self.ServiceError = ProtocolError
		self.build_item, self.build_listing = BoxItem, BoxListing

		super(BoxContainer, self).__init__(
			tb_interval=api['tb_interval'], tb_burst=api['tb_burst'],
			override_reactor=override_reactor )


	def _listdir(self, folder_id):
		return self.client.listdir(folder_id, fields=['name', 'size', 'content_modified_at'])

	def _rmdir(self, folder_id, recursive=False):
		return self.client.delete_folder(folder_id, recursive=recursive)


	@defer.inlineCallbacks
	def put_object(self, key, data, content_type=None, metadata=None):
		assert not content_type, content_type
		assert not metadata, metadata

		fold = self.key_bucket(key)
		@defer.inlineCallbacks
		def _upload_chunk():
			name, tried_cleanup = encode_key(key), False
			for fail in xrange(5): # arbitrary 3+ number, to avoid infinite loops
				try: defer.returnValue((yield self.client.put((name, data), self._folds[fold])))
				except self.ProtocolError as err: # handle conflicts by find/replacing the old file
					if err.code != 409: raise
					chunk_id = None
					try: chunk_id = self._chunks[key].backend_id
					except KeyError: pass
					err_body = getattr(err, 'body', None) # newer txboxdotnet
					if chunk_id is None and err_body:
						try:
							err_body = json.loads(err_body)
							assert err_body['code'] == 'item_name_in_use'
							chunk_id = err_body['context_info']['conflicts'][0]['id']
						except (ValueError, KeyError, IndexError, AssertionError):
							log.msg( 'Failed to process request'
								' error json: {!r}'.format(err_body), level=log.UNUSUAL )
					if chunk_id is None:
						try: chunk_id = yield self.client.resolve_path(name, root_id=self._folds[fold])
						except self.DoesNotExists: # 409 should mean "it exists!" - api bug/race?
							if fail > 1: raise err
							continue # try 1-2 more times (first attempt might've done mkdir)
					defer.returnValue((yield self.client.put((name, data), file_id=chunk_id)))
				except KeyError:
					if fail: raise
					self._folds[fold] = yield self._mkdir(fold)
				else: break
			else: raise RuntimeError('Expected loop break missing')
		info = (yield self._mkdir_wrapper(
			ft.partial(self._do_request, 'upload', _upload_chunk), fold ))['entries'][0]

		# Make sure there won't be two same-key chunks in different folders
		if key in self._chunks_misplaced:
			fold_dup, cid_dup = self._chunks_misplaced[key]
			assert fold != fold_dup, fold_dup
			yield self._do_request('delete duplicate chunk', self.client.delete_file, cid_dup)
			del self._chunks[cid_dup], self._chunks_misplaced[key]

		info['size'] = len(data)
		info['content_modified_at'] = datetime.utcnow().isoformat()
		self._chunks[key] = self._chunks[info['id']] = BoxItem(info, key=key)


	@defer.inlineCallbacks
	def delete_object(self, key):
		chunk_id = self._chunks[key].backend_id
		yield self._do_request('delete', self.client.delete_file, chunk_id)
		del self._chunks[key], self._chunks[chunk_id]
