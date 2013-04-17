#-*- coding: utf-8 -*-

import itertools as it, operator as op, functools as ft
from datetime import datetime
import re

from twisted.internet import reactor, defer

from allmydata.node import InvalidValueError, MissingConfigEntry
from allmydata.util import log

from .pubcloud_common import (
	encode_key, decode_key,
	PubCloudItem, PubCloudListing, PubCloudContainer )



def configure_skydrive_container(storedir, config):
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
		api = txSkyDrive(client_id=client_id, client_secret=client_secret)
		api.auth_user_process_url(auth_code)
		auth_code = api.auth_code
		config.write_private_config('skydrive_auth_code', auth_code)

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
		config.write_private_config('skydrive_folder_id', folder_id)

	container = SkyDriveContainer(
		api_parameters, folder_id, folder_path,
		client_id, client_secret, auth_code,
		folder_buckets=folder_buckets,
		token_update_handler=token_update_handler,
		folder_id_update_handler=folder_id_update_handler,
		access_token=access_token, refresh_token=refresh_token, )

	return container



class SkyDriveItem(PubCloudItem):
	modification_date_key = 'updated_time'

class SkyDriveListing(PubCloudListing): pass


class SkyDriveContainer(PubCloudContainer):

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
		if self.folder_id is None: reactor.callLater(1, self._mkdir_root)

		self._key_buckets_init(folder_buckets)

		self.ProtocolError, self.DoesNotExists = ProtocolError, DoesNotExists
		self.ServiceError = ProtocolError
		self.build_item, self.build_listing = SkyDriveItem, SkyDriveListing

		super(SkyDriveContainer, self).__init__(
			tb_interval=api['tb_interval'], tb_burst=api['tb_burst'],
			override_reactor=override_reactor )


	def _listdir(self, folder_id):
		return self.client.listdir(folder_id)

	def _rmdir(self, folder_id, recursive=None):
		return self.client.delete(folder_id) # always recursive here


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
