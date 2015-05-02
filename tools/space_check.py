#!/usr/bin/env python
#-*- coding: utf-8 -*-

# Simple script to montor/notify-about (lack of) available space on tahoe backends
# See space_check.yaml for usage and configuration info.

from __future__ import print_function

import itertools as it, operator as op, functools as ft
from os.path import dirname, basename, join, splitext, expanduser
import os, sys, types, yaml

from twisted.internet import defer, reactor


def size_human(size, _units=list(
		reversed(list((u, 2 ** (i * 10))
		for i, u in enumerate('BKMGT'))) )):
	for u, u1 in _units:
		if size > u1: break
	return '{:.1f}{}'.format(size / float(u1), u)

def free(path):
	df = os.statvfs(path)
	return float(df.f_bavail * df.f_bsize), float(df.f_blocks * df.f_frsize)


err = 0

def main(argv=None):
	import argparse
	parser = argparse.ArgumentParser(
		description='Tool to monitor free disk space on tahoe cloud backends.')
	parser.add_argument('-c', '--config',
		metavar='path', default='{}.yaml'.format(splitext(__file__)[0]),
		help='Path to configuration file (default: %(default)s).')
	parser.add_argument('--debug',
		action='store_true', help='Verbose operation mode.')
	optz = parser.parse_args(argv if argv is not None else sys.argv[1:])

	import logging
	log = logging.getLogger()
	logging.basicConfig(level=logging.WARNING if not optz.debug else logging.DEBUG)

	with open(optz.config) as conf: conf = yaml.safe_load(conf)

	@defer.inlineCallbacks
	def check_df():
		global err
		threshold = None

		def report(tag, df, ds):
			df_fraction = float(df) / ds
			df_msg = '{} space{{}}: {} / {} ({:.1f}% free, threshold: {:.1f}%)'\
				.format(tag, size_human(df), size_human(ds), df_fraction * 100, threshold * 100)
			if df_fraction < threshold:
				log.error(df_msg.format(' alert'))
				return 1
			else: log.debug(df_msg.format(''))
			return 0

		for driver, clouds in conf['backends'].viewitems():
			for tag, params in clouds.viewitems():
				threshold = params.get('threshold', conf['threshold'])\
					if isinstance(params, dict) else conf['threshold']

				if driver == 'vfs':
					err += report(tag, *free(expanduser( params
						if isinstance(params, types.StringTypes) else params['path'] )))

				elif driver == 'txskydrive':
					from txskydrive import api_v5 as skydrive
					api = skydrive.txSkyDrivePersistent.from_conf()
					err += report(tag, *(yield api.get_quota()))

				elif driver == 'txboxdotnet':
					from txboxdotnet import api_v2 as box
					api = box.txBoxPersistent.from_conf()
					err += report(tag, *(yield api.get_quota()))

				elif driver == 'txu1':
					from txu1 import api_v1 as u1
					api = u1.txU1Persistent.from_conf()
					err += report(tag, *(yield api.get_quota()))

	def done(res):
		if reactor.running: reactor.stop()
		return res

	reactor.callWhenRunning(
		lambda: defer.maybeDeferred(check_df).addBoth(done) )
	reactor.run()
	return err


if __name__ == '__main__': sys.exit(main())
