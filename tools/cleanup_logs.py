#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

import itertools as it, operator as op, functools as ft
from os.path import dirname, basename, join, splitext, expanduser
from datetime import datetime, timedelta
from subprocess import Popen, PIPE
import os, sys, types, yaml, glob, re, time


_size_units=list(
	reversed(list((u, 2 ** (i * 10))
	for i, u in enumerate('BKMGT'))) )

def size_human(size):
	for u, u1 in _size_units:
		if size > u1: break
	return '{:.1f}{}'.format(size / float(u1), u)

def size_human_parse(size):
	if not size or not isinstance(size, types.StringTypes): return size
	if size[-1].isdigit(): return float(size)
	for u, u1 in _size_units:
		if size[-1] == u: break
	else: raise ValueError('Unrecognized units: {} (value: {!r})'.format(size[-1], size))
	return float(size[:-1]) * u1

def free(path):
	df = os.statvfs(path)
	return float(df.f_bavail * df.f_bsize)


try: from dateutil import parser as dateutil_parser
except ImportError: dateutil_parser = None

_short_ts_days = dict(y=365.25, yr=365.25, mo=30.5, w=7, d=1)
_short_ts_s = dict(h=3600, hr=3600, m=60, min=60, s=1, sec=1)

def _short_ts_regexp():
	'''Generates regexp for parsing of
		shortened relative timestamps, as shown in the table.'''
	ago_re = list('^-?')
	for k in it.chain(_short_ts_days, _short_ts_s):
		ago_re.append(r'(?P<{0}>\d+{0}\s*)?'.format(k))
	ago_re.append(r'(\s+ago\b)?$')
	return re.compile(''.join(ago_re), re.I | re.U)
_short_ts_regexp = _short_ts_regexp()

def parse_timestamp(val):
	'''Match time either in human-readable format (as accepted by dateutil),
		or same time-offset format, as used in the table (e.g. "NdMh ago", or just "NdMh").'''
	val = val.replace('_', ' ')

	# Try to parse time offset in short format, similar to how it's presented
	match = _short_ts_regexp.search(val)
	if match:
		delta = list()
		parse_int = lambda v: int(''.join(c for c in v if c.isdigit()))
		for units in [_short_ts_days, _short_ts_s]:
			val = 0
			for k, v in units.iteritems():
				try:
					if not match.group(k): continue
					n = parse_int(match.group(k))
				except IndexError: continue
				val += n * v
			delta.append(val)
		return datetime.now() - timedelta(*delta)

	# Fallback to other generic formats
	ts = None
	if dateutil_parser: # try dateutil module, if available
		# dateutil fails to parse textual dates like "yesterday"
		try: ts = dateutil_parser.parse(val)
		except ValueError: pass
	if not ts:
		# coreutils' "date" parses virtually everything, but is more expensive to use
		with open(os.devnull, 'w') as devnull:
			proc = Popen(['date', '+%s', '-d', val], stdout=PIPE, stderr=devnull)
			val = proc.stdout.read()
			if not proc.wait(): ts = datetime.fromtimestamp(int(val.strip()))

	if ts: return ts
	raise ValueError('Unable to parse date/time string: {0}'.format(val))

def datetime_to_time(ts):
	return time.mktime(ts.timetuple())


def force_unicode(bytes_or_unicode, encoding='utf-8', errors='replace'):
	if isinstance(bytes_or_unicode, unicode): return bytes_or_unicode
	return bytes_or_unicode.decode(encoding, errors)


def main(argv=None):
	import argparse
	parser = argparse.ArgumentParser(
		description='Cleanup older logfiles.' )
	parser.add_argument('-c', '--config',
		metavar='path', default='{}.yaml'.format(splitext(__file__)[0]),
		help='Path to configuration file (default: %(default)s).')
	parser.add_argument('-f', '--force', action='store_true',
		help='Perform all possible cleanups even if there is enough space.')
	parser.add_argument('--dry-run', action='store_true',
		help='Dont actually remove any of the files.')
	parser.add_argument('--debug',
		action='store_true', help='Verbose operation mode.')
	optz = parser.parse_args(argv if argv is not None else sys.argv[1:])

	import logging
	log = logging.getLogger()
	logging.basicConfig(level=logging.WARNING if not optz.debug else logging.DEBUG)

	with open(optz.config) as conf: conf = yaml.safe_load(conf)

	path_base = conf['base_path']
	os.chdir(path_base)

	lwm, hwm = (size_human_parse(conf['space'].get(k)) for k in ['keep_free', 'warn'])
	assert lwm, 'space.keep_free parameter is required.'


	## Order files for removal
	if optz.force or free(path_base) < lwm:
		for alias, spec in conf['cleanup'].viewitems():
			paths = sorted( it.chain.from_iterable(it.imap(
				glob.iglob, spec['glob'] if not isinstance(
					spec['glob'], types.StringTypes ) else [spec['glob']] )),
				key=lambda p: os.stat(p).st_mtime )
			paths_queue = list() # paths in order of removal-priority

			if paths:
				ts_max = datetime_to_time(parse_timestamp(spec['stale']))

				preserve = set()
				if spec.get('keep_global'):
					preserve.update(paths[-spec['keep_global']:])

				paths_bydir = list()
				for path, paths_dir in it.groupby(
						sorted((dirname(path), path) for path in paths),
						key=op.itemgetter(0) ):
					paths_dir = sorted( # newest to oldest
						it.imap(op.itemgetter(1), paths_dir),
						reverse=True, key=lambda p: os.stat(p).st_mtime )
					if spec.get('keep_for_dir'): preserve.update(paths_dir[:spec['keep_for_dir']])
					for path in paths_dir:
						if path in preserve: continue
						if os.stat(path).st_mtime > ts_max: preserve.add(path) # not stale enough
					paths_bydir.append(paths_dir)

				for n in xrange(max(it.imap(len, paths_bydir))):
					for paths in paths_bydir:
						# Oldest paths are last, cleanup most populated dirs first
						if n >= len(paths): continue
						path = paths[-(n+1)]
						if path in preserve: continue
						paths_queue.append(path)

			log.debug(
				'Queue for alias "{}": {} file(s) ({} preserved)'\
				.format(alias, len(paths_queue), len(preserve)) )
			spec['paths'] = paths_queue


	## Proceed with removals
	while True:
		if not optz.force and free(path_base) >= lwm:
			log.debug('Finished cleanup: free space goal ({}) reached.'.format(size_human(lwm)))
			break

		for alias, spec, rel in sorted(
				( (alias, spec, (spec['importance'] / float(len(spec['paths']))))
					for alias, spec in conf['cleanup'].viewitems() if spec['paths'] ),
				key=op.itemgetter(2) ):
			path = spec['paths'][0]
			log.debug( u'Removing file for "{}" (rel_importnce: {:.2f}): {} (to free: {})'\
				.format(alias, rel, force_unicode(path), size_human(lwm - free(path_base))) )
			if not optz.dry_run: os.unlink(path)
			spec['paths'].remove(path)
			break
		else:
			log.debug('Finished cleanup: no more paths to remove.')
			break

	df = free(path_base)
	if df < hwm:
		log.warn( u'Free space warning (path: {}) - available: {}, threshold: {}'\
			.format(force_unicode(path_base), size_human(df), size_human(hwm)) )
		return 1


if __name__ == '__main__': sys.exit(main())
