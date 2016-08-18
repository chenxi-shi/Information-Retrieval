#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
from re import compile
from re import compile
import socket
import functools
import time
# import math
import gc
from stemming.porter2 import stem
# noinspection PyCompatibility
from urllib.parse import urlunparse, urlparse
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import TimeoutError
from Node_Part_Graph import Node
from elasticsearch_func import *

# import lxml

__author__ = "Chenxi Shi"
__copyright__ = "Copyright 2016, Information Retrieval hw3"
__version__ = "3.2.2"
__maintainer__ = "Chenxi Shi"
__email__ = "shi.che@husky.neu.edu"
__status__ = "Development"


class bfs_Wave:
	"""
	This class contral the process of crawling,
	including initialize starting links instantces,
	doing whole_work of Node class,
	recording deep info into Node.graph
	"""
	page_total = 0  # num of all page downloaded
	page_count = 0  # num of pages put into elasticsearch
	all_download_page_score = []  # sorted list of scores
	tt_doc_len = 0
	_child_deep = 1
	avg_doc_len = 500

	def __init__(self, _wave, _deep):
		self.wave = _wave  # sorted list of _next_wave_domain by last accessed time
		self.total = 0  # num of total url in wave
		self.current_node_num = 0
		self.wave_size = 0  # num of url in this wave
		# self.next_wave = set()
		self.deep = _deep
		self.wave_url_ids = set()
		self.wave_finished = 0
		self.gc_timer = 0

	@classmethod
	def from_start_links(cls, _start_url):
		_wave0 = Node.from_start_links(_start_url)  # set of Node instances
		return cls(_wave0, 0)  # return wave instance

	def __repr__(self):
		return 'Wave length ({!r})'.format(len(self.wave))

	@staticmethod
	def get_next_wave():
		print('starting get next wave')
		next_wave = {}
		domain_urls = Node.domain_urls
		for _domain, _detail in domain_urls.items():
			if _detail['next_wave']:
				next_wave[_domain] = _detail['last_time']
		next_wave = sorted(next_wave, key=next_wave.get)  # list of sorted domain
		return next_wave

	'''
	_source:
	{
	'docno': deep+domainno+pathno
	'HTTPheader': HTTPheader,
	'title': title,
	'text': text,
	'html_Source': html_Source,
	'in_links': in_links,
	'out_links': out_links,
	'author': Chenxi,
	'depth': depth,
	'url': url
	}
	'''

	@staticmethod
	def sorted_insert(_sorted_list, inserted_item):  # high to low
		lo = 0  # high tf in low index
		hi = len(_sorted_list)  # low tf in high index
		while lo < hi:
			mid = (lo + hi) // 2
			if inserted_item == _sorted_list[mid]:
				lo = mid + 1
				break
			elif inserted_item > _sorted_list[mid]:
				hi = mid
			else:
				lo = mid + 1
		return lo

	# it is a round_robin select method
	# @staticmethod
	def rr_select(self, _sorted_wave_domain):  # it is a sorted list
		last = len(_sorted_wave_domain) - 1
		self.total = 0
		while _sorted_wave_domain:
			current = (last + 1) % len(_sorted_wave_domain)
			_domain = _sorted_wave_domain[current]

			if Node.domain_urls[_domain]['next_wave']:
				_path, _parent_score = Node.domain_urls[_domain]['next_wave'].popitem()
				self.total += 1
				print('Total {} ==> PATH {} '.format(self.total, _path))
				last = current
				yield {'url': urlunparse((Node.domain_urls[_domain]['scheme'], _domain, _path, '', '', '')),
					   'parent_score': _parent_score}
			else:
				print('{} empty, remove'.format(_domain))
				_sorted_wave_domain.remove(_domain)
				last = current - 1
				continue

	def ram_clear(self, page_max):
		# Clear the memory
		collect = gc.collect
		self.gc_timer += 1
		if self.gc_timer > page_max:
			collected = collect()
			print('\nGarbage collector: collected {} objects.\n'.format(collected))
			self.gc_timer = 0

	def one_node_work(self, node_detail, _terms_list):
		"""
		Download & parse one page, then restore it into medium index
		:param node_detail:
		:param _terms_list:
		:return:
		"""
		node_instance = Node(node_detail['url'])
		parent_score = node_detail['parent_score']
		_html_info = node_instance.download_html()
		self.current_node_num += 1
		if _html_info:
			bfs_Wave.page_total += 1
			try:
				content_type = node_instance.HTTPheader['content-type']
			except:
				content_type = ''
			node_instance.parse_page(content_type)  # parse the page
			# strategy: filter current wave page with _okapi_score >0.3  <-- ttf <=1
			_okapi_score, _doc_len = okapi_tf(_terms_list, bfs_Wave.avg_doc_len, node_instance.text)
			if _okapi_score <= 0.6:
				print("{}/{}, Low score: {}, GIVEUP {}".format(self.current_node_num, self.total, _okapi_score,
															   node_instance.normed_url))
				self.ram_clear(300)
				return
			self.wave_size += 1
			self.wave_url_ids.add(node_instance.normed_url)
			# for download page, restore it into Node.domain_urls
			Node.domain_urls[node_instance.nmed_url_domain]['accessed'][
				node_instance.nmed_url_path] = _okapi_score * 0.8 + parent_score
			# load docs to medium index
			_source = {'docno': node_instance.normed_url,  # a string
					   'HTTPheader': '\n'.join(
						   '{}: {}'.format(k, v) for k, v in eval(str(node_instance.HTTPheader)).items()),  # a string
					   'title': node_instance.title,  # a string
					   'text': ''.join(node_instance.text),  # a string
					   'html_Source': node_instance.html,  # a string
					   'href': '\n'.join(node_instance.href),  # a string for medium
					   'score': _okapi_score * 0.8 + parent_score,  # a string for medium
					   'doc_len': _doc_len,  # a int
					   'author': 'Chenxi',  # a string
					   'depth': self.deep,  # a int
					   'url': node_instance.normed_url  # a string
					   }
			load_to_elasticsearch(es, medium_index, my_type, _source, node_instance.normed_url)

			# for download page, restore it's score into all_download_page_score, for filter low score page children
			bfs_Wave.all_download_page_score.insert(
				bfs_Wave.sorted_insert(bfs_Wave.all_download_page_score, _okapi_score), _okapi_score)
			print('{}/{}, Score: {}, Downloaded {}'.format(self.current_node_num, self.total, _okapi_score,
														   node_instance.normed_url))
		# This part can create _docno like deep-domain-pathhash
		# _parsed_url = urlparse(node_instance.normed_url)
		# _url_netloc = _parsed_url.netloc
		# _url_path = _parsed_url.path
		# m = hashlib.md5()
		# m.update(_url_path.encode(encoding='utf-8', errors='replace'))
		# _docno = '-'.join(map(str, [_deep, Node.domain_urls[_url_netloc]['domain_no'], m.hexdigest()]))
		self.ram_clear(300)
		return

	def low_score_calculator(self, maxurl):
		"""
		AFTER one wave, calculate lowest_score for one wave
		:param maxurl:
		:return:
		"""
		# ceil = math.ceil
		all_page_now = len(bfs_Wave.all_download_page_score)
		if self.deep == 0:
			_lowest_score = 0.6
		elif self.deep == 1:
			_lowest_score = 0.6
		# elif self.deep == 2:
		# 	if all_page_now < ceil(maxurl * 0.7):
		# 		_lowest_score = all_page_now
		# 	else:
		# 		_lowest_score = bfs_Wave.all_download_page_score[ceil(maxurl * 0.7)]
		# elif self.deep == 3:
		# 	if all_page_now < ceil(maxurl * 0.75):
		# 		_lowest_score = all_page_now
		# 	else:
		# 		_lowest_score = bfs_Wave.all_download_page_score[ceil(maxurl * 0.75)]
		else:
			if all_page_now < maxurl:
				_lowest_score = all_page_now
			else:
				_lowest_score = bfs_Wave.all_download_page_score[maxurl]

		return _lowest_score

	def restore_url_children(self, _url, _lowest_score):
		"""
		AFTER one wave
		:param _url:
		:param _lowest_score:
		:return:
		"""
		_parsed_url = urlparse(_url)
		_page_score = Node.domain_urls[_parsed_url.netloc]['accessed'][_parsed_url.path]

		if _page_score < _lowest_score:
			print("Low score: {}/{}, GIVEUP {}".format(_page_score, _lowest_score, _url))
			self.wave_size -= 1
			try:
				Node.graph.remove_node(_url)
			except:
				pass
			print('URL FINISHED in wave {}: {}/{}\n'.format(self.deep, self.wave_finished, self.wave_size))
		else:
			print('\nURL {}'.format(_url))
			try:
				medium_item = es.get(index=medium_index, doc_type=my_type, id=_url)
			except:
				pass
			else:
				bfs_Wave.tt_doc_len += medium_item['_source']['doc_len']
				bfs_Wave.page_count += 1
				bfs_Wave.avg_doc_len = bfs_Wave.tt_doc_len / bfs_Wave.page_count
				Node.add_all_child(_url, medium_item['_source']['href'].split('\n'),
								   medium_item['_source']['score'])
				_source = {'docno': medium_item['_source']['docno'],  # a string
						   'HTTPheader': medium_item['_source']['HTTPheader'],  # a string
						   'title': medium_item['_source']['title'],  # a string
						   'text': medium_item['_source']['text'],  # a string
						   'html_Source': medium_item['_source']['html_Source'],  # a string
						   'in_links': '',  # a string
						   'out_links': '',  # a string
						   'author': 'Chenxi',  # a string
						   'depth': self.deep,  # a int
						   'url': _url  # a string
						   }
				load_to_elasticsearch(es, my_index, my_type, _source, _url)
				self.wave_finished += 1

				print('# of HREF: {}'.format(len(Node.graph.out_edges(_url))))
				print('URL FINISHED in wave {}: {}/{}\n'.format(self.deep, self.wave_finished, self.wave_size))

		self.ram_clear(300)

	def __iter__(self):
		return self.wave


def whole_work(maxurl, poolsize, _start_links, _terms_list, _es, _medium_index, _medium_mappings):
	# remove the previous results
	# for _ in os.listdir(filepath):
	#   print('remove {}'.format(_))
	# 	os.remove('{}/{}'.format('results/', _))
	partial = functools.partial
	collect = gc.collect
	# at time 0
	collected = collect()
	print('\nGarbage collector: collected {} objects.\n'.format(collected))
	next_wave = bfs_Wave.from_start_links(_start_links)
	while bfs_Wave.page_count < maxurl:
		# create medium index
		create_dataset(_es, _medium_index, _medium_mappings)

		current_wave = next_wave  # instance of bfs_Wave
		print('-' * 30, 'WAVE STARTING', '---->URL num: {}'.format(bfs_Wave.page_count), '-' * 40)

		print('start download')
		rr_iterator = current_wave.rr_select(current_wave.wave)  # an iterator of high score url
		_start_time = time.time()
		pool = ThreadPool(poolsize-2)
		try:
			for _n in pool.imap(partial(current_wave.one_node_work, _terms_list=_terms_list),
										  rr_iterator,
										  chunksize=4):
				pass
		except TimeoutError as e:
			print(e)
			pass
		pool.close()
		pool.join()

		time.sleep(15)
		print("--- {0} seconds ---".format(time.time() - _start_time))
		collected = collect()
		print('\nGarbage collector: collected {} objects.\n'.format(collected))

		print('start restore')
		_start_time = time.time()
		_lowest_score = current_wave.low_score_calculator(maxurl)
		current_wave.wave_size = len(current_wave.wave_url_ids)

		def iter_set_pop(_set):
			for _ in range(len(_set)):
				yield _set.pop()

		pool = ThreadPool(poolsize)
		try:
			for _n in pool.imap(partial(current_wave.restore_url_children, _lowest_score=_lowest_score),
										  iter_set_pop(current_wave.wave_url_ids),
										  chunksize=8):
				if bfs_Wave.page_count > maxurl:
					print('FINISHED ALL {}, LEAVING NOW......'.format(bfs_Wave.page_count))
					time.sleep(10)
					break
		except TimeoutError as e:
			print(e)
			pass
		pool.close()
		pool.join()

		print("--- {0} seconds ---".format(time.time() - _start_time))
		print('-' * 15, '>WAVE SIZE {}'.format(current_wave.wave_size))
		print('-' * 15, '>URL RESTORED TOTAL: {}'.format(bfs_Wave.page_count))
		print('-' * 15, '>URL DOWNLOADED TOTAL: {}'.format(bfs_Wave.page_total))
		print('-' * 30, 'WAVE FINISHED', '-' * 40, '\n\n')

		if bfs_Wave.page_count > maxurl:
			time.sleep(10)
			break
		# release unreferenced memory
		collected = collect()
		print('\nGarbage collector: collected {} objects.\n'.format(collected))
		next_wave = bfs_Wave(bfs_Wave.get_next_wave(), current_wave.deep + 1)


# TODO: delete this line after debug
	# input('Press <ENTER> to go on\n>>>')


def canon_query(_q):
	_q = _q.lower().split()
	_q = [stem(_) for _ in _q]
	return _q


def token_text(text, one_kind_terms):
	# match token format (remove tokens not alpha and number
	regex = compile(r"(([-_a-z0-9]+\.)*[-_a-z0-9]+)")
	_d = regex.findall(text.lower())
	_tk_text = [stem(_t[0]) for _t in _d if _t[0]]
	_tt_qterm = sum(_tk_text.count(_t) for _t in canon_query(one_kind_terms))
	return _tt_qterm, len(_tk_text)


def okapi_tf(_term_list, avg_d_l, _doc):  # assume avg_d_l = 500
	_okapiTF_score = 0
	_doc_len = 500
	for k, v in _term_list.items():
		_tf, _doc_len = token_text(_doc, v)
		_okapiTF_score += _tf / (_tf + 0.5 + 1.5 * _doc_len / avg_d_l)
	return _okapiTF_score, _doc_len


def update_graph(_graph):
	for _node in _graph.nodes():
		_ol = '\n'.join([_[1] for _ in _graph.out_edges_iter(_node)])
		_il = '\n'.join([_[1] for _ in _graph.in_edges_iter(_node)])
		_change = {'in_links': _il, 'out_links': _ol}
		update_doc(es, my_index, my_type, _node, _change)


def read_filename_extension(_file):
	_text_pattern = compile(r'\Atext/')
	_not_text_set = set()
	with open(_file, 'r') as f:
		for _ in f:
			_ = _.split()
			if not _text_pattern.search(_[1].strip()):
				_not_text_set.add(_[0].strip())
	return _not_text_set


if __name__ == '__main__':
	start_time = time.time()
	# timeout in seconds
	# setdefaulttimeout = socket.setdefaulttimeout
	# timeout = 10
	# setdefaulttimeout(timeout)
	terms_list = {'meri_terms': 'WATER SEA OCEAN MARITIME OFFSHORE HYDRODYNAMIC SHOAL WATERWAY WATERLINE',
				  'acc_terms': 'ACCIDENTS FIRE IGNITE COLLISION COLLIDE INJURED DAMAGE SAFE POLLUTION STRUCK ALLISION RUPTUR BREACH FLOOD',
				  'ship_terms': 'SHIP BOAT TOWBOAT VESSEL AFT BARGE TANKER JETTY HULL ABOARD PILOT OPERATOR VISIBILITY CAPTAIN CREW STEER ANCHOR'}

	start_links = ['http://www.marineinsight.com/marine-safety/12-types-of-maritime-accidents/',
				   'http://www.shipwrecklog.com/',
				   'http://www.ntsb.gov/investigations/AccidentReports/Pages/marine.aspx',
				   'http://maritimeaccident.org/',
				   'http://en.wikipedia.org/wiki/List_of_maritime_disasters',
				   'http://en.wikipedia.org/wiki/Costa_Concordia_disaster',
				   'http://www.telegraph.co.uk/news/worldnews/europe/italy/10312026/Costa-Concordia-recovery-timeline-of-cruise-ship-disaster.html',
				   'https://en.wikipedia.org/wiki/RMS_Titanic'
				   ]

	test_links = ['http://en.wikipedia.org/wiki/List_of_maritime_disasters',
				  'http://csb.stanford.edu/class/public/pages/sykes_webdesign/05_simple.html']

	# 'http://csb.stanford.edu/class/public/pages/sykes_webdesign/'
	es = Elasticsearch()
	my_index = 'maritimeaccidents'
	medium_index = 'wave_medium'
	my_type = 'document'
	create_dataset(es, my_index, doc_mappings)
	Node.not_text_set = read_filename_extension('filename_extension')
	whole_work(35000, 12, start_links, terms_list, es, medium_index, medium_mappings)
	time.sleep(15)
	update_graph(Node.graph)

	print("--- {0} seconds ---".format(time.time() - start_time))
