import os
from functools import partial
from urllib.parse import urlunparse, urlparse
import time
from Node_Part import Node, okapi_tf
from elasticseach_func import *
import hashlib
# import lxml


class bfs_Wave:
	'''
	This class contral the process of crawling,
	including initialize starting links instantces,
	doing whole_work of Node class,
	recording deep info into Node.graph
	'''
	page_total = 0
	# file_num = 1
	page_count = 0
	# page_size = 1000
	tt_doc_len = 0
	_child_deep = 1
	avg_doc_len = 500
	def __init__(self, _wave, _deep):
		self.wave = _wave  # wave is sorted list of _next_wave_domain
		self.next_wave = set()
		self.deep = _deep

	@classmethod
	def from_start_links(cls, _start_url):
		_wave0 = Node.from_start_links(_start_url)  # set of Node instances
		return cls(_wave0, 0)  # return wave instance

	def __repr__(self):
		return 'Wave length ({!r})'.format(len(self.wave))

	@staticmethod
	def get_next_wave():
		next_wave = {}
		for _domain, _detail in Node.domain_urls.items():
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
	def one_node_work(node_instance, _deep, _terms_list):
		_html_info = Node.download_html(node_instance.normed_url)

		if _html_info:
			bfs_Wave.page_total += 1
			# return {'title': t, 'href': h, 'text': c}
			_medium_res = Node.parse_page(_html_info['content'])  # parse the page

			# strategy: delete page with _okapi_score <= 1/3  <-- ttf <=1
			_okapi_score, _doc_len = okapi_tf(_terms_list, bfs_Wave.avg_doc_len, _medium_res['text'])
			if _okapi_score <= 0.33:
				print("GIVEUP {}, low score: {}".format(node_instance.normed_url, _okapi_score))
				return
			bfs_Wave.tt_doc_len += _doc_len
			bfs_Wave.page_count += 1
			bfs_Wave.avg_doc_len = bfs_Wave.tt_doc_len / bfs_Wave.page_count
			node_instance.add_all_child(_deep, _medium_res)
			# This part can create _docno like deep-domain-pathhash
			'''
			_parsed_url = urlparse(node_instance.normed_url)
			_url_netloc = _parsed_url.netloc
			_url_path = _parsed_url.path
			m = hashlib.md5()
			m.update(_url_path.encode(encoding='utf-8', errors='replace'))
			_docno = '-'.join(map(str, [_deep, Node.domain_urls[_url_netloc]['domain_no'], m.hexdigest()]))
			'''

			_source = {'docno': node_instance.normed_url,       # a string
			           'HTTPheader': _html_info['HTTPheader'],  # a string
			           'title': _medium_res['title'],           # a string
			           'text': ''.join(_medium_res['text']), # a string
			           'html_Source': _html_info['content'],    # a string
			           'in_links': '\n'.join(Node.graph[node_instance.normed_url]['in_links']),     # a string
			           'out_links': '\n'.join(Node.graph[node_instance.normed_url]['out_links']),   # a string
			           'author': 'Chenxi',                      # a string
			           'depth': _deep,                          # a int
			           'url': node_instance.normed_url          # a string
			           }
			# pprint.pprint(_source)
			load_files(es, my_index, my_type, _source, node_instance.normed_url)
			'''
			filename.write('url: {}\n'.format(node_instance.normed_url))
			filename.write('num of href: {}\n'.format(len(_medium_res['href'])))
			filename.write('href: {}\n\n'.format(_medium_res['href']))
			filename.write('title: {}\n\n'.format(_medium_res['title']))
			filename.write('content: {}\n\n\n'.format(''.join(_medium_res['text'])))
			'''

			print('\nFINISHED {}'.format(node_instance.normed_url))
			print('# of HREF: {}'.format(len(_medium_res['href'])))
			print('URL FINISHED: {}\n'.format(bfs_Wave.page_total))


		# TODO pop up page after accessed
		# TODO: clear RAM after 1000 page

	@staticmethod
	def thread_handler(_url, _deep, _terms_list):
		# download page of one node

		#with open('{}{}'.format(_filepath, bfs_Wave.file_num * bfs_Wave.page_size), 'a+', errors='ignore') as test:
		bfs_Wave.one_node_work(node_instance=Node(_url),
		                       #filename=test,
		                       _deep=_deep,
		                       _terms_list=_terms_list)
		return _url

	def __iter__(self):
		return self.wave

	@staticmethod
	# it is a round_robin select method
	def rr_select(_wave_domain):  # it is a sorted list
		last = len(_wave_domain) - 1
		while _wave_domain:
			current = (last + 1) % len(_wave_domain)
			_domain = _wave_domain[current]

			if Node.domain_urls[_domain]['next_wave']:
				_path = Node.domain_urls[_domain]['next_wave'].pop()
				print('===========> PATH ', _path)
				last = current
				yield urlunparse((Node.domain_urls[_domain]['scheme'], _domain, _path, '', '', ''))
			else:
				print('{} empty, remove'.format(_domain))
				_wave_domain.remove(_domain)
				last = current - 1
				continue

# TODO: use yield for instance
def whole_work(maxurl, poolsize, _start_links, _terms_list):
	# remove the previous results
	'''
	for _ in os.listdir(filepath):
		print('remove {}'.format(_))
		os.remove('{}/{}'.format('results/', _))
	'''
	# at time 0
	#TODO put domain into next_wave_domain
	next_wave = bfs_Wave.from_start_links(_start_links)
	while bfs_Wave.page_total < maxurl:
		current_wave = next_wave  # instance of bfs_Wave
		print('-'*30, 'WAVE STARTING', '---->URL #: {}'.format(bfs_Wave.page_total), '-'*40)

		rr_iterator = bfs_Wave.rr_select(current_wave.wave)

		from multiprocessing.dummy import Pool as ThreadPool
		with ThreadPool(poolsize) as pool:
			for _n in pool.imap_unordered(partial(bfs_Wave.thread_handler,
			                                      #_filepath=filepath,
			                                      _deep=current_wave.deep,
			                                      _terms_list=_terms_list),
			                              rr_iterator,
			                              chunksize=poolsize):
				pass
		next_wave = bfs_Wave(bfs_Wave.get_next_wave(), current_wave.deep+1)

		print('--------------------->URL #: {}'.format(bfs_Wave.page_total))
		print('-' * 30, 'WAVE FINISHED', '-' * 40, '\n\n')
		# TODO: delete this line after debug
		#input('Press <ENTER> to go on\n>>>')

if __name__ == '__main__':
	start_time = time.time()
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
	               'http://en.wikipedia.org/wiki/Costa_Concordia']

	test_links = ['http://en.wikipedia.org/wiki/List_of_maritime_disasters',
	              'http://csb.stanford.edu/class/public/pages/sykes_webdesign/05_simple.html']

# 'http://csb.stanford.edu/class/public/pages/sykes_webdesign/'
	es = Elasticsearch()
	my_index = 'maritimeaccidents'
	my_type = 'document'
	create_dataset(es, my_index)

	whole_work(2000, 8, start_links, terms_list)

	print("--- {0} seconds ---".format(time.time() - start_time))