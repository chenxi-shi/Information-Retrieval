import os
from functools import partial
from urllib.parse import urlunparse, urlparse
import time
from Node_Part import Node
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
	file_num = 1
	page_count = 0
	page_size = 1000
	_child_deep = 1
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
	def one_node_work(node_instance, filename, _deep):
		_page_info = Node.download_html(node_instance.normed_url)

		if _page_info:
			bfs_Wave.page_total += 1
			_medium_res = Node.parse_page(_page_info['content'])  # parse the page
			node_instance.add_all_child(_deep, _medium_res)
			_parsed_url = urlparse(node_instance.normed_url)
			_url_netloc = _parsed_url.netloc
			_url_path = _parsed_url.path
			m = hashlib.md5()
			m.update(_url_path.encode(encoding='utf-8', errors='replace'))
			_docno = '-'.join(map(str, [_deep,
			                            Node.domain_urls[_url_netloc]['domain_no'],
			                            m.hexdigest()]))
			_source = {'docno': _docno,                         # a int
			           'HTTPheader': _page_info['HTTPheader'],  # a string
			           'title': _medium_res['title'],           # a string
			           'text': ''.join(_medium_res['content']), # a string
			           'html_Source': 'N/A',                    # a string
			           'in_links': '\n'.join(Node.graph[node_instance.normed_url]['in_links']),     # a string
			           'out_links': '\n'.join(Node.graph[node_instance.normed_url]['out_links']),   # a string
			           'author': 'Chenxi',                      # a string
			           'depth': _deep,                          # a int
			           'url': node_instance.normed_url          # a string
			           }
			pprint.pprint(_source)
			load_files(es, my_index, my_type, _source, _docno)
			filename.write('url: {}\n'.format(node_instance.normed_url))
			filename.write('num of href: {}\n'.format(len(_medium_res['href'])))
			filename.write('href: {}\n\n'.format(_medium_res['href']))
			filename.write('title: {}\n\n'.format(_medium_res['title']))
			filename.write('content: {}\n\n\n'.format(''.join(_medium_res['content'])))
			bfs_Wave.page_count += 1
			print('\n', '-' * 20, 'FINISHED {}'.format(node_instance.normed_url))
			print('-' * 20, 'num of href: {}'.format(len(_medium_res['href'])))
			print('-' * 20, 'URL FINISHED: {}\n'.format(bfs_Wave.page_total))


		# TODO pop up page after accessed
		# TODO: clear RAM after 1000 page

	@staticmethod
	def thread_handler(_url, _filepath, _deep):
		# download page of one node
		if bfs_Wave.page_count > 1000:
			bfs_Wave.file_num += 1
			bfs_Wave.page_count = 0

		with open('{}{}'.format(_filepath, bfs_Wave.file_num * bfs_Wave.page_size), 'a+', errors='ignore') as test:
			bfs_Wave.one_node_work(node_instance=Node(_url),
			                    filename=test,
			                    _deep=_deep)
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
def whole_work(maxurl, poolsize, _start_links, filepath):
	# remove the previous results
	for _ in os.listdir(filepath):
		print('remove {}'.format(_))
		os.remove('{}/{}'.format('results/', _))
	# at time 0
	#TODO put domain into next_wave_domain
	next_wave = bfs_Wave.from_start_links(_start_links)
	while bfs_Wave.page_total < maxurl:
		current_wave = next_wave  # instance of bfs_Wave
		print('-'*40, 'WAVE STARTING', '---->URL #: {}'.format(bfs_Wave.page_total), '-'*40)

		rr_iterator = bfs_Wave.rr_select(current_wave.wave)

		from multiprocessing.dummy import Pool as ThreadPool
		with ThreadPool(poolsize) as pool:
			for _n in pool.imap_unordered(partial(bfs_Wave.thread_handler, _filepath=filepath, _deep=current_wave.deep),
			                              rr_iterator,
			                              chunksize=poolsize):
				print('FINISHED SOURCE: {}'.format(_n))

		next_wave = bfs_Wave(bfs_Wave.get_next_wave(), current_wave.deep+1)

		print('--------------------->URL #: {}'.format(bfs_Wave.page_total))
		print('-' * 40, 'WAVE FINISHED', '-' * 40, '\n\n')
		# TODO: delete this line after debug
		input('Press <ENTER> to go on')

if __name__ == '__main__':
	start_time = time.time()
	meri_query = 'WATER SEA OCEAN MARITIME OFFSHORE HYDRODYNAMIC SHOAL WATERWAY WATERLINE'
	acc_query = 'ACCIDENTS FIRE IGNITE COLLISION COLLIDE INJURED DAMAGE SAFE POLLUTION STRUCK ALLISION RUPTUR BREACH FLOOD'
	ship_query = 'SHIP BOAT TOWBOAT VESSEL AFT BARGE TANKER JETTY HULL ABOARD ' \
	             'PILOT OPERATOR VISIBILITY CAPTAIN CREW STEER'

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
	my_index = 'hw3_dataset'
	my_type = 'document'
	create_dataset(es, my_index)

	whole_work(2000, 8, test_links, 'results/')

	print("--- {0} seconds ---".format(time.time() - start_time))