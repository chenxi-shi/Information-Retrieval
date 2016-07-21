#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import re
import socket
from re import compile
import bs4
import time
import requests
from requests import RequestException
from reppy.cache import RobotsCache
import networkx as nx
import lxml.html
# noinspection PyCompatibility
from urllib.error import URLError
# noinspection PyCompatibility
from urllib.parse import urlparse, unquote, urljoin, urlunparse

from requests.packages.urllib3.exceptions import LocationValueError

__author__ = "Chenxi Shi"
__copyright__ = "Copyright 2016, Information Retrieval hw3"
__credits__ = ["Chenxi Shi"]
__version__ = "3.2.2"
__maintainer__ = "Chenxi Shi"
__email__ = "shi.che@husky.neu.edu"
__status__ = "Coding"

'''
graph = {'A': {'out_links':set(['B', 'C']), 'deep':x, 'in_links': set()},
		 'B': {'out_links':set(['A', 'D', 'E']), 'deep':x, 'in_links': set()},
		 'C': {'out_links':set(['A', 'F']), 'deep':x, 'in_links': set()},
		 'D': {'out_links':set(['B']), 'deep':x, 'in_links': set()},
		 'E': {'out_links':set(['B', 'F']), 'deep':x, 'in_links': set()},
		 'F': {'out_links':set(['C', 'E']), 'deep':x, 'in_links': set()}}
'''


class Node:  # deep first strategy
	_app_proto_pattern = compile(r'\Ahttps?://')
	_end_slash_pattern = compile(r'/\Z')  # don't do it! add / with mistaken, getting 404
	_end_index_pattern = compile(r'((index)\.html?/?)\Z')
	_octets_pattern = compile(r'%[0-9a-f]{2}')
	_port_80_pattern = compile(r':80\Z')
	_port_443_pattern = compile(r':443\Z')
	_dupl_slash_dot_pattern = compile(r'/*\.*/')
	_netloc_pattern = compile(r'\A((([\w\-]{1,63}\.){1,4})[A-Za-z-]{1,62})(:\d+)?\Z')
	_URL_pattern = compile(
		r'\A(((https?|ftp)://)|(\.*/+))?((((([\w\-]{1,63}\.){1,3})[A-Za-z-]{1,62})?(:\d+)?(/[\w\$-_\.\+!\*\'\(\),&:;=\?@]{1,1600})?)|(((25[0-5]|2[0-4]\d|1\d\d|\d{1,2})\.){3}(25[0-5]|2[0-4]\d|1\d\d|\d{1,2})\/?))\Z')
	graph = nx.DiGraph()
	domain_urls = {}
	not_text_set = set()
	'''
	domain_urls = {domain:{accessed:{path: url_id}, next_wave:{path:0.2*score}, session:session, scheme: http,
							last_time:d_time, domain_no:x, robot_rules:rules},
					domain:{accessed:{path: url_id}, next_wave:{path:0.2*score}, session:session, scheme: http,
							last_time:d_time, domain_no:x, robot_rules:rules}...}
	'''

	user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'  # agent: IE
	headers = {'User-Agent': user_agent, 'Connection': 'Keep-Alive'}

	__slots__ = ['normed_url', 'nmed_url_domain', 'nmed_url_path', 'HTTPheader', 'html', 'title', 'href', 'text']

	def __init__(self, normed_url):  # 'http://www.google.com' for starting links
		self.normed_url = normed_url
		self.nmed_url_domain = ''
		self.nmed_url_path = ''
		self.HTTPheader = ''
		self.html = ''
		self.title = ''
		self.href = []
		self.text = ''

	@staticmethod
	def robot_rules(_url_scheme, _url_netloc):  # return a robot rules objects
		_domain = urlunparse((_url_scheme, _url_netloc, '', '', '', ''))
		robots = RobotsCache()
		try:
			rules = robots.fetch(_domain, timeout=5)
		except Exception as exc:
			print('FAIL to fatch robot.txt {},{}'.format(_url_scheme, _url_netloc))
			print(exc)
			return None
		return rules

	# @classmethod
	@staticmethod
	def from_start_links(_start_url, _src_url='http://www.google.com'):
		session = requests.Session
		_next_wave_domain = []
		for _url in _start_url:
			_session = session()
			print('URL: {}'.format(_url))
			print('SOURCE: {}'.format(_src_url))
			_url_scheme, _url_netloc, _url_path = Node.canon_url(_url, _src_url)
			_url = urlunparse((_url_scheme, _url_netloc, _url_path, '', '', ''))
			if _url not in Node.graph:
				Node.graph.add_node(_url)
			if _url_netloc in Node.domain_urls:
				Node.domain_urls[_url_netloc]['next_wave'][_url_path] = 0
			else:
				Node.domain_urls[_url_netloc] = {'accessed': {},
												 'next_wave': {_url_path: 0},  # set()
												 'last_time': 0,
												 'domain_no': len(Node.domain_urls) + 1,
												 'robot_rules': Node.robot_rules(_url_scheme, _url_netloc),
												 'session': _session,
												 'scheme': _url_scheme}

			_next_wave_domain.append(_url_netloc)
		return _next_wave_domain  # list of _next_wave_domain

	@staticmethod
	def domain_1s_timer(_d_time):
		_time_diff = time.perf_counter() - _d_time
		if _time_diff < 1:
			time.sleep(_time_diff)
		elif _time_diff < 0:
			raise ValueError('domain time should be less than current time,\n domain time is {}'.format(_d_time))
		return time.perf_counter()

	def download_html(self):
		session = requests.Session
		_parsed_url = urlparse(self.normed_url)
		self.nmed_url_domain = _parsed_url.netloc
		self.nmed_url_path = _parsed_url.path
		if self.nmed_url_domain not in Node.domain_urls:
			Node.domain_urls[self.nmed_url_domain] = {'accessed': {},
													  'next_wave': {},
													  'last_time': 0,
													  'domain_no': len(Node.domain_urls) + 1,
													  'robot_rules': Node.robot_rules(_parsed_url.scheme,
																					  self.nmed_url_domain),
													  'session': session(),
													  'scheme': _parsed_url.scheme}

		_last_time = Node.domain_1s_timer(Node.domain_urls[self.nmed_url_domain]['last_time'])
		try:
			_res = Node.domain_urls[self.nmed_url_domain]['session'].get(self.normed_url, headers=Node.headers, timeout=5)
			_res.raise_for_status()
		except URLError as e:
			if hasattr(e, 'reason'):
				print('We failed to reach a server. {}'.format(self.normed_url))
				print('Reason: ', e.reason)
			elif hasattr(e, 'code'):
				print('The server couldn\'t fulfill the request. {}'.format(self.normed_url))
				print('Error code: ', e.code)
			return False
		except RequestException as e:
			print('Status_code Error! {}'.format(self.normed_url))
			print(e)
			return False
		except LocationValueError as e:
			print('Error: {}, URL: {}'.format(e, self.normed_url))
			return False
		except:
			return False
		else:
			Node.domain_urls[self.nmed_url_domain]['last_time'] = _last_time
			# Node.domain_urls[_domain]['last_time'] = time.perf_counter()
			Node.domain_urls[self.nmed_url_domain]['accessed'][self.nmed_url_path] = len(
				Node.domain_urls[self.nmed_url_domain]['accessed']) + 1
			self.HTTPheader = _res.headers
			try:
				_content_type = self.HTTPheader['content-type']
			except:
				pass
			else:
				_text_pattern = re.compile(r'\Atext/')
				if not _text_pattern.search(_content_type):  # content_type not text
					return False
			self.html = _res.text
			return True  # {'HTTPheader': _res.headers, 'content': _res.text}  # auto decode to utf-8

	# @staticmethod
	def parse_page(self, content_type):  # extract all right href, not next wave href
		beautifulsoup = bs4.BeautifulSoup
		fromstring = lxml.html.fromstring
		_soup = beautifulsoup(self.html, "lxml")
		# for remove BMP、JPG、JPEG、PNG、GIF, PDF, CSS, JS,TIFF, webm, mp4, ogg, 3gp, flv, exe
		# _link_ext_pattern = compile(r'\.((Ee][Xx][Ee]|[Ff][Ll][Vv]|3[Gg][Pp]|[Oo][Gg]{2}|[Mm][Pp][34]|[Ww][Ee][Bb][Mm]|[jJ][pP][eE]?[gG])|([pP]([nN][gG]|[dD][fF]))|([bB][mM][pP])|(([gG]|[tT])[iI][fF]{1,2})|(([cC]|[jJ])[sS]{1,2}))\Z')

		# kill all script and style elements
		for script in _soup(["script", "style"]):
			script.extract()  # rip it out
		_with_extension_pattern = re.compile(r'\.[a-zA-Z0-9]{1,5}\Z')

		# _text_pattern = re.compile(r'\Atext/')
		# for remove CSS, webm, ogg, 001, 301, 906, 907, a11, acp, ai, aif, aifc, aiff, anv, asf, asx, au, avi, awf, bmp,
		# _wanted_pattern = re.compile(r'\.((323)|([Aa][Ss]([Aa]|[Pp]))|([Bb][Ii][Zz])|([Cc][Mm][Ll])|([Dd]([Cc]|[Tt])[Dd])|([Ee][Nn][Tt])|([Hh][Tt][Cc]))\Z')
		# mime = MimeTypes()

		def has_href(_tag):
			if _tag.name == 'a':
				if _tag.has_attr('href') and Node._URL_pattern.match(_tag['href']):  # remove in page anchor
					_extension = _with_extension_pattern.search(_tag['href'].lower())
					if _extension:  # have extension
						if _extension.group() not in Node.not_text_set:  # remove unwanted extension
							return True  # text extension
					else:  # not have extension
						return True

		def html2text(soup4html):
			"""  done it outside function
			# kill all script and style elements
			for script in soup4html(["script", "style"]):
				script.extract()  # rip it out
			"""
			# get text
			text = soup4html.get_text()

			# break into lines and remove leading and trailing space on each
			lines = (line.strip() for line in text.splitlines())
			# break multi-headlines into a line each
			chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
			# drop blank lines
			text = ' '.join(chunk for chunk in chunks if chunk)
			return text

		xml_pattern = compile(r'\w*/xml')
		if xml_pattern.search(content_type):
			ht = fromstring(self.html.encode(errors='replace'))
			self.href = ht.xpath('//a/@href')
		else:
			self.href = [_['href'] for _ in _soup.find_all(has_href)]
		self.title = _soup.title.get_text() if _soup.title else 'N/A'
		self.text = html2text(_soup)
		return  # {'title': _title, 'href': _href, 'text': _text}

	def __repr__(self):
		return 'Node({!r})'.format(self.normed_url)

	@staticmethod
	def add_all_child(normed_url, _href, node_score):
		"""
		add NOT accessed & permited child url into Node.graph
		:param normed_url:
		:param _href:
		:param node_score:
		:return:
		"""
		session = requests.Session
		_total_children = len(_href)
		print('chidren num {}'.format(_total_children))
		#_added_child = 0
		for _child_url in _href:
			# start doing norm url
			#_added_child += 1
			#print('{}/{} {}'.format(_added_child, _total_children, _child_url))
			_session = session()
			_url_scheme, _url_netloc, _url_path = Node.canon_url(_child_url, normed_url)
			_child_url = urlunparse((_url_scheme, _url_netloc, _url_path, '', '', ''))

			# if old domain
			if _url_netloc in Node.domain_urls:
				# if blocked by robots.txt
				if Node.domain_urls[_url_netloc]['robot_rules']:
					try:
						_disallowed = Node.domain_urls[_url_netloc]['robot_rules'].disallowed(_child_url, '*')
					except:
						continue
					if _disallowed:
						continue
				# if allowed or no rule & not old path
				if _url_path not in Node.domain_urls[_url_netloc]['accessed']:
					# add the basic score of parent score to child score
					Node.domain_urls[_url_netloc]['next_wave'][_url_path] = 0.2 * node_score

			else:  # if new domain
				_rules = Node.robot_rules(_url_scheme, _url_netloc)
				if _rules:
					try:
						_disallowed = _rules.disallowed(_child_url, '*')
					except:
						print('GIVEUP! robot.txt block {}'.format(_child_url))
						# print(_url)
						print(_url_scheme, _url_netloc, _url_path)
						# print(self.normed_url, '\n')
						continue
					if _disallowed:
						# print('GIVEUP! robot.txt block {}'.format(_child_url))
						continue
				# if allowed or no rules
				Node.domain_urls[_url_netloc] = {'accessed': {},
												 'next_wave': {_url_path: 0.2 * node_score},
												 'last_time': 0,
												 'domain_no': len(Node.domain_urls) + 1,
												 'robot_rules': _rules,
												 'session': _session,
												 'scheme': _url_scheme}
			# print('{} CHILD: {}'.format(Node.domain_urls[_url_netloc]['domain_no'], _child_url))
			# create child in Node.graph
			if normed_url != _child_url:
				Node.graph.add_edge(normed_url, _child_url)

	def __iter__(self):
		return Node.graph.out_edges_iter(self.normed_url)  # list of children urls

	@staticmethod
	def canon_url(_url, _src_url):  # _src_netloc must include scheme and netloc
		if not isinstance(_url, str):
			print('URL should be string, now it is {}'.format(type(_url)))
			return
		_parsed_url = urlparse(_url)  # url1
		_url_netloc = Node._dupl_slash_dot_pattern.sub('/', _parsed_url.netloc)  # remove duplicate slash and dot

		# print(_parsed_url)
		if _url_netloc:
			if not Node._netloc_pattern.search(_url_netloc):
				print('netloc is not right, change to path')
				_url_path = _parsed_url.netloc + _parsed_url.path
				_url_path = Node._dupl_slash_dot_pattern.sub('/', _url_path)  # remove duplicate slash and dot
				_parsed_src_url = urlparse(_src_url)
				if not _parsed_src_url.netloc:
					print('src_url needs net location')
					return
				if not _parsed_src_url.scheme:
					print('src_url needs scheme')
					return

				_url = urljoin(_src_url, _url_path)  # join netloc with parent url
				_parsed_url = urlparse(_url)
				print(_url)
		else:
			_url = urljoin(_src_url, _parsed_url.path)  # join netloc with parent url
			_parsed_url = urlparse(_url)

		# print(_parsed_url)
		if not _parsed_url.scheme:
			_url = urlunparse(('http', _parsed_url.netloc, _parsed_url.path, '', '', ''))
			_parsed_url = urlparse(_url)
		# print(_parsed_url)
		# url 1 or 2
		_url_scheme = _parsed_url.scheme.lower()
		_url_netloc = _parsed_url.netloc.lower()
		_url_path = Node._dupl_slash_dot_pattern.sub('/', _parsed_url.path)  # remove duplicate slash and dot
		_url_path = Node._end_index_pattern.sub('', _url_path)  # remove index.htm
		_url_path = unquote(_url_path, encoding='utf-8', errors='replace')  # change octets in url

		if _url_scheme == 'http':  # remove port num
			_url_netloc = Node._port_80_pattern.sub('', _parsed_url.netloc.lower())
		elif _url_scheme == 'https':
			_url_netloc = Node._port_443_pattern.sub('', _parsed_url.netloc.lower())
		# _url = urlunparse((_url_scheme, _url_netloc, _url_path, '', '', ''))

		return _url_scheme, _url_netloc, _url_path  # remove fragment


# canon url with head requests
'''
@staticmethod
	def canon_url_with_head(_url, _src_url, _session):  # _src_netloc must inludes scheme and netloc
		if not isinstance(_url, str):
			print('URL should be string, now it is {}'.format(type(_url)))
			return
		_parsed_url = urlparse(_url)  # url1
		_url_scheme = _parsed_url.scheme.lower()
		_url_netloc = _parsed_url.netloc.lower()
		_url_path = Node._dupl_slash_dot_pattern.sub('/',
													 _parsed_url.path)  # remove duplicate slash and dot may inside of them
		if not _url_netloc or not _url_scheme:
			_parsed_src_url = urlparse(_src_url)
			if not _parsed_src_url.netloc:
				print('src_url needs net location')
				return
			if not _parsed_src_url.scheme:
				print('src_url needs scheme')
				return
			_url = urljoin(_src_url, _url_netloc + _url_path)  # join netloc with parent url
			_parsed_url = urlparse(_url)
		try:
			_head = _session.head(_url, headers=Node.headers, allow_redirects=False, timeout=10)
		except TimeoutError as e:
			print('WARNING: {} time out'.format(_url))
			print(e)

		else:
			if 'Location' in _head.headers:
				_url = _head.headers['Location']
				_parsed_url = urlparse(_url)  # url2
				_url_scheme = _parsed_url.scheme.lower()
				_url_netloc = _parsed_url.netloc.lower()
				_url_path = Node._dupl_slash_dot_pattern.sub('/',
															 _parsed_url.path)  # remove duplicate slash and dot may inside of them

				if not _url_netloc or not _url_scheme:
					_parsed_src_url = urlparse(_src_url)
					if not _parsed_src_url.netloc:
						print('src_url needs net location')
						return
					if not _parsed_src_url.scheme:
						print('src_url needs scheme')
						return
					_url = urljoin(_src_url, _url_netloc + _url_path)  # join netloc with relative url
					_parsed_url = urlparse(_url)

		# url 1 or 2
		_url_scheme = _parsed_url.scheme.lower()
		_url_path = Node._end_index_pattern.sub('', _parsed_url.path)  # remove index.htm
		_url_path = unquote(_url_path, encoding='utf-8', errors='replace')  # change octets in url

		if _url_scheme == 'http':  # remove port num
			_url_netloc = Node._port_80_pattern.sub('', _parsed_url.netloc.lower())
		elif _url_scheme == 'https':
			_url_netloc = Node._port_443_pattern.sub('', _parsed_url.netloc.lower())
		# _url = urlunparse((_url_scheme, _url_netloc, _url_path, '', '', ''))
		return _url_scheme, _url_netloc, _url_path  # remove fragment
'''

if __name__ == '__main__':

	# for test
	def one_node_work(node_instance, filename, _deep):
		try:
			_page_info = Node.download_html(node_instance.normed_url)
		except ValueError as err:
			print(node_instance.normed_url)
			print(err)
		else:
			_medium_res = Node.parse_page(_page_info['content'])  # parse the page
			node_instance.add_all_child(_deep, _medium_res)
			filename.write('url: {}\n'.format(node_instance.normed_url))
			filename.write('num of href: {}\n'.format(len(_medium_res['href'])))
			filename.write('title: {}\n\n'.format(_medium_res['title']))
			filename.write('href: {}\n\n'.format(_medium_res['href']))
			filename.write('content: {}\n\n\n'.format(''.join(_medium_res['text'])))

		for k, v in Node.domain_urls.items():
			print('domain: {}'.format(k))
			print('domain_no: {}'.format(v['domain_no']))
			print('accessed: \n{}'.format(v['accessed']))
			print('next_wave: \n{}'.format(v['next_wave']))


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
				   'https://en.wikipedia.org/wiki/RMS_Titanic']

	start_nodes = Node.from_start_links(start_links)
	with open('results/test.txt', 'w', errors='ignore') as test:
		for _ in start_nodes:
			one_node_work(_, test, 0)

	print("--- {0} seconds ---".format(time.time() - start_time))
