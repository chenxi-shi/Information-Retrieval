#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import pprint
import socket
from urllib.error import URLError
from urllib.parse import urlparse, unquote, urljoin, urlunparse
import re

from bs4 import BeautifulSoup
from stemming.porter2 import stem
import time
import requests
from reppy.cache import RobotsCache
# import lxml

'''
graph = {'A': {'out_links':set(['B', 'C']), 'deep':x, 'in_links': set()},
         'B': {'out_links':set(['A', 'D', 'E']), 'deep':x, 'in_links': set()},
         'C': {'out_links':set(['A', 'F']), 'deep':x, 'in_links': set()},
         'D': {'out_links':set(['B']), 'deep':x, 'in_links': set()},
         'E': {'out_links':set(['B', 'F']), 'deep':x, 'in_links': set()},
         'F': {'out_links':set(['C', 'E']), 'deep':x, 'in_links': set()}}
'''

# timeout in seconds
timeout = 10
socket.setdefaulttimeout(timeout)


class Node:  # deep first strategy
	_app_proto_pattern = re.compile(r'\Ahttps?://')
	_end_slash_pattern = re.compile(r'/\Z')  # don't do it! add / with mistaken, getting 404
	_end_index_pattern = re.compile(r'((index)\.html?/?)\Z')
	_octets_pattern = re.compile(r'%[0-9a-f]{2}')
	_port_80_pattern = re.compile(r':80\Z')
	_port_443_pattern = re.compile(r':443\Z')
	_dupl_slash_dot_pattern = re.compile(r'/*\.*/')
	_netloc_pattern = re.compile(r'\A((([\w\-]{1,63}\.){1,3})[A-Za-z-]{1,62})(:\d+)?\Z')
	_URL_pattern = re.compile(
		r'\A(((https?|ftp):\/\/)|(\.*\/+))?((((([\w\-]{1,63}\.){1,3})[A-Za-z-]{1,62})?(:\d+)?(\/[\w\$-\_\.\+\!\*\'\(\)\,\&\:\;\=\?\@]{1,1600})?)|(((25[0-5]|2[0-4]\d|1\d\d|\d{1,2})\.){3}(25[0-5]|2[0-4]\d|1\d\d|\d{1,2})\/?))\Z')
	graph = {}
	domain_urls = {}
	'''
	domain_urls = {domain:{accessed:{}, next_wave:set(), session:session, scheme: http,
							last_time:d_time, domain_no:x, robot_rules:rules},
					domain:{accessed:{}, next_wave:set(), session:session, scheme: http,
							last_time:d_time, domain_no:x, robot_rules:rules}...}
	'''

	user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'  # agent: IE
	headers = {'User-Agent': user_agent, 'Connection': 'Keep-Alive'}

	def __init__(self, normed_url):  # 'http://www.google.com' for starting links
		self.normed_url = normed_url
		print('SOURCE: {}'.format(self.normed_url))

	@staticmethod
	def robot_rules(_url_scheme, _url_netloc):  # return a robot rules objects
		#_parsed_url = urlparse(_url)
		_domain = urlunparse((_url_scheme, _url_netloc, '', '', '', ''))
		robots = RobotsCache()
		try:
			#print('DOMAIN: {}'.format(_domain))
			rules = robots.fetch(_domain)
		except Exception as exc:
			print('FAIL to fatch robot.txt')
			print(_url_scheme, _url_netloc)
			print(exc)
			return None
		return rules

	# @classmethod
	@staticmethod
	def from_start_links(_start_url, _src_url='http://www.google.com'):
		_next_wave_domain = []
		for _url in _start_url:
			_session = requests.Session()
			print('URL: {}'.format(_url))
			print('SOURCE: {}'.format(_src_url))
			_url_scheme, _url_netloc, _url_path = Node.conon_url(_url, _src_url)
			_url = urlunparse((_url_scheme, _url_netloc, _url_path, '', '', ''))
			if _url not in Node.graph:
				Node.graph[_url] = {'out_links': set(), 'deep': 0, 'in_links': set()}
			if _url_netloc in Node.domain_urls:
				Node.domain_urls[_url_netloc]['next_wave'].add(_url_path)
			else:
				Node.domain_urls[_url_netloc] = {'accessed': {},
				                                 'next_wave': {_url_path},  # set()
				                                 'last_time': 0,
				                                 'domain_no': len(Node.domain_urls) + 1,
				                                 'robot_rules': Node.robot_rules(_url_scheme, _url_netloc),
				                                 'session': _session,
				                                 'scheme': _url_scheme}

			_next_wave_domain.append(_url_netloc)
		return _next_wave_domain  # list of _next_wave_domain

	@staticmethod
	def parse_page(_html):  # extract all right href, not next wave href
		_soup = BeautifulSoup(_html, "html.parser")
		# for remove BMP、JPG、JPEG、PNG、GIF, PDF, CSS, JS,TIFF
		_link_ext_pattern = re.compile(
			r'\.(([jJ][pP][eE]?[gG])|([pP]([nN][gG]|[dD][fF]))|([bB][mM][pP])|(([gG]|[tT])[iI][fF]{1,2})|(([cC]|[jJ])[sS]{1,2}))\Z')

		# kill all script and style elements
		for script in _soup(["script", "style"]):
			script.extract()  # rip it out

		def has_href(_tag):
			if _tag.name == 'a':
				if _tag.has_attr('href') and Node._URL_pattern.match(_tag['href']):  # remove in page anchor
					if not _link_ext_pattern.search(_tag['href']):
						return True

		def html2text(soup4html):
			'''  done it outside function
			# kill all script and style elements
			for script in soup4html(["script", "style"]):
				script.extract()  # rip it out
			'''
			# get text
			text = soup4html.get_text()

			# break into lines and remove leading and trailing space on each
			lines = (line.strip() for line in text.splitlines())
			# break multi-headlines into a line each
			chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
			# drop blank lines
			text = ' '.join(chunk for chunk in chunks if chunk)
			return text

		_href = [_['href'] for _ in _soup.find_all(has_href)]
		return {'title': _soup.title.get_text(), 'href': _href, 'content': html2text(_soup)}

	def __repr__(self):
		return 'Node({!r})'.format(self.normed_url)

	def add_all_child(self, _deep, _medium_res):  # add child url
		if self.normed_url not in Node.graph:
			Node.graph[self.normed_url] = {'out_links': set(), 'deep': _deep, 'in_links': set()}
		for _child_url in _medium_res['href']:
			# start doing norm url
			_session = requests.Session()
			_url_scheme, _url_netloc, _url_path = Node.conon_url(_child_url, self.normed_url)
			_child_url = urlunparse((_url_scheme, _url_netloc, _url_path, '', '', ''))

			# if old domain
			if _url_netloc in Node.domain_urls:
				# if blocked by robots.txt
				if Node.domain_urls[_url_netloc]['robot_rules']:
					try:
						_disallowed = Node.domain_urls[_url_netloc]['robot_rules'].disallowed(_child_url, '*')
					except:
						print('GIVEUP! robot.txt block {}'.format(_child_url))
						continue
					if _disallowed:
						print('GIVEUP! robot.txt block {}'.format(_child_url))
						continue
				# if allowed or no rule & not old path
				if _url_path not in Node.domain_urls[_url_netloc]['accessed']:
					Node.domain_urls[_url_netloc]['next_wave'].add(_url_path)

			else:  # if new domain
				_rules = Node.robot_rules(_url_scheme, _url_netloc)
				if _rules:
					try:
						_disallowed = _rules.disallowed(_child_url, '*')
					except:
						print('GIVEUP! robot.txt block {}'.format(_child_url))
						#print(_url)
						#print(_url_scheme, _url_netloc, _url_path)
						#print(self.normed_url, '\n')
						continue
					if _disallowed:
						print('GIVEUP! robot.txt block {}'.format(_child_url))
						continue
				# if allowed or no rules
				Node.domain_urls[_url_netloc] = {'accessed': {},
				                                 'next_wave': {_url_path},
				                                 'last_time': 0,
				                                 'domain_no': len(Node.domain_urls) + 1,
				                                 'robot_rules': _rules,
				                                 'session': _session,
				                                 'scheme': _url_scheme}
			# print('{} CHILD: {}'.format(Node.domain_urls[_url_netloc]['domain_no'], _child_url))
			# create child in Node.graph
			if _child_url not in Node.graph:
				Node.graph[_child_url] = {'out_links': set(), 'deep': _deep+1, 'in_links': {self.normed_url}}
			else:
				Node.graph[_child_url]['in_links'].add(self.normed_url)
			# updata current url in Node.graph
			Node.graph[self.normed_url]['out_links'].add(_child_url)

	def __iter__(self):
		return iter(Node.graph[self.normed_url]['out_links'])  # list of children urls

	@staticmethod
	def domain_1s_timer(_d_time):
		_time_diff = time.perf_counter() - _d_time
		if _time_diff < 1:
			time.sleep(_time_diff)
		elif _time_diff < 0:
			raise ValueError('domain time should be less than current time,\n domain time is {}'.format(_d_time))
		return time.perf_counter()

	@staticmethod
	# TODO: JSON and XML  r.json() #Requests中内置的JSON解码器
	# TODO: score page and filter it
	def download_html(_link):
		_parsed_url = urlparse(_link)
		_domain = _parsed_url.netloc
		if _parsed_url.netloc not in Node.domain_urls:
			Node.domain_urls[_domain] = {'accessed': {},
			                             'next_wave': set(),
			                             'last_time': 0,
			                             'domain_no': len(Node.domain_urls) + 1,
			                             'robot_rules': Node.robot_rules(_parsed_url.scheme, _parsed_url.netloc),
			                             'session': requests.Session(),
			                             'scheme': _parsed_url.scheme}

		try:
			_last_time = Node.domain_1s_timer(Node.domain_urls[_domain]['last_time'])
			_res = Node.domain_urls[_domain]['session'].get(_link, headers=Node.headers)
			_res.raise_for_status()
		except URLError as e:
			if hasattr(e, 'reason'):
				print('We failed to reach a server.')
				print('Reason: ', e.reason)
			elif hasattr(e, 'code'):
				print('The server couldn\'t fulfill the request.')
				print('Error code: ', e.code)
		except requests.RequestException as e:
			print('Status_code Error! ')
			print(e)
		else:
			Node.domain_urls[_domain]['last_time'] = _last_time
			#Node.domain_urls[_domain]['last_time'] = time.perf_counter()
			Node.domain_urls[_domain]['accessed'][_parsed_url.path] = len(Node.domain_urls[_domain]['accessed']) + 1
			resp_header = '\n'.join('{}: {}'.format(k, v) for k, v in eval(str(_res.headers)).items())
			return {'HTTPheader': resp_header, 'content': _res.text}  # auto decode to utf-8


	@staticmethod
	def conon_url(_url, _src_url):  # _src_netloc must inludes scheme and netloc
		if not isinstance(_url, str):
			print('URL should be string, now it is {}'.format(type(_url)))
			return
		_parsed_url = urlparse(_url)  # url1
		_url_netloc = Node._dupl_slash_dot_pattern.sub('/', _parsed_url.netloc)  # remove duplicate slash and dot

		#print(_parsed_url)
		if _url_netloc:
			if not Node._netloc_pattern.search(_url_netloc):
				print(_parsed_url)
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
		else:
			_url = urljoin(_src_url, _parsed_url.path)  # join netloc with parent url
			_parsed_url = urlparse(_url)

		#print(_parsed_url)
		if not _parsed_url.scheme:
			_url = urlunparse(('http', _parsed_url.netloc, _parsed_url.path, '', '', ''))
			_parsed_url = urlparse(_url)
		#print(_parsed_url)
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


'''
@staticmethod
	def conon_url_with_head(_url, _src_url, _session):  # _src_netloc must inludes scheme and netloc
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
		filename.write('content: {}\n\n\n'.format(''.join(_medium_res['content'])))

	for k, v in Node.domain_urls.items():
		print('domain: {}'.format(k))
		print('domain_no: {}'.format(v['domain_no']))
		print('accessed: \n{}'.format(v['accessed']))
		print('next_wave: \n{}'.format(v['next_wave']))

def conon_query(_q):
	_q = _q.lower()
	_q = _q.split()
	_q = [stem(_) for _ in _q]
	return _q

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

	start_nodes = Node.from_start_links(start_links)
	with open('results/test.txt', 'w', errors='ignore') as test:
		for _ in start_nodes:
			one_node_work(_, test, 0)

	print("--- {0} seconds ---".format(time.time() - start_time))
