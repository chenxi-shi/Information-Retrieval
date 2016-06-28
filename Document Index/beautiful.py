import os
import re
from os.path import dirname, abspath

import time
from stemming.porter2 import stem
from nltk.stem.porter import *


def token_text(text):
	_tk_text = []
	# match token format (remove tokens not alpha and number)
	regex = re.compile(r"(([a-z0-9]+\.)*[a-z0-9]+)")
	_ = text.lower()
	_ = regex.findall(_)
	for _t in _:
		_t = _t[0]
		if re.match(r"\A([a-z]\.)+[a-z]\Z", _t):
			_t += '.'  # for u.s.
		_tk_text.append(_t)
	# print(_tk_text)
	return _tk_text


def comb_tokens(tokens_41):
	"""
	Sortly putting one doc tokens into tokens dict for 1000 docs
	tokens =
	{
	term_id:{ttf: y,
			details: [{'doc_id': doc_id, 'tf': x, 'tp': [ , ]},
					{'doc_id': doc_id, 'tf': x, 'tp': [ , ]}, ...]},
	term_id:{ttf: y,
			details: [{'doc_id': doc_id, 'tf': x, 'tp': [ , ]},
					{'doc_id': doc_id, 'tf': x, 'tp': [ , ]}, ...]},
	...}
	:return:
	"""
	for te, d in tokens_41.items():
		if te in parse_docs.tokens:
			lo = 0  # high tf
			hi = len(parse_docs.tokens[te]['details'])  # low tf
			while lo < hi:
				mid = (lo + hi) // 2
				if d['details'][0]['tf'] == parse_docs.tokens[te]['details'][mid]['tf']:
					lo = mid + 1
					break
				elif d['details'][0]['tf'] > parse_docs.tokens[te]['details'][mid]['tf']:
					hi = mid
				else:
					lo = mid + 1
			parse_docs.tokens[te]['ttf'] += d['ttf']
			parse_docs.tokens[te]['details'].insert(lo, d['details'][0])
		else:
			parse_docs.tokens[te] = d



def index_tokens(doc_id, text):
	"""
	one doc tokens
	{
	term_id:{ttf: y,
			details: [{'doc_id': doc_id, 'tf': x, 'tp': [ , ]}]},
	term_id:{ttf: y,
			details: [{'doc_id': doc_id, 'tf': x, 'tp': [ , ]}]},
	...}
	Resolve tokens in 1 document
	Tokenize,
	leave token with wrong format
	leave stopwords
	stemming tokens
	add into term-map
	:param doc_id:
	:param docno:
	:param text:
	:return: No return
	"""
	_tokens_41 = {}
	_tokened_text = token_text(text)
	_doc_len = 0

	for idx in range(len(_tokened_text)):  # iterate every token
		a_token = _tokened_text[idx]
		if a_token in parse_docs.stop_words:  # remove stop words
			pass
		else:
			_doc_len += 1
			st_term = stem(a_token)
			if st_term in parse_docs.term_map:  # add word to term-map
				pass
			else:
				# for fast search term_map = {term: term_id}
				parse_docs.term_map[st_term] = len(parse_docs.term_map) + 1

			term_id = parse_docs.term_map[st_term]  # return term's id from term_map
			if term_id in _tokens_41:
				_tokens_41[term_id]['ttf'] += 1
				_tokens_41[term_id]['details'][0]['tf'] += 1
				_tokens_41[term_id]['details'][0]['tp'].append(idx + 1)
			else:
				_tokens_41[term_id] = {'ttf': 1, 'details': [{'doc_id': doc_id, 'tf': 1, 'tp': [idx + 1]}]}
	parse_docs.doc_map[doc_id].append(_doc_len)
	return _tokens_41


def parse_doc(doc):
	"""
	parse out docno and text inside one doc string
	:param doc:
	:return:
	"""
	_docno = ''
	_text = ''
	_doc_id = len(parse_docs.doc_map) + 1
	_ = iter(doc.splitlines())
	for line in _:
		# meet docno
		if re.match(r'^(<({0}|{1})>\s*)(.*?)(\s*</({0}|{1})>)$'.format('docno', 'docno'.upper()), line):
			_docno = \
				re.sub(r'(<({0}|{1})>\s*)|(\s*</({0}|{1})>)'.format('docno', 'docno'.upper()), '', line).split()[0]
			_doc_id = len(parse_docs.doc_map) + 1
			parse_docs.doc_map[_doc_id] = [_docno]  # doc_id: [docno, doc_len]
		# meet text
		elif re.match(r'^<({}|{})>$'.format('text', 'text'.upper()), line):
			while True:  # go in one text
				line = next(_)
				if re.match(r'^</({}|{})>$'.format('text', 'text'.upper()), line):
					break  # leave one text
				else:
					_text += line + '\n'  # append text
		# meet others
		else:
			pass
	print(_docno)
	# exit()
	# print(_text)
	return _doc_id, _text


class parse_docs(object):
	'''
	functions:
	parse_file(pause_num=1000)
	index_tokens(docno, text)
	'''
	st = PorterStemmer()
	term_map = {}
	tokens = {}
	finished = False
	stop_words = None
	doc_map = {}  # docno: [doc_id, doc_len]

	def __init__(self, doc_path):
		self.doc_path = doc_path
		self.pause_file = os.listdir(doc_path)[0]
		self.pause_pointer = None
		self.exe_time = 0
		self.doc_b = 0
		with open('stoplist.txt') as f:
			parse_docs.stop_words = f.read().split()
		parse_docs.stop_words = set(parse_docs.stop_words)

	def parse_file(self, pause_num=1000):
		"""
		parse files and iterate docno & text,
		stop when parsed pause_num(1000) docs,
		and set finished to True

		:param pause_num:
		:return:
		"""
		_doc = ''
		_parsed_doc_num = 0
		for _ in os.listdir(self.doc_path)[os.listdir(self.doc_path).index(self.pause_file):]:
			if re.match(r"^ap89\d+", _):
				with open('{}/{}'.format(self.doc_path, _), 'r', errors='ignore') as f:
					if self.pause_pointer:
						f.seek(self.pause_pointer, 0)
						self.pause_pointer = None
					while True:  # in file
						line_byte = f.tell()
						line = f.readline()
						if not line:  # end of the file
							break
						# TODO put parse doc into this function, less matches, speed up
						if re.match(r'^<({}|{})>$'.format('doc', 'doc'.upper()), line):
							# beginning of one doc
							_doc = ''
							self.doc_b = line_byte
							_parsed_doc_num += 1
							if _parsed_doc_num > pause_num:  # paused #1000 document
								self.pause_file = _  # restore pause file id
								self.pause_pointer = self.doc_b  # restore pause pointer
								self.exe_time += 1
								return None, None  # jump out of function
						elif re.match(r'^</({}|{})>$'.format('doc', 'doc'.upper()), line):
							# ending of one doc
							yield parse_doc(_doc)  # return doc_id, text
						else:
							# inside the doc
							_doc += line
		# finished reading all of files
		self.exe_time += 1
		parse_docs.finished = True

	def produce_1(self, res_path, pause_num=1000):
		"""
		term_id:ttf|doc_id,tf,tp,tp,tp,...|doc_id,tf,tp,tp,tp,...|...
		term_id:ttf|doc_id,tf,tp,tp,tp,...|doc_id,tf,tp,tp,tp,...|...
		...
		write 1000 into one file : INV and CAT
		:param res_path:
		:param pause_num:
		:return:
		"""
		parse_docs.tokens = {}  # clear up tokens for next 1000 docs
		for _doc_id, _text in self.parse_file(pause_num=pause_num):
			if not _doc_id:  # pause at 1000 docs
				break  # write 1000 doc into file
			_tokens_41 = index_tokens(doc_id=_doc_id, text=_text)  # resolve tokens
			comb_tokens(tokens_41=_tokens_41)  # put one_doc tokens into token's dict
		# write these 1000 docs into one file
		cat = open('{}/CAT_{}'.format(res_path, self.exe_time * pause_num), 'w')
		with open('{}/INV_{}'.format(res_path, self.exe_time * pause_num), 'w') as inv:
			for te, d in parse_docs.tokens.items():
				t_b = inv.tell()
				inv.write('{}:{}'.format(te, d['ttf']))
				for doc in d['details']:
					inv.write('|{},{}'.format(doc['doc_id'], doc['tf']))
					for tp in doc['tp']:
						inv.write(',{}'.format(tp))
				t_e = inv.tell()
				#inv.write('\n')
				cat.write('{}\t{}\t{}\n'.format(te, t_b, t_e-t_b))
		cat.close()

	def produce_all(self, res_path, pause_num=1000):
		for _ in os.listdir(res_path):
			print('Remove old results')
			try:
				os.remove('{}/{}'.format(res_path, _))
			except OSError:
				pass
		while not self.finished:  # iterate all of docs in directory and write files
			self.produce_1(res_path, pause_num)

file_dir = dirname(dirname(abspath('__file__')))
start_time = time.time()
'''
a = parse_docs('{}\\dataset'.format(file_dir))
res_path = file_dir + '/medium_res'
a.produce_all(res_path, 3)
'''
a = parse_docs('{}\\ap89_collection'.format(file_dir))
res_dir = file_dir + '/medium_res'

a.produce_all(res_dir, 1000)

with open('{}/medium_res/term_map'.format(file_dir), 'w') as t_m:
	for t in sorted(parse_docs.term_map, key=parse_docs.term_map.get):
		t_m.write('{}\t{}\n'.format(parse_docs.term_map[t], t))
with open('{}/medium_res/doc_map'.format(file_dir), 'w') as d_l:
	for d_id in sorted(parse_docs.doc_map):  # doc_id: [docno, doc_len]
		d_l.write('{}\t{}\t{}\n'.format(d_id, parse_docs.doc_map[d_id][0],  parse_docs.doc_map[d_id][1]))

print('--- {} seconds ---'.format(time.time() - start_time))
