import random
import time
import math
from stemming.porter2 import stem
from elasticsearch import Elasticsearch
import networkx as nx
from math import fabs, sqrt

from es_methods import *


def canon_query(_query):
	_query = _query.strip().lower().split()
	_query = [stem(_) for _ in _query]
	return _query


def root_set_okapi_bm25(_query_term_list, _avg_doc_len, _doc_count, _source_index, _my_type, _return_num=1000):
	_bm25_docs = {}
	# _bm25_docs = {_doc_id: {'okapi_bm25': y, 'doc_length': x, catagry_terms_name1: tf, catagry_terms_name2: tf},
	#               _doc_id: {'okapi_bm25': y, 'doc_length': x, catagry_terms_name1: tf, catagry_terms_name2: tf}...}
	_catagry_doc_freq = {}
	log = math.log
	# Collecting doc infomation
	print('Collecting doc infomation....')
	for catagry_terms_name, one_catagry_terms in _query_term_list.items():  # loop one kind of terms
		_canoned_query_list = canon_query(one_catagry_terms)
		max_doc_freq = 0
		x = 0
		# Collecting information fo doc
		for _term in _canoned_query_list:
			x += 1
			print('{} {}'.format(x, _term))
			for _doc_id, _doc_freq, _term_freq in doc_freq_AND_term_freq(es, _source_index, _my_type, _term):
				max_doc_freq = max(max_doc_freq, _doc_freq)
				if _doc_id in _bm25_docs:
					if catagry_terms_name in _bm25_docs[_doc_id]:
						_bm25_docs[_doc_id][catagry_terms_name] += _term_freq
					else:
						_bm25_docs[_doc_id][catagry_terms_name] = _term_freq
				else:
					_bm25_docs[_doc_id] = {}
					_bm25_docs[_doc_id]['doc_length'] = doc_length(_doc_id, es, _source_index, _my_type)
					_bm25_docs[_doc_id][catagry_terms_name] = _term_freq
				# print(_doc_id, _term, _bm25_docs[_doc_id][catagry_terms_name], _term_freq)

		_catagry_doc_freq[catagry_terms_name] = max_doc_freq
		# Calculate scores
		for _doc_id, _doc_details in _bm25_docs.items():  # loop all docs
			if catagry_terms_name in _doc_details:
				_term_freq = _doc_details[catagry_terms_name]
			else:  # no term in this doc
				_bm25_docs[_doc_id][catagry_terms_name] = 0
				_term_freq = 0

			_doc_length = _doc_details['doc_length']
			_okapi_bm25 = 2.2 * _term_freq / (_term_freq + 1.2 * (0.25 + 0.75 * _doc_length / _avg_doc_len)) * log(
				(_doc_count + 0.5) / (max_doc_freq + 0.5))  # tf_q = 1

			if 'okapi_bm25' in _doc_details:  # catagry terms appeared before
				_doc_details['okapi_bm25'] += _okapi_bm25
			else:  # catagry terms didn't appeared before
				_doc_details['okapi_bm25'] = _okapi_bm25

			# print(_doc_id, _term_freq, _okapi_bm25)
			# print('2.2 * {} / ({} + 1.2 * (0.25 + 0.75 * {} / {})) * log(({} + 0.5) / ({} + 0.5))'.format(_term_freq, _term_freq, _doc_length, _avg_doc_len, _doc_count, max_doc_freq))
			#
			# print(_doc_details['okapi_bm25'], _okapi_bm25)
	# Writing doc info into file
	print('Writing doc info into file')
	with open('doc_set.txt', 'w', errors='replace') as doc_set_file:
		for _doc_id, _doc_details in _bm25_docs.items():
			doc_set_file.write('{}\t{}\t{}\n'.format(_doc_id, _doc_details['doc_length'], _doc_details['okapi_bm25']))
			_bm25_docs[_doc_id] = _doc_details['okapi_bm25']
	print('Finished root_set_okapi_bm25')
	return set(sorted(_bm25_docs, key=_bm25_docs.get, reverse=True)[:_return_num])


def in_links_AND_out_links(_id, _es_instance, _target_index):
	_res = _es_instance.search(index=_target_index,
	                           doc_type='document',
	                           body={"query": {"match": {"_id": _id}}},
	                           fields=["in_links", "out_links"],
	                           request_timeout=30)
	_in_links = set()
	_out_links = set()
	if _res:
		if _res['hits']['hits']:

			try:
				_in_links = _res['hits']['hits'][0]['fields']['in_links']
			except:
				pass
			else:
				if _in_links:
					_in_links = set(_res['hits']['hits'][0]['fields']['in_links'])
					for _in_link in _in_links:
						if not _es_instance.exists(index=_target_index, doc_type='document', id=_in_link,
						                           request_timeout=30, ignore=[400, 404]):
							_in_links.discard(_in_link)

			try:
				_out_links = _res['hits']['hits'][0]['fields']['out_links']
			except:
				pass
			else:
				if _out_links:
					_out_links = set(_res['hits']['hits'][0]['fields']['out_links'])
					for _out_link in _out_links:
						if not _es_instance.exists(index=_target_index, doc_type='document', id=_out_link,
						                           request_timeout=30, ignore=[400, 404]):
							_out_links.discard(_out_link)

	return _in_links, _out_links


def base_set(_root_set, _es_instance, _target_index, d=200, _base_set_size=10000, from_file=False):
	random_sample = random.sample
	_base_set = _root_set.copy()
	_in_out_graph = nx.DiGraph()
	in_out_graph = open('in_out_graph.txt', 'w', errors='replace')
	# Updating _in_out_graph
	print('Updating _in_out_graph')
	_x = 0
	for _page in _root_set:
		_x += 1
		print('{} {}'.format(_x, _page))
		_in_links, _out_links = in_links_AND_out_links(_page, _es_instance, _target_index)
		in_out_graph.write('{}|'.format(_page))
		if _in_links:
			for _in_link in _in_links:
				_in_out_graph.add_edge(_in_link, _page)
				in_out_graph.write('{}\t'.format(_in_link))
		in_out_graph.write('|')
		if _out_links:
			for _out_link in _out_links:
				_in_out_graph.add_edge(_page, _out_link)
				in_out_graph.write('{}\t'.format(_out_link))
		in_out_graph.write('\n')

	# TODO: write graph into file
	# Creating base set
	print('Size of base_set {}'.format(len(_base_set)))
	print('Updating in_links into base set...')
	while len(_base_set) < _base_set_size:
		for _page in _root_set:
			_out_links = {_edge[1] for _edge in _in_out_graph.out_edges_iter(_page)}
			_base_set.update(_out_links)
			_in_links = {_edge[0] for _edge in _in_out_graph.in_edges_iter(_page)}
			if len(_in_links) < d:
				_base_set.update(_in_links)
			else:
				_base_set.update(set(random_sample(_in_links, d)))
			if len(_base_set) > _base_set_size:
				break

	in_out_graph.close()
	return _base_set


def base_set_2_graph(_base_set, _es_instance, _target_index):
	in_out_graph = nx.DiGraph()
	for _link in _base_set:
		_res = _es_instance.search(index=_target_index,
		                           doc_type='document',
		                           body={"query": {"match": {"_id": _link}}},
		                           fields=["in_links", "out_links"],
		                           request_timeout=30)
		if _res['hits']['hits']:
			try:
				_in_links = _res['hits']['hits'][0]['fields']['in_links']
			except:
				pass
			else:
				if _in_links:
					_in_links = set(_in_links)
					if _in_links:
						for _in_link in _in_links:
							if _in_link in _base_set:
								in_out_graph.add_edge(_in_link, _res['hits']['hits'][0]['_id'])

			try:
				_out_links = _res['hits']['hits'][0]['fields']['out_links']
			except:
				pass
			else:
				if _out_links:
					_out_links = set(_out_links)
					if _out_links:
						for _out_link in _out_links:
							if _out_link in _base_set:
								in_out_graph.add_edge(_res['hits']['hits'][0]['_id'], _out_link)


	for _node in in_out_graph.nodes_iter():
		in_out_graph.node[_node]['in_links_count'] = len(in_out_graph.in_edges(_node))
		in_out_graph.node[_node]['out_links_count'] = len(in_out_graph.out_edges(_node))
	return in_out_graph


# used by HITS_score
def ditermine_converged(_score, _new_score, _exponent=11):
	for page in _score.keys():
		if fabs(_score[page][0] - _new_score[page][0]) > 1 / 10 ** _exponent:  # this spent 25min to converged,
			return False
		if fabs(_score[page][1] - _new_score[page][1]) > 1 / 10 ** _exponent:  # this spent 25min to converged,
			return False
	return True


# TODO: HITS Score
def HITS_score(_base_graph):
	_HITS_score = {_page: [1, 1] for _page in _base_graph.nodes_iter()}  # [auth, hub]
	_new_HITS_score = _HITS_score.copy()

	while True:
		for _page in _HITS_score.keys():
			for _in_link in _base_graph.in_edges_iter(_page):
				_new_HITS_score[_page][0] += _HITS_score[_in_link[0]][1]

			for _out_link in _base_graph.out_edges_iter(_page):
				_new_HITS_score[_page][1] += _HITS_score[_out_link[1]][0]

		# Starting norm
		d_a = sqrt(sum(_[0] ** 2 for _ in _new_HITS_score.values()))
		for _page in _new_HITS_score.keys():
			_new_HITS_score[_page][0] = _new_HITS_score[_page][0] / d_a

		d_h = sqrt(sum(_[1] ** 2 for _ in _new_HITS_score.values()))
		for _page in _new_HITS_score.keys():
			_new_HITS_score[_page][1] = _new_HITS_score[_page][1] / d_h

		if not ditermine_converged(_HITS_score, _new_HITS_score, _exponent=9):
			_HITS_score = _new_HITS_score.copy()
		else:
			break
	print('-' * 30, 'CONVERGED!!!', '-' * 30)
	return _HITS_score


def top_500_auth(_HITS_score):
	return sorted(_HITS_score.keys(), key=lambda x: _HITS_score[x][0], reverse=True)[:500]


def top_500_hub(_HITS_score):
	return sorted(_HITS_score.keys(), key=lambda x: _HITS_score[x][1], reverse=True)[:500]


def root_from_file(filename):
	with open(filename, 'r', errors='replace') as _root_set_file:
		_root_set = set([_.strip() for _ in _root_set_file])
	return _root_set


if __name__ == '__main__':
	start_time = time.time()

	es = Elasticsearch()
	source_index = 'maritimeaccidents'
	test_index = 'test'
	my_type = 'document'

	terms_list = {'meri_terms': 'WATER SEA OCEAN MARITIME OFFSHORE HYDRODYNAMIC SHOAL WATERWAY WATERLINE',
	              'acc_terms': 'ACCIDENTS FIRE IGNITE COLLISION COLLIDE INJURED DAMAGE SAFE POLLUTION STRUCK ALLISION RUPTUR BREACH FLOOD',
	              'ship_terms': 'SHIP BOAT TOWBOAT VESSEL AFT BARGE TANKER JETTY HULL ABOARD PILOT OPERATOR VISIBILITY CAPTAIN CREW STEER ANCHOR'}

	test_terms_list = {'meri_terms': 'algorithm',
	                   'acc_terms': 'free',
	                   'ship_terms': 'encyclopedia'}

	# # Collecting root set
	# print('Collecting root set...')
	# avg_doc_len, doc_count = avg_doc_len_AND_doc_count(es, source_index, my_type)
	# print('avg_doc_len {}, doc_count {}'.format(avg_doc_len, doc_count))
	# # _query_term_list, _avg_doc_len, _doc_count, _source_index, _my_type, _return_num=1000
	# first_set_1000 = root_set_okapi_bm25(terms_list, avg_doc_len, doc_count, source_index, my_type)
	# print(first_set_1000)
	# with open('root_set.txt', 'w', errors='replace') as root_set_file:
	# 	for page in first_set_1000:
	# 		root_set_file.write('{}\n'.format(page))
	#
	first_set_1000 = root_from_file('root_set.txt')
	print(first_set_1000)

	# # Collecting base set
	# base_set_10000 = base_set(first_set_1000, es, source_index)
	# with open('base_set.txt', 'w', errors='replace') as base_set_file:
	# 	for page in base_set_10000:
	# 		base_set_file.write('{}\n'.format(page))

	base_set_10000 = root_from_file('base_set.txt')
	print(base_set_10000)

	base_graph = base_set_2_graph(base_set_10000, es, source_index)
	_HITS_score = HITS_score(base_graph)
	with open('top_500_auth.txt', 'w', errors='replace') as top_500:
		t_500_a = top_500_auth(_HITS_score)
		print(t_500_a)
		for top in t_500_a:
			top_500.write('{}\t{}\t{}\n'.format(top, _HITS_score[top][0], base_graph.node[top]['in_links_count']))

	with open('top_500_hub.txt', 'w', errors='replace') as top_500:
		t_500_h = top_500_hub(_HITS_score)
		print(t_500_h)
		for top in t_500_h:
			top_500.write('{}\t{}\t{}\n'.format(top, _HITS_score[top][1], base_graph.node[top]['out_links_count']))

	print("--- {0} seconds ---".format(time.time() - start_time))
