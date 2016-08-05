'''
restore_query_es.py uesd by load_data_es.py,
It parse the queries, restore qrels into dict,
'''

import pprint
from collections import defaultdict

from load_data_es import *


def get_stop_set(stplst_path=r'AP_DATA\stoplist.txt'):
	'''
	get a set, including all stop words
	:param stplst_path:
	:return:
	'''
	with open(stplst_path, 'r', errors='replace') as s:
		s_set = set(s.read().strip().split())
	return s_set


# this is a generator
def all_queries(_query_file=r'AP_DATA\modified_queries.txt'):
	'''
	a gernerator to read queries by line from query txt
	:param _query_file:
	:return:
	'''
	with open(_query_file, 'r', errors='replace') as q:
		_query_set = set()
		for _ in q:
			_query_set.add(_.strip())
	for _q in filter(len, _query_set):
		yield _q


def get_query_list(_query, stopwords_set):
	'''
	parse a query string into query id and query list, without stopwords
	:param _query:
	:param stopwords_set:
	:return:
	'''
	_query = _query.strip().split()

	# remove ., ,, (, ) in query
	for _term in _query.copy():
		_query.remove(_term)
		if _term != 'U.S.':
			_term = re.split(r'[,\.\(\)\"]', _term.strip())
			_term = list(filter(None, _term))[0]
			_query.append(_term)
		else:
			_query.append('U.S.')

	_query_id = int(_query[0])
	_query = set(_query[1:])

	# remove stop words in query
	for _term in _query.copy():
		if _term in stopwords_set:
			_query.discard(_term)
	return {_query_id: _query}


def load_qrels(_query_dict, _qrels_file='AP_DATA\qrels.adhoc.51-100.AP89.txt'):
	'''
	from qrels txt get docs relevant label, and restore by query id
	_result_dict = {query_id: {'doc_id': {'label": 1}, 'doc_id': {'label": 0}, ...},
					query_id: {'doc_id': {'label": 1}, 'doc_id': {'label": 0}, ...}, ...}
	:param _query_dict:
	:param _qrels_file:
	:return:
	'''
	# _all_doc_id_set = set()
	with open(_qrels_file, 'r', errors='replace') as _qrels:
		_results_dict = defaultdict(dict)
		for _ in _qrels:
			_ = _.strip().split()
			_query = int(_[0])
			if _query in _query_dict.keys():
				if _query not in _results_dict:
					_results_dict[_query] = defaultdict(dict)
				_results_dict[_query][_[2]]['label'] = int(_[3])

	return _results_dict
				# _all_doc_id_set.add(_[2])

	# # write all doc id whose has a label in to a file
	# with open('all_doc_id.txt', 'w', errors='replace') as f:
	# 	for _doc_id in _all_doc_id_set:
	# 		f.write('{}\n'.format(_doc_id))


def get_query_dict():
	_stop_set = get_stop_set()
	# print(stop_set)
	_query_dict = {}
	for _ in all_queries():
		_query = get_query_list(_, _stop_set)
		_query_dict.update(_query)
	return _query_dict

if __name__ == '__main__':
	# es = Elasticsearch()
	# target_index = 'hw6_ap_dataset'
	# doc_type = 'document'


	stop_set = get_stop_set()
	# print(stop_set)
	query_dict = {}
	for _ in all_queries():
		query = get_query_list(_, stop_set)
		query_dict.update(query)

	results_dict = load_qrels(query_dict)

	_write_es_flag = False
	if _write_es_flag:
		whole_prep_dataset(es, target_index, doc_type)
	pass
