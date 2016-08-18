import os
import pprint
import sys
from collections import defaultdict
from os.path import dirname, abspath
from re import compile, match

import requests
from elasticsearch import Elasticsearch
from elasticsearch import ElasticsearchException
from elasticsearch.helpers import bulk, scan
from stemming.porter2 import stem

import settings


def create_setting(es_instance, _target_index,
                   stplst_path="stoplist.txt"):  # stoplist.txt has to be put in es config folder
	index_settings = {
		"index": {
			"store": {
				"type": "default"
			},
			"number_of_shards": 1,
			"number_of_replicas": 0
		},
		"analysis": {
			"analyzer": {
				"english_text": {
					"type": "english",
					"stopwords_path": stplst_path,
					"tokenizer": "whitespace",
					"filter": [#"standard",
					           "lowercase",
					           "asciifolding",
					           "my_stemmer"]
				}
			}
		},
		"filter": {
			"my_stemmer": {
				"type": "stemmer",
				"name": "porter2"
			}
		}
	}
	# ,
	# "protwords": {
	# 	"type": "keyword_marker",
	# 	"keywords": ["$", "$$", "$$$"]
	# }
	# create empty index
	if es_instance.indices.exists(index=_target_index):
		print("Index {} exists. Delete and rebuild it.".format(_target_index))
		es_instance.indices.delete(index=_target_index)
	try:
		es_instance.indices.create(
			index=_target_index,
			body={"settings": index_settings}  # ,
			# ignore=[400, 404]
		)
	except ElasticsearchException as e:
		e2 = sys.exc_info()
		print(e)
		pprint.pprint("<p>Error: {}</p>".format(e2))


def put_my_mapping(es_instance, _target_index, _doc_type):
	"""
	put_mapping only used to create query mapping
	:param query_id:
	:param es_instance:
	:param _target_index:
	:return:
	"""
	doc_mappings = {
		_doc_type: {
			"properties": {
				"subject": {
					"type": "string",
					"store": True,
					"index": "not_analyzed"
				},
				"text": {
					"type": "string",
					"store": True,
					"index": "analyzed",
					"term_vector": "with_positions_offsets_payloads",
					"analyzer": "english_text"
				},
				"spam": {
					"type": "integer",
					"store": True,
					"index": "not_analyzed"
				},
				"features": {
					# "type": "object",  # cannot use nested, when insert with {}, there would be some error
					"dynamic": True,  # object is not support store: True
					"properties": {
						"from": {"type": "string", "index": "not_analyzed"},
						"to": {"type": "string", "index": "not_analyzed"},
						"weird_char": {"type": "integer", "index": "not_analyzed"},
						"weird_addr": {"type": "integer", "index": "not_analyzed"},
						"weird_sbj": {"type": "integer", "index": "not_analyzed"},
						"weird_target": {"type": "integer", "index": "not_analyzed"},
						"weird_content": {"type": "integer", "index": "not_analyzed"},
						"weird_msg_id": {"type": "integer", "index": "not_analyzed"},
						"servers_count": {"type": "integer", "index": "not_analyzed"},
						"span_time": {"type": "integer", "index": "not_analyzed"},
						"wrong_time": {"type": "integer", "index": "not_analyzed"}
					}
				}
			}
		}
	}
	if es_instance.indices.exists(index=_target_index):
		# TODO: debug;
		print("putting mapping")
		try:
			es_instance.indices.put_mapping(
				index=_target_index,
				doc_type=_doc_type,
				body=doc_mappings,
				# ignore=[400, 404]
			)
		except ElasticsearchException as e:
			e2 = sys.exc_info()
			print(e)
			pprint.pprint("<p>Error: {}</p>".format(e2))
	else:
		print("the index {} is not exits, exit now.".format(_target_index))
		exit(-1)


def create_dataset(es_instance, _target_index,
                   _train_type="for_train", _test_type="for_test"):
	try:  # Check status of ES server
		requests.get("http://localhost:9200")
	except:
		print("Elasticsearch service has not be started, run it now.")
		import subprocess
		subprocess.check_call(r"C:\Users\Chenxi\Desktop\HW3\DOCs\elasticsearch-2.3.3\bin\elasticsearch",
		                      shell=True)
		print('after subprocess')

	# path is the parent dir of __file__"s location
	path = dirname(abspath("__file__"))
	resource_path = os.path.join(path, "AP_DATA")
	doc_path = os.path.join(resource_path, "ap89_collection")
	stop_list_path = os.path.join(resource_path, "stoplist.txt")

	with open("finished_docs.txt", "w", errors="replace", encoding='utf8') as _ff:
		pass
	create_setting(es_instance, _target_index)
	put_my_mapping(es_instance, _target_index, _train_type)
	put_my_mapping(es_instance, _target_index, _test_type)


def search_string_in_text(_str, _text):
	_pattern = compile(r"{}".format(_str))
	_match = _pattern.findall(_text)
	return len(_match)


def term_freq(_es_instance, _my_index, _my_type, _term_string, _search_field, _search_size=10000):
	'''
	used for not_analyzed strings
	:param _es_instance:
	:param _my_index:
	:param _my_type:
	:param _term_string:
	:param _search_field:
	:param _search_size:
	:return:
	'''
	_body = {
		"query": {
			"wildcard": {
				_search_field: {
					"value": "*{}*".format(_term_string)
				}
			}
		},
		"fields": [_search_field]
	}
	_res = _es_instance.search(index=_my_index, doc_type=_my_type,
	                           _source=False,
	                           size=_search_size,
	                           request_timeout=40,
	                           body=_body)

	# pprint.pprint(_res)
	for _hit in _res["hits"]["hits"]:
		_term_freq = search_string_in_text(_term_string, _hit["fields"][_search_field][0])
		yield _hit["_id"], _term_freq


def search_explain_tree(_tree, _search_pattern):
	if "description" in _tree:
		if _search_pattern.match(_tree["description"]):
			# print('get termFreq : {}'.format(_tree["value"]))
			return _tree["value"]
		else:
			if "details" in _tree and _tree["details"]:
				for _sub_tree in _tree["details"]:
					_search_res = search_explain_tree(_sub_tree, _search_pattern)
					if _search_res:
						return _search_res
			else:
				return False


def unigram_term_freq(_es_instance, _my_index, _my_type, _term, _search_field,
                      stopwords_set, _type_size=10000):
	tf_pattern = compile(r"termFreq=.+")
	if _term in stopwords_set:
		return {}
	_body = {
		"query": {
			"match": {_search_field: _term}
		},
		"fields": []
	}
	_res = _es_instance.search(index=_my_index, doc_type=_my_type,
	                           _source=False,
	                           df="text",
	                           explain=True,
	                           size=_type_size,
	                           request_timeout=40,
	                           body=_body)

	# _es_instance.refresh(index=_my_index)
	# pprint.pprint(_res)
	_df = _res["hits"]["total"]
	_tf_dict = {}
	if _df > 0:
		for _hit in _res["hits"]["hits"]:
			if "_explanation" in _hit:
				_tree = _hit["_explanation"]
				# yield df, doc_id, tf
				_tf = search_explain_tree(_tree, tf_pattern)
				if not _tf:
					pprint.pprint(_hit["_explanation"])
					exit(-1)
				_tf_dict[_hit["_id"]] = _tf

	return _tf_dict


def multigram_term_freq(_es_instance, _my_index, _my_type, _spam_terms_lst, _search_field,
                        stopwords_set, return_size=10000):
	tf_pattern = compile(r"phraseFreq=.+")
	clouses = []
	for term in _spam_terms_lst:
		if term not in stopwords_set:
			clouses.append(
				{
					"span_term": {_search_field: stem(term)}
				}
			)
	if clouses:
		if len(clouses) == 1:
			_term = clouses[0]["span_term"][_search_field]
			return unigram_term_freq(_es_instance, _my_index, _my_type, _term, _search_field,
			                         stopwords_set)
	else:
		return {}
	# print(clouses)
	_body = {
		"query": {
			"bool": {
				"must": [
					{
						"span_near": {
							"clauses": clouses,
							"slop": 0,
							"in_order": True,
							"collect_payloads": False
						}
					}
				]
			}
		},
		"fields": [],
		"explain": True
	}
	# pprint.pprint(_body)
	_res = _es_instance.search(index=_my_index, doc_type=_my_type,
	                           _source=False,
	                           df="text",
	                           explain=True,
	                           size=return_size,
	                           request_timeout=40,
	                           body=_body)
	if len(_res["hits"]["hits"]) > return_size:
		print("TOO MUCH hits!")
		print(len(_res["hits"]["hits"]))
	_df = _res["hits"]["total"]
	_tf_dict = {}
	if _df > 0:
		for _hit in _res["hits"]["hits"]:
			if "_explanation" in _hit:
				_tree = _hit["_explanation"]
				# yield df, doc_id, tf
				_tf = search_explain_tree(_tree, tf_pattern)
				if not _tf:
					pprint.pprint(_hit["_explanation"])
					exit(-1)
				_tf_dict[_hit["_id"]] = _tf

	return _tf_dict


def get_header_features(_es_instance, _my_index, _my_type):
	_features_dict = defaultdict(dict)
	_query = {
		"query": {
			"match_all": {}
		},
		"fields": [
			"spam",
			"features.wrong_time",
			"features.weird_sbj",
			"features.weird_msg_id",
			"features.servers_count",
			"features.weird_target",
			"features.weird_content",
			"features.weird_char",
			"features.weird_addr",
			"features.span_time"
		]
	}
	for _doc in scan(_es_instance, index=_my_index, doc_type=_my_type, query=_query):
		_features_dict[int(_doc["_id"])] = {
			"features": {
				'servers_count': _doc["fields"]["features.servers_count"][0],
				'span_time': _doc["fields"]["features.span_time"][0],
				'weird_addr': _doc["fields"]["features.weird_addr"][0],
				'weird_char': _doc["fields"]["features.weird_char"][0],
				'weird_content': _doc["fields"]["features.weird_content"][0],
				'weird_msg_id': _doc["fields"]["features.weird_msg_id"][0],
				'weird_sbj': _doc["fields"]["features.weird_sbj"][0],
				'weird_target': _doc["fields"]["features.weird_target"][0],
				'wrong_time': _doc["fields"]["features.wrong_time"][0]
			},
			'spam': _doc["fields"]["spam"][0]
		}

	return _features_dict


def get_header_features_backup(_es_instance, _my_index, _my_type):
	_features_dict = defaultdict(dict)
	_query = {
		"query": {
			"match_all": {}
		},
		"fields": [
			"spam",
			"features.wrong_time",
			"features.weird_sbj",
			"features.weird_msg_id",
			"features.servers_count",
			"features.weird_target",
			"features.weird_content",
			"features.weird_char",
			"features.weird_addr",
			"features.span_time"
		]
	}
	for _doc in scan(_es_instance, index=_my_index, doc_type=_my_type, query=_query):
		# pprint.pprint(_doc)
		# break
		# print(type(_doc["fields"]["features.servers_count"][0]))
		_features_dict[_doc["_id"]] = {
			'servers_count': _doc["fields"]["features.servers_count"][0],
			'span_time': _doc["fields"]["features.span_time"][0],
			'weird_addr': _doc["fields"]["features.weird_addr"][0],
			'weird_char': _doc["fields"]["features.weird_char"][0],
			'weird_content': _doc["fields"]["features.weird_content"][0],
			'weird_msg_id': _doc["fields"]["features.weird_msg_id"][0],
			'weird_sbj': _doc["fields"]["features.weird_sbj"][0],
			'weird_target': _doc["fields"]["features.weird_target"][0],
			'wrong_time': _doc["fields"]["features.wrong_time"][0],
			'spam': _doc["fields"]["spam"][0],
		}
		_features_dict[_doc["_id"]]["features_all"] = [_features_dict[_doc["_id"]]['servers_count'],
		                                               _features_dict[_doc["_id"]]['span_time'],
		                                               _features_dict[_doc["_id"]]['weird_addr'],
		                                               _features_dict[_doc["_id"]]['weird_char'],
		                                               _features_dict[_doc["_id"]]['weird_content'],
		                                               _features_dict[_doc["_id"]]['weird_msg_id'],
		                                               _features_dict[_doc["_id"]]['weird_sbj'],
		                                               _features_dict[_doc["_id"]]['weird_target'],
		                                               _features_dict[_doc["_id"]]['wrong_time']]

	return _features_dict


def unique_term_count(_es_instance, _my_index, _my_type, _search_field, _doc_id=False):
	body = {
		"aggs": {
			"unique_terms": {
				"cardinality": {
					"field": _search_field
				}
			}
		},
		"size": 0
	}
	# Second method: replace ""field": _search_field" with
	# "script": "doc[\"{}\"].values".format(_search_field)
	if _doc_id:
		body["query"] = {"match": {"_id": _doc_id}}
	res = _es_instance.search(index=_my_index,
	                          doc_type=_my_type,
	                          request_timeout=40,
	                          body=body)
	# pprint.pprint(res)
	return int(res["aggregations"]["unique_terms"]["value"])


def generate_all_doc_id_list(_es_instance, _my_index, _my_type="_all", _int_doc_id=True):
	_docs_lst = []
	_true_value_lst = []
	if _my_type == "_all":
		for _doc in scan(_es_instance, index=_my_index,
		                 query={"query": {"match_all": {}},
		                        "fields": ["spam"]}):
			_true_value_lst.append(_doc['fields']['spam'][0])
			if _int_doc_id:
				_docs_lst.append(int(_doc['_id']))
			else:
				_docs_lst.append(_doc['_id'])
	else:
		for _doc in scan(_es_instance, index=_my_index, doc_type=_my_type,
		                 query={"query": {"match_all": {}},
		                        "fields": ["spam"]}):
			_true_value_lst.append(_doc['fields']['spam'][0])
			if _int_doc_id:
				_docs_lst.append(int(_doc['_id']))
			else:
				_docs_lst.append(_doc['_id'])

	return _docs_lst, _true_value_lst


def generate_all_doc_list(_es_instance, _my_index, _my_type="_all"):
	_docs_lst = []
	if _my_type == "_all":
		for _doc in scan(_es_instance, index=_my_index,
		                 query={"query": {"match_all": {}}}):
			_docs_lst.append(_doc)
	else:
		for _doc in scan(_es_instance, index=_my_index, doc_type=_my_type,
		                 query={"query": {"match_all": {}}}):
			_docs_lst.append(_doc)

	return _docs_lst


def doc_termvector(_es_instance, _source_index, _my_type, _search_field, _doc_id):
	_res = _es_instance.termvectors(index=_source_index,
	                                doc_type=_my_type,
	                                id=_doc_id,
	                                fields=_search_field,
	                                field_statistics=False,
	                                term_statistics=True,
	                                positions=False,
	                                payloads=False,
	                                offsets=False)

	# pprint.pprint(_res)
	_terms_dict = {}
	if _res["term_vectors"]:
		for _term, _term_detail in _res["term_vectors"][_search_field]["terms"].items():
			_terms_dict[_term] = _term_detail['term_freq']
	return _terms_dict


def load_to_elasticsearch(_es_instance, _my_index, _my_type, _source, _doc_id):
	action = {
		'_index': _my_index,
		'_type': _my_type,
		'_source': _source,
		'_id': _doc_id
	}
	bulk(_es_instance, [action])


if __name__ == "__main__":
	settings.init()

	es = Elasticsearch()
	target_index = "hw7_dataset"
	test_index = "hw7_test"
	train_type = "for_train"
	test_type = "for_test"
	create_dataset(es, test_index)

	# url_id1 = 'hhh1'
	# url_id2 = 'hhh2'
	# url_id3 = 'hhh3'
	#
	# source1 = {"subject": "subject",
	#            "text": "free spam click buy spam click buy",
	#            "spam": 1,
	#            "features": dict.fromkeys(["from", "to", "weird_char", "weird_addr",
	#                                       "weird_sbj", "weird_target",
	#                                       "weird_content", "weird_msg_id",
	#                                       "servers_count", "span_time"], 0)}
	#
	# source2 = {"subject": "subject",
	#            "text": "free",
	#            "spam": 1,
	#            "features": dict.fromkeys(["from", "to", "weird_char", "weird_addr", "weird_sbj", "weird_target",
	#                                       "weird_content", "weird_msg_id",
	#                                       "servers_count", "span_time"], 0)}
	#
	# source3 = {"subject": "subject",
	#            "text": "spam click buy spam click buy",
	#            "spam": 1,
	#            "features": dict.fromkeys(["from", "to", "weird_char", "weird_addr", "weird_sbj", "weird_target",
	#                                       "weird_content", "weird_msg_id",
	#                                       "servers_count", "span_time"], 0)}





	# load_to_elasticsearch(es, test_index, train_type, source1, url_id1)
	# load_to_elasticsearch(es, test_index, train_type, source2, url_id2)
	# load_to_elasticsearch(es, test_index, train_type, source3, url_id3)

	for doc_id, tf in term_freq(es, test_index, train_type, 10000, "free spam click", "text"):
		print(doc_id, tf)
