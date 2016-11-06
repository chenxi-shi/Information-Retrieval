#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
load_data.py includes the functions, that load docs into es server.
The details of docs only includes text and title.
"""
from __future__ import print_function

import os.path
import sys
import re
from os.path import dirname, abspath
import time

from bs4 import BeautifulSoup
import requests
from elasticsearch import Elasticsearch, ElasticsearchException
import pprint

from es_methods import insert_doc, search_doc


def create_setting_mapping(es_instance, _target_index, stplst_path="stoplist.txt"):
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
					"tokenizer": "standard",
					"filter": ["standard", "lowercase", "my_stemmer"]
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

	doc_mappings = {
		"document": {
			"properties": {
				"docno": {
					"type": "string",
					"store": True,
					"index": "not_analyzed"
				},
				"head": {
					"type": "string",
					"store": True,
					"index": "analyzed",
					"term_vector": "with_positions_offsets_payloads",
					"analyzer": "english_text"
				},
				"text": {
					"type": "string",
					"store": True,
					"index": "analyzed",
					"term_vector": "with_positions_offsets_payloads",
					"analyzer": "english_text"
				},
				"queries": {
					"type": "integer",
					"store": True,
					"index": "no"
				},
			}
		}
	}

	# create empty index
	if es_instance.indices.exists(index=_target_index):
		print("Index {} exists. Delete and rebuild it.".format(_target_index))
		es_instance.indices.delete(index=_target_index)
	try:
		es_instance.indices.create(
			index=_target_index,
			body={"settings": index_settings, "mappings": doc_mappings},
			ignore=[400, 404]
		)
	except ElasticsearchException as e:
		e2 = sys.exc_info()
		print(e)
		pprint.pprint("<p>Error: {}</p>".format(e2))


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
					"tokenizer": "standard",
					"filter": ["standard", "lowercase", "my_stemmer"]
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


def put_my_mapping(es_instance, _target_index, query_id):
	"""
	put_mapping only used to create query mapping
	:param query_id:
	:param es_instance:
	:param _target_index:
	:return:
	"""
	doc_mappings = {
		"{}_doc".format(query_id): {
			"properties": {
				"docno": {
					"type": "string",
					"store": True,
					"index": "not_analyzed"
				},
				"head": {
					"type": "string",
					"store": True,
					"index": "analyzed",
					"term_vector": "with_positions_offsets_payloads",
					"analyzer": "english_text"
				},
				"text": {
					"type": "string",
					"store": True,
					"index": "analyzed",
					"term_vector": "with_positions_offsets_payloads",
					"analyzer": "english_text"
				},
				"query_label": {
					"type": "float",
					"store": True
				},
				"features": {
					# "type": "object",  # cannot use nested, when insert with {}, there would be some error
					"dynamic": True,  # object is not support store: True
					"properties": {
						"doc_len": {"type": "integer"},
						"okapi_tf": {"type": "float"},
						"tf_idf": {"type": "float"},
						"bm25": {"type": "float"},
						"laplace": {"type": "float"},
						"jm": {"type": "float"},
						"text_term_appeared_in_query_len": {"type": "float"},
						"title_term_appeared_in_query_len": {"type": "float"},
						"text_unique_term": {"type": "integer"},
						"title_unique_term": {"type": "integer"},
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
				doc_type="{}_doc".format(query_id),
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


def put_new_field(es_instance, _target_index, _query_id, field_dict):
	field_mappings = {
		"properties": field_dict
	}
	if es_instance.indices.exists(index=_target_index):
		try:
			es_instance.indices.put_mapping(
				index=_target_index,
				doc_type="{}_doc".format(_query_id),
				body=field_mappings,
				ignore=[400, 404],
				refresh=True
			)
		except ElasticsearchException as e:
			e2 = sys.exc_info()
			print(e)
			pprint.pprint("<p>Error: {}</p>".format(e2))
	else:
		print("the index {} is not exits, exit now.".format(_target_index))
		exit(-1)


def open_parse_doc(_doc_id, _label, docs_path=r"AP_DATA\ap89_collection"):
	_file_name, _doc_name = _doc_id.split("-")
	_doc = open(r"{}\{}".format(docs_path, _file_name), "r", encoding="utf-8", errors="replace").read()
	soup = BeautifulSoup(_doc, "html.parser")
	for _doc in soup.find_all("doc"):
		_docno = "".join(_doc.docno.string.split())
		if _docno == _doc_id:
			_head = "".join((txt.string.replace("\n", " ") for txt in _doc.find_all("head")))
			_text = "".join((txt.string.replace("\n", " ") for txt in _doc.find_all("text")))

			return {"text": _text, "head": _head, "query_label": _label,
			        "features": dict.fromkeys(["doc_len", "okapi_tf", "tf_idf", "bm25", "laplace", "jm",
			                                   "text_term_appeared_in_query_len", "title_term_appeared_in_query_len",
			                                   "text_unique_term", "title_unique_term"], 0)}


def get_index_mapping(es_instance, _target_index):
	if es_instance.indices.exists(index=_target_index):
		try:
			_mapping_detail = es_instance.indices.get_mapping(
				index=_target_index,
				# doc_type="{}_doc".format(query_id),
				ignore=[400, 404]
			)
		except ElasticsearchException as e:
			e2 = sys.exc_info()
			print(e)
			pprint.pprint("<p>Error: {}</p>".format(e2))
		else:
			# {index: {"mappings": {type: {mapping detail}, type: {mapping detail}...}}}
			return _mapping_detail
	else:
		print("the index {} is not exits, exit now.".format(_target_index))
		exit(-1)


def write_doc_details(_es_instance, _source_index, _source_type,  _target_index, _results_dict, doc_path=r"AP_DATA\ap89_collection"):
	'''
	load_files write doc details into es,
	including {"text": _text, "head": _head, "query_label": _label, "features": {}}
	AP890101-0001
	_result_dict = {query_id: {"doc_id": {"label": 1}, "doc_id": {"label": 0}, ...},
				query_id: {"doc_id": {"label": 1}, "doc_id": {"label": 0}, ...}, ...}
	:param _es_instance:
	:param _target_index:
	:param _results_dict:
	:param doc_path:
	:return:
	'''
	_x = 0
	for _query_id, _docs in _results_dict.items():
		put_my_mapping(_es_instance, _target_index, "{}_doc".format(_query_id))
		for _docno, _doc_label in _docs.items():
			_doc_detail = search_doc(_es_instance, _source_index, _source_type, {"_id": _docno}, _fields_list=["title", "text"], _source_list=False)
			if _doc_detail:
				_doc_detail = _doc_detail['fields']
				_doc_detail["text"] = _doc_detail["text"][0]
				_doc_detail["head"] = _doc_detail["title"][0]
				del _doc_detail["title"]
				_doc_detail["query_label"] = float(_doc_label["label"])
				_doc_detail["features"] = dict.fromkeys(["doc_len", "okapi_tf", "tf_idf", "bm25", "laplace", "jm",
				                                   "text_term_appeared_in_query_len", "title_term_appeared_in_query_len",
				                                   "text_unique_term", "title_unique_term"], 0)

				# TODO: change to fit _doc_detail
				# _doc_detail = open_parse_doc(_docno, _doc_detail["label"], doc_path)
				_doc_type = "{}_doc".format(_query_id)
				insert_doc(_es_instance, _target_index, _doc_type, _doc_detail, _docno)
				_x += 1
				print("{} Finished {}".format(_x, _docno))


def whole_prep_dataset(es_instance, _source_index, _source_type, _target_index, _results_dict):
	try:  # Check status of ES server
		requests.get("http://localhost:9200")
	except:
		print("Elasticsearch service has not be started, run it now.")
		import subprocess
		subprocess.check_call(r"C:\Users\Chenxi\Desktop\HW3\DOCs\elasticsearch-2.3.3\bin\elasticsearch",
		                      shell=True)
		print('after subprocess')


	create_setting(es_instance, _target_index)
	write_doc_details(es_instance, _source_index, _source_type, _target_index, _results_dict)


if __name__ == "__main__":
	start_time = time.time()

	es = Elasticsearch()
	target_index = "hw6_ap_dataset"
	doc_type = "document"

	_write_es_flag = True
	if _write_es_flag:
		whole_prep_dataset(es, target_index)

	print("--- {0} seconds ---".format(time.time() - start_time))
