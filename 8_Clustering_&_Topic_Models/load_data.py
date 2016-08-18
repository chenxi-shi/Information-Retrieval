#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import print_function

from es_method import create_dataset, load_to_elasticsearch, all_doc_generator, doc_length, update_doc
from settings import *


def parse_doc(_doc_path):
	for one_file in listdir(_doc_path):
		one_file = open('{}/{}'.format(_doc_path, one_file), 'r', encoding='utf-8', errors='ignore').read()
		soup = BeautifulSoup(one_file, 'html.parser')
		for doc in soup.find_all('doc'):
			docno = ''.join(doc.docno.string.strip().split())
			_text = ''.join((txt.string for txt in doc.find_all('text')))
			_text = " ".join(filter(None, _text.strip().split()))
			yield docno, {"text": _text, "doc_len": 0}


def load_text_into_es(_es_instance, _my_index, _my_type, _doc_path):
	try:  # Check status of ES server
		requests.get('http://localhost:9200')
	except:
		print('Elasticsearch service has not be stared, auto-exit now.')
		exit()

	create_dataset(_es_instance, _my_index, _my_type)
	for _doc_id, text_dict in parse_doc(_doc_path):
		load_to_elasticsearch(_es_instance, _my_index, _my_type, text_dict, _doc_id)
		print("Finished {}".format(_doc_id))


def load_doc_len_into_es(_es_instance, _my_index, _my_type):
	_doc_id_set = all_doc_generator(_es_instance, _my_index, [], _my_type=_my_type)
	for _doc_id in _doc_id_set:
		print(_doc_id)
		_doc_len = doc_length(_es_instance, _my_index, _my_type, _doc_id)
		_change = {"doc_len": _doc_len}
		update_doc(_es_instance, _my_index, _my_type, _doc_id, _change_doc=_change)


if __name__ == '__main__':
	load_text_into_es(es, doc_index, doc_type, docs_path)
	load_doc_len_into_es(es, doc_index, doc_type)
