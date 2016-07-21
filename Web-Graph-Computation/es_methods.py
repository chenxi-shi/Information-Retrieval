import pprint
from elasticsearch import Elasticsearch, TransportError
from elasticsearch.helpers import bulk
import networkx as nx


def doc_unique_term_length(_es_instance, docno, _my_index, _my_type):
	body = {
		'query': {
			'match': {'_id': docno}
		},
		'aggs': {
			'count': {
				'stats': {
					'script': 'doc["text"].values.size()'
				}
			}
		}
	}
	res = _es_instance.search(index=_my_index,
							  doc_type=_my_type,
							  # size=6000,  # Here is RIGHT! TODO: change size to 1000
							  request_timeout=20,
							  body=body)
	pprint.pprint(res)
	return  # int(res['aggregations']['count']['avg'])

def doc_freq_AND_term_freq(_es_instance, _my_index, _my_type, term, _doc_freq=False, _corpus_size=10000):
	body = {
		"query": {
			"filtered": {
				"query": {
					"bool": {
						"must": {"match": {"text": term}}
					}
				}
			}
		},
		'script_fields': {
			'tf': {
				'script': {
					'inline': '_index[field][term].tf()',
					'params': {
						'field': 'text',
						'term': term
					}
				}
			},
			'df': {
				'script': {
					'inline': '_index[field][term].df()',
					'params': {
						'field': 'text',
						'term': term
					}
				}
			}
		},
		'size': _corpus_size,
		'fields': []
	}
	res = _es_instance.search(index=_my_index,
	                          doc_type=_my_type,
	                          request_timeout=40,
	                          body=body)
	# pprint.pprint(res)
	if res['hits']['hits']:
		if not _doc_freq:   # using len(res['hits']['hits']) also a good idea
			_doc_freq = res['hits']['hits'][0]['fields']['df'][0]
		for _doc in res['hits']['hits']:
			_id = _doc['_id']
			_term_freq = _doc['fields']['tf'][0]
			yield _id, _doc_freq, _term_freq


def doc_length(_doc_id, es_instance, _source_index, _my_type):
	res = es_instance.termvectors(index=_source_index,
								  doc_type=_my_type,
								  id=_doc_id,
								  fields='text',
								  field_statistics=True,
								  term_statistics=True,
								  positions=False,
								  payloads=False,
								  offsets=False)
	es_instance.indices.refresh(index=_source_index)
	_doc_length = sum(_term_detail['term_freq'] for _term_detail in res['term_vectors']['text']['terms'].values())
	return _doc_length


def doc_length2(_doc_list, es_instance, _source_index, _my_type):
	res = es_instance.mtermvectors(index=_source_index,
								   doc_type=_my_type,
								   ids=_doc_list,
								   fields='text',
								   field_statistics=True,
								   term_statistics=True,
								   positions=False,
								   payloads=False,
								   offsets=False)
	es_instance.indices.refresh(index=_source_index)
	return res


def avg_doc_len_AND_doc_count(es_instance, my_index, my_type, d_id_example='http://maritimeaccident.org/'):
	res = es_instance.termvectors(index=my_index,
								  doc_type=my_type,
								  id=d_id_example,
								  fields='text',
								  field_statistics=True,
								  term_statistics=True,
								  positions=False,
								  payloads=False,
								  offsets=False)
	doc_count = res['term_vectors']['text']['field_statistics']['doc_count']
	sum_ttf = res['term_vectors']['text']['field_statistics']['sum_ttf']
	return int(sum_ttf / doc_count), doc_count


if __name__ == '__main__':
	es = Elasticsearch()
	source_index = 'maritimeaccidents'
	my_type = 'document'

	url_id = 'http://en.wikipedia.org/wiki/Strand_jack'
	url_id2 = 'http://en.wikipedia.org/wiki/Belfast_Lough'

	url_id3 = 'hhh'
	url_id4 = 'hhh2'
	url_id5 = 'hhh3'


	source3 = {"docno": url_id3,
	          "HTTPheader": "HTTPheader",
	          "title": "title",
	          "text": "algorithm, Strand jack - algorithms, chenxi Wikipedia, the free encyclopedia Strand jack From Wikipedia chenxi",
	          "html_Source": "html_Source",
	          "in_links": ["123"],
	          "out_links": ["456"],
	          "author": "Chenxi",
	          "depth": 0,
	          "url": url_id3}

	source4 = {"docno": url_id4,
	          "HTTPheader": "HTTPheader",
	          "title": "title",
	          "text": "algorithms, Strand jack - chenxi Wikipedia, the free Strand jack From Wikipedia chenxi",
	          "html_Source": "html_Source",
	          "in_links": ["123"],
	          "out_links": ["456"],
	          "author": "Chenxi",
	          "depth": 0,
	          "url": url_id4}

	source5 = {"docno": url_id5,
	          "HTTPheader": "HTTPheader",
	          "title": "title",
	          "text": "Strand jack - chenxi Wikipedia, the free Strand jack From Wikipedia chenxi",
	          "html_Source": "html_Source",
	          "in_links": ["123"],
	          "out_links": ["456"],
	          "author": "Chenxi",
	          "depth": 0,
	          "url": url_id5}


	def load_to_elasticsearch(_es_instance, _my_index, _my_type, _source, _docno):
		action = {
			'_index': _my_index,
			'_type': _my_type,
			'_source': _source,
			'_id': _docno
		}
		bulk(_es_instance, [action])


	load_to_elasticsearch(es, 'test', my_type, source3, url_id3)
	load_to_elasticsearch(es, 'test', my_type, source4, url_id4)
	load_to_elasticsearch(es, 'test', my_type, source5, url_id5)


	# def update_doc(_es_instance, _my_index, _my_type, _docno, change_dict):
	# 	action = {
	# 		'_op_type': 'update',
	# 		'_index': _my_index,
	# 		'_type': _my_type,
	# 		'_id': _docno,
	# 		'doc': change_dict
	# 	}
	# 	try:
	# 		bulk(_es_instance, [action])
	# 	except TransportError as e:
	# 		print(e)
	# 	except:
	# 		pass


	# update_doc(es, my_index, my_type, url_id3, source)
	print(doc_length('hhh', es, 'test', my_type))
	print(avg_doc_len_AND_doc_count('hhh', es, 'test', my_type))
	# pprint.pprint(doc_freq(es, 'test', my_type, 'algorithm'))
	# TODO: term should be lowercase and stem
	for _df, _tf in doc_freq_AND_term_freq(es, 'test', my_type, 'algorithm'):
		print(_df, _tf)
	# print(doc_length(es, url_id, my_index, my_type))
	# print(doc_length(es, url_id2, my_index, my_type))
