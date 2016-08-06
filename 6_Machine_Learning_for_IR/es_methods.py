import pprint
from re import compile
from collections import defaultdict

from elasticsearch import Elasticsearch, TransportError
from elasticsearch.helpers import bulk, scan


def doc_unique_term_length(_es_instance, docno, _my_index, _my_type):
	body = {
		"query": {
			"match": {"_id": docno}
		},
		"aggs": {
			"count": {
				"stats": {
					"script": "doc[\"text\"].values.size()"
				}
			}
		}
	}
	res = _es_instance.search(index=_my_index,
	                          doc_type=_my_type,
	                          # size=6000,  # Here is RIGHT! TODO: change size to 1000
	                          request_timeout=40,
	                          body=body)
	pprint.pprint(res)
	return  # int(res["aggregations"]["count"]["avg"])

def text_unique_term_count(_es_instance, _my_index, _my_type, _doc_id=False):
	body = {
		"aggs": {
			"unique_terms": {
				"cardinality": {
					"script": "doc[\"text\"].values"
				}
			}
		}
	}
	if _doc_id:
		body["query"] = {"match": {"_id": _doc_id}}
	res = _es_instance.search(index=_my_index,
	                          doc_type=_my_type,
	                          # size=6000,  # Here is RIGHT! TODO: change size to 1000
	                          request_timeout=40,
	                          body=body)
	# pprint.pprint(res)
	return  int(res["aggregations"]["unique_terms"]["value"])

def title_unique_term_count(_es_instance, _my_index, _my_type, _doc_id=False):
	body = {
		"aggs": {
			"unique_terms": {
				"cardinality": {
					"script": "doc[\"head\"].values"
				}
			}
		}
	}
	if _doc_id:
		body["query"] = {"match": {"_id": _doc_id}}
	res = _es_instance.search(index=_my_index,
	                          doc_type=_my_type,
	                          # size=6000,  # Here is RIGHT! TODO: change size to 1000
	                          request_timeout=40,
	                          body=body)
	# pprint.pprint(res)
	return  int(res["aggregations"]["unique_terms"]["value"])


def doc_freq_AND_term_freq_without_english_stemmer(_es_instance, _my_index, _my_type, term, _doc_freq=False, _corpus_size=10000):
	'''
	When using english stemmer, script will not work for tf, df
	:param _es_instance:
	:param _my_index:
	:param _my_type:
	:param term:
	:param _doc_freq:
	:param _corpus_size:
	:return:
	'''
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
		"script_fields": {
			"tf": {
				"script": {
					"inline": "_index[field][term].tf()",
					"params": {
						"field": "text",
						"term": term
					}
				}
			},
			"df": {
				"script": {
					"inline": "_index[field][term].df()",
					"params": {
						"field": "text",
						"term": term
					}
				}
			}
		},
		"size": _corpus_size,
		"fields": []
	}
	res = _es_instance.search(index=_my_index,
	                          doc_type=_my_type,
	                          request_timeout=40,
	                          body=body)
	# pprint.pprint(res)
	# print(len(res["hits"]["hits"]))
	if res["hits"]["hits"]:
		if not _doc_freq:  # using len(res["hits"]["hits"]) also a good idea
			_doc_freq = res["hits"]["hits"][0]["fields"]["df"][0]
		for _doc in res["hits"]["hits"]:
			_id = _doc["_id"]
			_term_freq = _doc["fields"]["tf"][0]
			yield _id, _doc_freq, _term_freq


def doc_freq_AND_term_freq(_es_instance, _my_index, _my_type, _type_size, _term, _search_field):

	tf_pattern = compile(r"termFreq=.+")
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
		return _df, _tf_dict
	else:
		return 0, _tf_dict




def doc_length(es_instance, _source_index, _my_type, _doc_id):
	res = es_instance.termvectors(index=_source_index,
	                              doc_type=_my_type,
	                              id=_doc_id,
	                              fields="text",
	                              field_statistics=True,
	                              term_statistics=True,
	                              positions=False,
	                              payloads=False,
	                              offsets=False)

	# pprint.pprint(res)
	_doc_length = sum(_term_detail["term_freq"] for _term_detail in res["term_vectors"]["text"]["terms"].values())
	return _doc_length


def doc_length2(_doc_list, es_instance, _source_index, _my_type):
	res = es_instance.mtermvectors(index=_source_index,
	                               doc_type=_my_type,
	                               ids=_doc_list,
	                               fields="text",
	                               field_statistics=True,
	                               term_statistics=True,
	                               positions=False,
	                               payloads=False,
	                               offsets=False)
	es_instance.indices.refresh(index=_source_index)
	return res


def avg_doc_len_AND_doc_count(_es_instance, _my_index, _my_type, d_id_example="http://maritimeaccident.org/"):
	res = _es_instance.termvectors(index=_my_index,
	                               doc_type=_my_type,
	                               id=d_id_example,
	                               fields="text",
	                               field_statistics=True,
	                               term_statistics=True,
	                               positions=False,
	                               payloads=False,
	                               offsets=False)
	doc_count = res["term_vectors"]["text"]["field_statistics"]["doc_count"]
	sum_ttf = res["term_vectors"]["text"]["field_statistics"]["sum_ttf"]
	return int(sum_ttf / doc_count), doc_count


def total_num_docs(_es_instance, _my_index, _my_type):
	body = {
		"query": {
			"match_all": {}
		},
		"fields": []
	}
	_res = _es_instance.search(index=_my_index,
	                          doc_type=_my_type,
	                          request_timeout=40,
	                          body=body)
	return _res["hits"]["total"]


def search_doc(_es_instance, _my_index, _my_type, _match_dict, _fields_list=False, _source_list=False):
	body = {
		"query": {
			"match": _match_dict
		}
	}

	if _fields_list:
		body.update({"fields": _fields_list})

	if _source_list:   # used for non-leaf nodes (i.e nodes that have children)
		body.update({"_source": _source_list})

	_res = _es_instance.search(index=_my_index,
	                          doc_type=_my_type,
	                          request_timeout=40,
	                          body=body)
	if _res["hits"]["total"] > 0:
		return _res["hits"]["hits"][0]
	else:
		print("{} does not exit".format(_match_dict))
		exit(-1)

def insert_doc(_es_instance, _target_index, _my_type, _doc_detail, _docno):
	action = {
		"_index": _target_index,
		"_type": _my_type,
		"_source": _doc_detail,
		"_id": _docno
	}
	bulk(_es_instance, [action])


def update_doc(_es_instance, _target_index, _my_type, _docno, _change_doc=None, _change_doc_as_upsert=False, _change_script=None, _change_params=None, _change_upsert=None):
	action = {
		"_op_type": "update",
		"_index": _target_index,
		"_type": _my_type,
		"_retry_on_conflict": 3,
		"_id": _docno
	}
	# TODO: make sure "doc": _change_dict
	if _change_doc:
		action["doc"] = _change_doc  # not apply when field is nested object
	if _change_doc_as_upsert:
		action["doc_as_upsert"] = True  # when set this true, no exception will be threw when id is not exist
	if _change_script:
		action["script"] = _change_script  # apply when field is nested object
	if _change_params:
		action["params"] = _change_params  # apply when field is nested object
	if _change_upsert:
		action["upsert"] = _change_upsert
	try:
		bulk(_es_instance, [action])
	except TransportError as e:
		print(e)
	except:
		pass

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

def generate_all_doc(_es_instance, _my_index, _my_type="_all"):
	if _my_type == "_all":
		for _doc in scan(_es_instance, index=_my_index,
		                 query={"query": {"match_all": {}}}):
			yield _doc
	else:
		for _doc in scan(_es_instance, index=_my_index, doc_type=_my_type,
		               query={"query": {"match_all": {}}}):
			yield _doc
	# for x in generate_all_doc(es, "hw6_ap_dataset", "56_doc"):
	# 	print(x)

# change_script = "if( ctx._source.features.containsKey(\"okapi_tf\") ) {{" \
#                 "ctx._source.features.okapi_tf += {0};" \
#                 "}} else {{" \
#                 "	ctx._source.features.okapi_tf = {0};" \
#                 "}};" \
#                 "if( ctx._source.features.containsKey(\"tf_idf\") ) {{" \
#                 "ctx._source.features.tf_idf += {1};" \
#                 "}} else {{" \
#                 "	ctx._source.features.tf_idf = {1};" \
#                 "}};" \
#                 "if( ctx._source.features.containsKey(\"bm25\") ) {{" \
#                 "ctx._source.features.bm25 += {2};" \
#                 "}} else {{" \
#                 "	ctx._source.features.bm25 = {2};" \
#                 "}}".format(111, 222, 333)
# change_script = "if( ctx._source.features.containsKey(\"okapi_tf\") ){ ctx._source.features.okapi_tf += 123; } else { ctx._source.features.okapi_tf = 123; };if( ctx._source.features.containsKey(\"tf_idf\") ){ ctx._source.features.tf_idf += 123; } else { ctx._source.features.tf_idf = 123; };if( ctx._source.features.containsKey(\"bm25\") ){ ctx._source.features.bm25 += 123; } else { ctx._source.features.bm25 = 123; }"
# update_doc(es, target_index, "64_doc", "AP890711-0075", _change_script=change_script)

if __name__ == "__main__":
	es = Elasticsearch()
	source_index = "maritimeaccidents"
	my_type = "document"


	# url_id = 'http://en.wikipedia.org/wiki/Strand_jack'
	# url_id2 = 'http://en.wikipedia.org/wiki/Belfast_Lough'
	#
	# url_id3 = 'hhh'
	# url_id4 = 'hhh2'
	# url_id5 = 'hhh3'
	#
	# source3 = {"docno": url_id3,
	#            "HTTPheader": "HTTPheader",
	#            "title": "title",
	#            "text": "algorithm, Strand jack - algorithms, chenxi Wikipedia, the free encyclopedia Strand jack From Wikipedia chenxi",
	#            "html_Source": "html_Source",
	#            "in_links": ["123"],
	#            "out_links": ["456"],
	#            "author": "Chenxi",
	#            "depth": 0,
	#            "url": url_id3}
	#
	# source4 = {"docno": url_id4,
	#            "HTTPheader": "HTTPheader",
	#            "title": "title",
	#            "text": "algorithms, Strand jack - chenxi Wikipedia, the free Strand jack From Wikipedia chenxi",
	#            "html_Source": "html_Source",
	#            "in_links": ["123"],
	#            "out_links": ["456"],
	#            "author": "Chenxi",
	#            "depth": 0,
	#            "url": url_id4}
	#
	# source5 = {"docno": url_id5,
	#            "HTTPheader": "HTTPheader",
	#            "title": "title",
	#            "text": "Strand jack - chenxi Wikipedia, the free Strand jack From Wikipedia chenxi",
	#            "html_Source": "html_Source",
	#            "in_links": ["123"],
	#            "out_links": ["456"],
	#            "author": "Chenxi",
	#            "depth": 0,
	#            "url": url_id5}


	def load_to_elasticsearch(_es_instance, _my_index, _my_type, _source, _docno):
		action = {
			'_index': _my_index,
			'_type': _my_type,
			'_source': _source,
			'_id': _docno
		}
		bulk(_es_instance, [action])


	#
	# load_to_elasticsearch(es, 'test', my_type, source3, url_id3)
	# load_to_elasticsearch(es, 'test', my_type, source4, url_id4)
	# load_to_elasticsearch(es, 'test', my_type, source5, url_id5)

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
