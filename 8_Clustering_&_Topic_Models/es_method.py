from elasticsearch import TransportError

from settings import *


def create_hw8_part2_dataset(es_instance, my_index, my_type):
	index_settings = {
		'index': {
			'number_of_shards': 1,
			'number_of_replicas': 0
		}
	}

	doc_mappings = {
		my_type: {
			'properties': {
				"kmean_cluster": {
					'type': 'integer',
					'store': True,
					'index': 'not_analyzed'
				}
			}
		}
	}

	# create empty index
	if es_instance.indices.exists(index=my_index):
		print('Index {} exists. Delete and rebuild it.'.format(my_index))
		es_instance.indices.delete(index=my_index)
	try:
		es_instance.indices.create(
			index=my_index,
			body={'settings': index_settings, 'mappings': doc_mappings},
			# ignore=400
		)
	except ElasticsearchException as e:
		e2 = sys.exc_info()
		print(e)
		pprint.pprint("<p>Error: {}</p>".format(e2))


# TODO: recreate the dataset
def create_dataset(es_instance, my_index, my_type, stplst_path="stoplist.txt"):
	index_settings = {
		'index': {
			'store': {
				'type': 'default'
			},
			'number_of_shards': 1,
			'number_of_replicas': 0
		},
		"analysis": {
			"analyzer": {
				"english_text": {
					"type": "english",
					"stopwords_path": stplst_path,
					"tokenizer": "standard",
					"filter": ["standard",
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

	# we will use user on several places
	doc_mappings = {
		my_type: {
			'properties': {
				'text': {
					'type': 'string',
					'store': True,
					'index': 'analyzed',
					'term_vector': 'with_positions_offsets_payloads',
					'analyzer': 'english_text'
				},
				"doc_len": {
					'type': 'integer',
					'store': True,
					'index': 'not_analyzed'
				}
			}
		}
	}

	# create empty index
	if es_instance.indices.exists(index=my_index):
		print('Index {} exists. Delete and rebuild it.'.format(my_index))
		es_instance.indices.delete(index=my_index)
	try:
		es_instance.indices.create(
			index=my_index,
			body={'settings': index_settings, 'mappings': doc_mappings},
			# ignore=400
		)
	except ElasticsearchException as e:
		e2 = sys.exc_info()
		print(e)
		pprint.pprint("<p>Error: {}</p>".format(e2))


def load_to_elasticsearch(_es_instance, _my_index, _my_type, _source, _doc_id):
	action = {
		'_index': _my_index,
		'_type': _my_type,
		'_source': _source,
		'_id': _doc_id
	}
	bulk(_es_instance, [action])


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
	if "text" not in res["term_vectors"]:
		return 0
	_doc_length = sum(_term_detail["term_freq"] for _term_detail in res["term_vectors"]["text"]["terms"].values())
	return _doc_length


def update_doc(_es_instance, _target_index, _my_type, _docno, _change_doc=None, _change_doc_as_upsert=False,
               _change_script=None, _change_params=None, _change_upsert=None):
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


def doc_freq_AND_term_freq(_es_instance, _my_index, _my_type, _term, _search_field, _type_size=10000):
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


def all_doc_generator(_es_instance, _my_index, _fields, _my_type="_all", _doc_id_flg=True):
	_query = {
		"query": {
			"match_all": {}
		},
		"fields": _fields
	}
	if _doc_id_flg:
		_doc_id_set = set()
		if _my_type == "_all":
			for _doc in scan(_es_instance,
			                 index=_my_index,
			                 query=_query):
				_doc_id_set.add(_doc["_id"])
		else:
			for _doc in scan(_es_instance,
			                 index=_my_index,
			                 doc_type=_my_type,
			                 query=_query):
				_doc_id_set.add(_doc["_id"])
		return _doc_id_set
	else:
		_doc_detail_dict = defaultdict(dict)
		if _my_type == "_all":
			for _doc in scan(_es_instance,
			                 index=_my_index,
			                 query=_query):
				_doc_detail_dict[_doc["_id"]] = _doc["fields"]
		else:
			for _doc in scan(_es_instance,
			                 index=_my_index,
			                 doc_type=_my_type,
			                 query=_query):
				_doc_detail_dict[_doc["_id"]] = _doc["fields"]
		return _doc_detail_dict


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
	return int(res["aggregations"]["unique_terms"]["value"])


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
		return None
