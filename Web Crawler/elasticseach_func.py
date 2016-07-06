import pprint
import sys

import requests
from elasticsearch import Elasticsearch, TransportError
from elasticsearch.helpers import bulk, scan

# we will use user on several places
doc_mappings = {
	'document': {
		'properties': {
			"docno": {
				"type": "string",
				"store": True,
				"index": "analyzed",
				"term_vector": "with_positions_offsets_payloads"
			},
			"HTTPheader": {
				"type": "string",
				"store": True,
				"index": "not_analyzed"
			},
			"title": {
				"type": "string",
				"store": True,
				"index": "analyzed",
				"term_vector": "with_positions_offsets_payloads"
			},
			"text": {
				"type": "string",
				"store": True,
				"index": "analyzed",
				"term_vector": "with_positions_offsets_payloads"
			},
			"html_Source": {
				"type": "string",
				"store": True,
				"index": "no"
			},
			"in_links": {
				"type": "string",
				"store": True,
				"index": "no"
			},
			"out_links": {
				"type": "string",
				"store": True,
				"index": "no"
			},
			"author": {
				"type": "string",
				"store": True,
				"index": "analyzed"
			},
			"depth": {
				"type": "integer",
				"store": True,
				"index": "not_analyzed"
			},
			"url": {
				"type": "string",
				"store": True,
				"index": "not_analyzed"
			}
		}
	}
}

medium_mappings = {
	'document': {
		'properties': {
			"docno": {
				"type": "string",
				"store": True,
				"index": "analyzed",
				"term_vector": "with_positions_offsets_payloads"
			},
			"HTTPheader": {
				"type": "string",
				"store": True,
				"index": "not_analyzed"
			},
			"title": {
				"type": "string",
				"store": True,
				"index": "analyzed",
				"term_vector": "with_positions_offsets_payloads"
			},
			"text": {
				"type": "string",
				"store": True,
				"index": "analyzed",
				"term_vector": "with_positions_offsets_payloads"
			},
			"html_Source": {
				"type": "string",
				"store": True,
				"index": "no"
			},
			"author": {
				"type": "string",
				"store": True,
				"index": "analyzed"
			},
			"depth": {
				"type": "integer",
				"store": True,
				"index": "not_analyzed"
			},
			"url": {
				"type": "string",
				"store": True,
				"index": "not_analyzed"
			},
			"href": {
				"type": "string",
				"store": True,
				"index": "no"
			},
			"score": {
				"type": "integer",
				"store": True,
				"index": "not_analyzed"
			},
			"doc_len": {
				"type": "integer",
				"store": True,
				"index": "not_analyzed"
			}
		}
	}
}


def create_dataset(_es_instance, _my_index, _mappings):
	index_settings = {
		'index': {
			'store': {
				'type': 'default'
			},
			'number_of_shards': 1,
			'number_of_replicas': 0
		}
	}

	try:  # Check status of ES server
		requests.get('http://localhost:9200')
	except:
		print('Elasticsearch service has not be stared, auto-exit now.')
		exit()

	# create empty index
	if _es_instance.indices.exists(index=_my_index):
		print('Index {} exists. Delete and rebuild it.'.format(_my_index))
		_es_instance.indices.delete(index=_my_index, ignore=[400, 404])
	try:
		_es_instance.indices.create(
			index=_my_index,
			body={'settings': index_settings, 'mappings': _mappings},
			ignore=400
		)
	except:
		e = sys.exc_info()
		pprint.pprint('<p>Error: {}</p>'.format(e))


'''
_source:
{
'HTTPheader': HTTPheader,
'title': title,
'text': text,
'html_Source': html_Source,
'in_links': in_links,
'out_links': out_links,
'author': Chenxi,
'depth': depth,
'url': url
}
'''


def load_to_elasticsearch(_es_instance, _my_index, _my_type, _source, _docno):
	action = {
		'_index': _my_index,
		'_type': _my_type,
		'_source': _source,
		'_id': _docno
	}
	bulk(_es_instance, [action])


def remove_doc(_es_instance, _my_index, _my_type, _docno):
	action = {
		'_op_type': 'delete',
		'_index': _my_index,
		'_type': _my_type,
		'_id': _docno,
	}
	try:
		bulk(_es_instance, [action])
	except TransportError as e:
		print(e)
	except:
		pass


def update_doc(_es_instance, _my_index, _my_type, _docno, change_dict):
	action = {
		'_op_type': 'update',
		'_index': _my_index,
		'_type': _my_type,
		'_id': _docno,
		'doc': change_dict
	}
	try:
		bulk(_es_instance, [action])
	except TransportError as e:
		print(e)
	except:
		pass

def merge(_es_instance, _my_index, _target_index, _my_type, _docno, change_dict):
	for _m in scan(_es_instance, index=_my_index, doc_type=_my_type,
	               query={"query": {"match_all": {}}}):
		_target = _es_instance.search(index=_target_index,
		                           doc_type='document',
		                           body={"query": {"match": {"_id": _m['_source']}}},
		                           request_timeout=30)
		if _target['hits']['hits']:
			_target_rec = _target['hits']['hits'][0]
			c = {"docno": _m['_source']['docno'],
			     "HTTPheader": _m['_source']['HTTPheader'],
			     "title": _m['_source']['title'],
			     "text": _m['_source']['text'],
			     "html_Source": _m['_source']['html_Source'],
			     "in_links": _m['_source']['in_links'] + _target_rec['_source']['in_links'],
			     "out_links": _m['_source']['out_links'],
			     "author": _m['_source']['author'] + _target_rec['_source']['author'],
			     "depth": _m['_source']['depth'],
			     "url": _m['_source']['url']}
			update_doc(_es_instance, _target_index, _my_type, _m['_id'], c)
		else:
			load_to_elasticsearch(_es_instance, _target_index, _my_type, _m['_source'], _m['_id'])


if __name__ == '__main__':
	es = Elasticsearch()
	my_index = 'maritimeaccidents'
	my_type = 'document'

	_source = {'docno': 1,
	           'HTTPheader': '2',
	           'title': '3',
	           'text': '4',
	           # 'html_Source': html_Source,
	           'in_links': '\n'.join(Node.graph[node_instance.normed_url]['in_links']),
	           'out_links': '\n'.join(Node.graph[node_instance.normed_url]['out_links']),
	           'author': 'Chenxi',
	           'depth': _deep,
	           'url': node_instance.normed_url
	           }

	create_dataset(es, my_index)
	load_files(es, my_index, my_type, )

	es = Elasticsearch()
	my_index = 'wave_medium'
	my_type = 'document'
	c = {"title": "hhhh", 'html_Source': 'wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww'}
	update_doc(es, my_index, my_type, "http://m.guardian.co.uk", c)
	es.get(index=my_index, doc_type=my_type, id="http://m.guardian.co.uk")
