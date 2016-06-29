'''
PUT /test/document/_mapping
{
  "document": {
    "properties": {
      "docno": {
        "type": "string",
        "store": true,
        "index": "analyzed",
        "term_vector": "with_positions_offsets_payloads"
      },
      "HTTPheader": {
        "type": "string",
        "store": true,
        "index": "not_analyzed"
      },
      "title":{
        "type": "string",
        "store": true,
        "index": "analyzed",
        "term_vector": "with_positions_offsets_payloads"
      },
      "text": {
        "type": "string",
        "store": true,
        "index": "analyzed",
        "term_vector": "with_positions_offsets_payloads"
      },
      "html_Source": {
        "type":"string",
        "store": true,
        "index": "no"
      },
      "in_links":{
        "type": "string",
        "store": true,
        "index": "no"
      },
      "out_links":{
        "type": "string",
        "store": true,
        "index": "no"
      },
      "author":{
        "type": "string",
        "store": true,
        "index": "analyzed"
      },
      "depth": {
        "type": "integer",
        "store": true,
        "index": "not_analyzed"
      },
      "url": {
        "type": "string",
        "store": true,
        "index": "not_analyzed"
      }
    }
  }
}
'''
import pprint
import sys

import requests
from elasticsearch import Elasticsearch
from elasticsearch import helpers


def create_dataset(_es_instance, _my_index):
	index_settings = {
		'index': {
			'store': {
				'type': 'default'
			},
			'number_of_shards': 3,
			'number_of_replicas': 0
		}
	}

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

	try:  # Check status of ES server
		requests.get('http://localhost:9200')
	except:
		print('Elasticsearch service has not be stared, auto-exit now.')
		exit()

	# create empty index
	if _es_instance.indices.exists(index=_my_index):
		print('Index {} exists. Delete and rebuild it.'.format(_my_index))
		_es_instance.indices.delete(index=_my_index)
	try:
		_es_instance.indices.create(
			index=_my_index,
			body={'settings': index_settings, 'mappings': doc_mappings},
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

def load_files(_es_instance, _my_index, _my_type, _source, _docno):
	action = {
		'_index': _my_index,
		'_type': _my_type,
		'_source': _source,
		'_id': _docno
	}
	helpers.bulk(_es_instance, [action])


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
