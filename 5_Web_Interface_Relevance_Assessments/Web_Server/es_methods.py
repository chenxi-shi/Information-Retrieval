import pprint
from elasticsearch import Elasticsearch


def get_top_several(_es_instance, _my_index, _my_type, _query, _count=200):
	body = {
		"query": {
			"query_string": {
				"lenient": True,
				"query": _query,
				# TODO: default_field?
				"default_field": "text"
			}
		},
		"highlight": {
			"fields": {
				"text": {
					"type": "fvh",
					"number_of_fragments": 0,
					"pre_tags": ["<mark>"],
					"post_tags": ["</mark>"]
				}
			}
		},
		"fields": [
			"title"
		]
	}

	res = _es_instance.search(index=_my_index,
	                          doc_type=_my_type,
	                          size=_count,
	                          from_=0,
	                          request_timeout=20,
	                          pretty=True,
	                          body=body)
	#pprint.pprint(res)
	return res['took'], res['hits']['hits']  # int(res['aggregations']['count']['avg'])


es = Elasticsearch()
source_index = 'maritimeaccidents'
my_type = 'document'
query1 = "costa concordia disaster and recovery"
query2 = 'South Korea ferry disaster'
query3 = 'Lampedusa migrant shipwreck'

#took, hits = get_top_several(es, source_index, my_type, query2, _count=3)



# with open('search_152601.txt', 'w', errors='replace') as f:
# 	for _hit in hits:
# 		f.write('{}\n'.format(_hit["_id"]))
#
# took, hits = get_top_several(es, source_index, my_type, query2, 200, _fields=['text'])
# with open('search_152602.txt', 'w', errors='replace') as f:
# 	for _hit in hits:
# 		f.write('{}\n'.format(_hit["_id"]))
#
# took, hits = get_top_several(es, source_index, my_type, query3, 200, _fields=['text'])
# with open('search_152603.txt', 'w', errors='replace') as f:
# 	for _hit in hits:
# 		f.write('{}\n'.format(_hit["_id"]))
