from elasticsearch import Elasticsearch
from elasticseach_func import *

source_es = Elasticsearch(["http://104.196.4.140:9200"])  # google cloud engine
source_index = 'maritimeaccidents'
target_es = Elasticsearch()  # localhost
target_index = 'chenxi_maritimeaccidents'

my_type = 'document'

create_dataset(target_es, target_index, doc_mappings)

for _m in scan(source_es, index=source_index, doc_type=my_type, query={"query": {"match_all": {}}}):
	load_to_elasticsearch(target_es, target_index, my_type, _m['_source'], _m['_id'])