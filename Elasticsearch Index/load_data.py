#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import print_function

import os
import re
import sys
import time
from bs4 import BeautifulSoup
from os.path import dirname, abspath
import requests
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import pprint


def create_dataset(es_instance, my_index, settings, mappings):
    try:
        es_instance.indices.create(
            index=my_index,
            body={'settings': settings, 'mappings': mappings},
            ignore=400
        )
    except:
        e = sys.exc_info()
        pprint.pprint('<p>Error: {}</p>'.format(e))


def parse_doc(doc_path):
    for one_file in os.listdir(doc_path):
        one_file = open('{}/{}'.format(doc_path, one_file), 'r', encoding='utf-8', errors='ignore').read()
        soup = BeautifulSoup(one_file, 'html.parser')
        for doc in soup.find_all('doc'):
            docno = ''.join(doc.docno.string.split())
            text = ''.join((txt.string.replace('\n', ' ') for txt in doc.find_all('text')))
            yield docno, text


def load_docs(es_instance, my_index, my_type, doc_path):
    action = ({
        '_index': my_index,
        '_type': my_type,
        '_source': {'docno': docno, 'text': text},
        '_id': docno
    } for docno, text in parse_doc(doc_path))
    helpers.bulk(es_instance, action)
    # helpers.parallel_bulk(client=es_instance, actions=action, thread_count=4)


def parse_query(resource_path):
    with open('{}/query_desc.51-100.short.txt'.format(resource_path), 'r', errors='ignore') as queries:
        for line in queries:
            if line is not '\n':  # remove the \n at the end of each line
                label, query = re.split(pattern='. +', maxsplit=1, string=line)
                label = int(label)
                # filter empty space, first 4 words, and duplication
                query = re.split(' ', maxsplit=3, string=query)[-1]
                query = re.sub('(ing|ed|al)$', '', query)
                # lowercase every letter
                query = query.lower()
                yield label, query


def load_query(es_instance, my_index, my_type, resource_path):
    action = ({
        '_index': my_index,
        '_type': my_type,
        '_source': {'queryno': queryno, 'sentence': query},
        '_id': queryno
    } for queryno, query in parse_query(resource_path))
    # helpers.parallel_bulk(client=es_instance, actions=action, thread_count=4)
    helpers.bulk(es_instance, action)
    # TODO: para_bulk


def whole_prep_dataset(es_instance, my_index, doc_type, query_type):
    try:  # Check status of ES server
        requests.get('http://localhost:9200')
    except:
        print('Elasticsearch service has not be stared, auto-exit now.')
        exit()
    # path is the parent dir of __file__'s location
    path = dirname(dirname(abspath(__file__)))
    resource_path = '{}/AP_DATA'.format(path)
    doc_path = '{}/ap89_collection'.format(resource_path)

    # create empty index,
    if es_instance.indices.exists(index=my_index):
        print('Index {} exists. removing it...'.format(my_index))
        es_instance.indices.delete(index=my_index)

    settings = {
        'index': {
            'store': {
                'type': 'default'
            },
            'max_result_window': 85000,
            'number_of_shards': 1,
            'number_of_replicas': 0
        },
        'analysis': {
            'analyzer': {
                'articles': {
                    'type': 'english',
                    'stopwords_path': 'stoplist.txt'
                }
            }
        }
    }
    doc_mappings = {
        'properties': {
            'docno': {
                'type': 'string',
                'store': True,
                'index': 'not_analyzed'
            },
            'text': {
                'type': 'string',
                'store': True,
                'index': 'analyzed',
                'term_vector': 'with_positions_offsets_payloads',
                'analyzer': 'articles'
            }
        }
    }
    query_mappings = {
        'properties': {
            'queryno': {
                'type': 'short',
                'store': True,
                'index': 'not_analyzed'
            },
            'sentence': {
                'type': 'string',
                'store': True,
                'index': 'analyzed',
                'term_vector': 'with_positions_offsets_payloads',
                'analyzer': 'articles'
            }
        }
    }
    mappings = {
        'document': doc_mappings,
        'query': query_mappings
    }
    print('Creating index {}...'.format(my_index))
    create_dataset(es_instance, my_index, settings, mappings)
    print('Loading documents...')
    load_docs(es_instance, my_index, doc_type, doc_path)
    load_query(es_instance, my_index, query_type, resource_path)
    print('Dataset is all set.')


if __name__ == '__main__':
    start_time = time.time()
    es = Elasticsearch()
    ap_index = 'ap_dataset'
    doc_type = 'document'
    query_type = 'query'

    whole_prep_dataset(es, ap_index, doc_type, query_type)
    print("--- {} seconds ---".format(time.time() - start_time))


