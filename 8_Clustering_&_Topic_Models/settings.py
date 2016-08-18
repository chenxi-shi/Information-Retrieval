import sys
from os.path import dirname, abspath
from os import listdir
from time import time
from collections import defaultdict
from math import log
from re import compile
import re
from pickle import dump, load
from itertools import combinations

from stemming.porter2 import stem
from elasticsearch import Elasticsearch, ElasticsearchException
from elasticsearch.helpers import bulk, scan
import pprint
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.cluster import KMeans
from elasticsearch import Elasticsearch
from sklearn.decomposition import LatentDirichletAllocation
from bs4 import BeautifulSoup
import requests

import logging

es = Elasticsearch()
doc_index = 'hw8_ap_dataset'
doc_type = 'document'
es_field = ["text"]

# path is the parent dir of __file__'s location
parentpath = dirname(dirname(abspath(__file__)))
resource_path = '{}/AP_DATA'.format(parentpath)
docs_path = '{}/ap89_collection'.format(resource_path)

n_samples = 2000
n_features = 1000
n_topics = 25
n_top_words = 25

min_df = 4
n_topics2 = 200
n_clusters = 25
verbose=1
