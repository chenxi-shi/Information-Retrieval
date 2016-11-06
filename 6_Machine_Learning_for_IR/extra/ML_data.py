import pprint
import time
from collections import defaultdict
from math import log
import csv

import pandas as pd
from elasticsearch import Elasticsearch
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.svm import SVC

from restore_query_es import get_query_dict
from es_methods import generate_all_doc_list, generate_all_doc
import settings


def get_mean(_doc_lst, _feature):
	_sum = sum(_doc["_source"]["features"][_feature] for _doc in _doc_lst)
	return _sum / len(_doc_lst)


def get_standard_deviation(_doc_lst, _feature, _mean):
	_sum = sum(abs(_doc["_source"]["features"][_feature] - _mean) for _doc in _doc_lst)
	return _sum / len(_doc_lst)


def write_train_test_files(_query_id_lst, _norm_feature):
	with open('training_features.csv', 'w', newline='') as csvfile:
		spamwriter = csv.writer(csvfile)  # , quotechar='|', quoting=csv.QUOTE_MINIMAL)
		spamwriter.writerow(["index", "query_id", "doc_id",
		                     "okapi_tf", "tf_idf", "bm25", "laplace", "jm",
		                     "title_unique_term", "text_unique_term",
		                     "text_term_appeared_in_query_len", "title_term_appeared_in_query_len",
		                     "doc_len",
		                     "label"])
		_count = 0
		for _query_id in _query_id_lst[:2]:
			_mean_dict = defaultdict(dict)
			_doc_type = "{}_doc".format(_query_id)
			_doc_list = generate_all_doc_list(settings.es, settings.target_index, _my_type=_doc_type)
			print('doc_list {}'.format(len(_doc_list)))
			if not _doc_list:
				print(_f)
				print(_doc_type)
				print(_query_id)
				exit(-1)
			for _f in _norm_feature:
				_mean_dict[_f]['mean'] = get_mean(_doc_list, _f)

			for _f in _norm_feature:
				_mean_dict[_f]['std_dev'] = get_standard_deviation(_doc_list, _f, _mean_dict[_f]['mean'])

			for _doc in _doc_list:
				_count += 1
				_features = _doc["_source"]["features"]
				# if _doc["_source"]["query_label"] not in settings.label_dict:
				# 	if _doc["_source"]["query_label"] == 0.0:
				# 		settings.label_dict[_doc["_source"]["query_label"]] = 0
				# 	elif _doc["_source"]["query_label"] < 0.6:
				# 		settings.label_dict[_doc["_source"]["query_label"]] = 1
				# 	elif _doc["_source"]["query_label"] < 1:
				# 		settings.label_dict[_doc["_source"]["query_label"]] = 2
				# 	elif _doc["_source"]["query_label"] < 1.3:
				# 		settings.label_dict[_doc["_source"]["query_label"]] = 3
				# 	elif _doc["_source"]["query_label"] < 1.6:
				# 		settings.label_dict[_doc["_source"]["query_label"]] = 4
				# 	elif _doc["_source"]["query_label"] < 2:
				# 		settings.label_dict[_doc["_source"]["query_label"]] = 5
				# 	elif _doc["_source"]["query_label"] < 3:
				# 		settings.label_dict[_doc["_source"]["query_label"]] = 6
				# label = settings.label_dict[_doc["_source"]["query_label"]]
				for _f in _features.keys():
					if _mean_dict[_f]:
						_features[_f] = (_features[_f] - _mean_dict[_f]['mean']) / _mean_dict[_f]['std_dev']

				_row = [_count, _query_id, _doc["_id"],
				        _features["okapi_tf"], _features["tf_idf"], _features["bm25"], _features["laplace"],
				        _features["jm"],
				        _features["title_unique_term"], _features["text_unique_term"],
				        _features["text_term_appeared_in_query_len"], _features["title_term_appeared_in_query_len"],
				        _features["doc_len"],
				        _doc["_source"]["query_label"]]
				spamwriter.writerow(_row)

	with open('test_features.csv', 'w', newline='') as csvfile:
		spamwriter = csv.writer(csvfile)  # , quotechar='|', quoting=csv.QUOTE_MINIMAL)
		spamwriter.writerow(["index", "query_id", "doc_id",
		                     "okapi_tf", "tf_idf", "bm25", "laplace", "jm",
		                     "title_unique_term", "text_unique_term",
		                     "text_term_appeared_in_query_len", "title_term_appeared_in_query_len",
		                     "doc_len",
		                     "label"])
		_count = 0
		_query_id = _query_id_lst[2]
		_mean_dict = defaultdict(dict)
		_doc_type = "{}_doc".format(_query_id)
		_doc_list = generate_all_doc_list(settings.es, settings.target_index, _my_type=_doc_type)
		print('doc_list {} {}'.format(_query_id, len(_doc_list)))
		if not _doc_list:
			print(_f)
			print(_doc_type)
			print(_query_id)
			exit(-1)
		for _f in _norm_feature:
			_mean_dict[_f]['mean'] = get_mean(_doc_list, _f)

		for _f in _norm_feature:
			_mean_dict[_f]['std_dev'] = get_standard_deviation(_doc_list, _f, _mean_dict[_f]['mean'])
		for _doc in _doc_list:
			_count += 1
			_features = _doc["_source"]["features"]
			# if _doc["_source"]["query_label"] not in settings.label_dict:
			# 	if _doc["_source"]["query_label"] == 0.0:
			# 		settings.label_dict[_doc["_source"]["query_label"]] = 0
			# 	elif _doc["_source"]["query_label"] < 0.6:
			# 		settings.label_dict[_doc["_source"]["query_label"]] = 1
			# 	elif _doc["_source"]["query_label"] < 1:
			# 		settings.label_dict[_doc["_source"]["query_label"]] = 2
			# 	elif _doc["_source"]["query_label"] < 1.3:
			# 		settings.label_dict[_doc["_source"]["query_label"]] = 3
			# 	elif _doc["_source"]["query_label"] < 1.6:
			# 		settings.label_dict[_doc["_source"]["query_label"]] = 4
			# 	elif _doc["_source"]["query_label"] < 2:
			# 		settings.label_dict[_doc["_source"]["query_label"]] = 5
			# 	elif _doc["_source"]["query_label"] < 3:
			# 		settings.label_dict[_doc["_source"]["query_label"]] = 6
			# label = settings.label_dict[_doc["_source"]["query_label"]]
			for _f in _features.keys():
				if _mean_dict[_f]:
					_features[_f] = (_features[_f] - _mean_dict[_f]['mean']) / _mean_dict[_f]['std_dev']

			_row = [_count, _query_id, _doc["_id"],
			        _features["okapi_tf"], _features["tf_idf"], _features["bm25"], _features["laplace"],
			        _features["jm"],
			        _features["title_unique_term"], _features["text_unique_term"],
			        _features["text_term_appeared_in_query_len"], _features["title_term_appeared_in_query_len"],
			        _features["doc_len"],
			        _doc["_source"]["query_label"]]
			spamwriter.writerow(_row)

	with open('test_true_values.txt', 'w', errors='replace') as t:
		for _query_id in _query_id_lst[2:]:
			_doc_type = "{}_doc".format(_query_id)
			_count = 0
			for _doc in generate_all_doc(settings.es, settings.target_index, _my_type=_doc_type):
				_count += 1
				# print('{} {}'.format(_query_id, _count))
				t.write('{} {} {} {}\n'.format(_query_id, 0, _doc["_id"], _doc["_source"]["query_label"]))

	with open('train_true_values.txt', 'w', errors='replace') as t:
		for _query_id in _query_id_lst[:2]:
			_doc_type = "{}_doc".format(_query_id)
			for _doc in generate_all_doc(settings.es, settings.target_index, _my_type=_doc_type):
				t.write('{} {} {} {}\n'.format(_query_id, 0, _doc["_id"], _doc["_source"]["query_label"]))
	# pprint.pprint(settings.label_dict)


def to_predict(_linreg, _X, _query_id_lst, _doc_id_lst, _output_file):
	_y_pred = _linreg.predict(_X).tolist()
	# print(_y_pred)
	# print(len(_y_pred))
	# exit(-1)
	_y_pred_dict = defaultdict(dict)
	for _i in range(len(_y_pred)):
		_y_pred_dict[_query_id_lst[_i]][_doc_id_lst[_i]] = _y_pred[_i]

	# print(_linreg.classes_)

	with open(_output_file, 'w', errors='replace') as _t:
		for _query_id, _docs_scores in _y_pred_dict.items():
			_docs_id = sorted(_docs_scores, key=_docs_scores.get, reverse=True)
			_count = 0
			for _doc_id in _docs_id:
				_count += 1
				_t.write('{} Q0 {} {} {} Exp\n'.format(_query_id, _doc_id, _count, _docs_scores[_doc_id]))


if __name__ == "__main__":
	start_time = time.time()

	settings.init()
	query_dict = get_query_dict()

	write_train_test_files(settings.linreg_query_lst, settings.norm_feature)

	# training
	train_data = pd.read_csv('training_features.csv', index_col=0, encoding="ISO-8859-1")
	query_id_train = train_data["query_id"].tolist()
	doc_id_train = train_data["doc_id"].tolist()
	train_features = train_data[settings.feature_selected]
	# train_true = list(train_data["label"])
	train_true = train_data["label"]

	# testing
	test_data = pd.read_csv('test_features.csv', index_col=0, encoding="ISO-8859-1")
	query_id_test = test_data["query_id"].tolist()
	doc_id_test = test_data["doc_id"].tolist()
	test_features = test_data[settings.feature_selected]
	test_true = test_data["label"]

	linlogi = LinearRegression()
	# print(type(train_true))
	print(len(train_true))
	print(len(test_true))

	linlogi.fit(train_features, train_true)
	print(linlogi.coef_)
	for _ in zip(settings.feature_selected, linlogi.coef_):
		print(_)

	# linsvm = SVC(probability=True)
	# linsvm.fit(train_features, list(train_true))
	# for _ in zip(settings.feature_selected, linlogi.coef_):
	# 	print(_)

	to_predict(linlogi, test_features, query_id_test, doc_id_test, 'lin_test_predict.txt')
	to_predict(linlogi, train_features, query_id_train, doc_id_train, 'lin_train_predict.txt')
	# to_predict(linsvm, test_features, query_id_test, doc_id_test, 'svm_test_predict.txt')
	# to_predict(linsvm, train_features, query_id_train, doc_id_train, 'svm_train_predict.txt')

	print("--- {0} seconds ---".format(time.time() - start_time))
