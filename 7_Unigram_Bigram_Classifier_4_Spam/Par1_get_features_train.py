import pprint
from collections import defaultdict
from os.path import dirname, abspath, join

from elasticsearch import Elasticsearch
from sklearn.ensemble import RandomForestClassifier

from es_methods import get_header_features_backup, unigram_term_freq, multigram_term_freq
import settings


def spam_words_lst(_path):
	_spam_words_set = set()
	with open(_path, "r", errors="replace", encoding='utf8') as _f:
		for _line in _f:
			_spam_words_set.add(_line.strip())
	return list(_spam_words_set)


def get_all_features(_es_instance, _target_index, _target_type, _spam_words_lst,
                     stopwords_set):
	_features_dict = get_header_features_backup(_es_instance, _target_index, _target_type)
	_feature_index_dict = {
		'servers_count': 0,
		'span_time': 1,
		'weird_addr': 2,
		'weird_char': 3,
		'weird_content': 4,
		'weird_msg_id': 5,
		'weird_sbj': 6,
		'weird_target': 7,
		'wrong_time': 8
	}
	print("Doc count: {}".format(len(_features_dict)))
	for _doc_id, _features in _features_dict.items():
		_features_dict[_doc_id]["features_all"].extend([0] * len(_spam_words_lst))

	for _i in range(len(_spam_words_lst)):  # iterate all spam words
		_spam_item = _spam_words_lst[_i]  # for one spam word
		_spam_item = _spam_item.strip().lower().split()
		print(_spam_item)
		if len(_spam_item) == 1:
			_tf_dict = unigram_term_freq(es, _target_index, _target_type, _spam_item[0], "text", stopwords_set)
		elif len(_spam_item) > 1:
			_tf_dict = multigram_term_freq(es, _target_index, _target_type, _spam_item, "text", stopwords_set)
		else:
			print("Spam item is wrong, length {}".format(len(_spam_item)))
			exit(-1)
		for _doc_id, _hit_count in _tf_dict.items():
			_features_dict[_doc_id]["features_all"][_i + 9] = _hit_count  # for a doc has this spam word

	# don't need to norm
	for _w in _spam_words_lst:
		_feature_index_dict[_w] = len(_feature_index_dict)
	return _features_dict, _feature_index_dict


def create_matrix(_features_dict):
	_features_matrix = []
	_label_lst = []
	_doc_id_lst = []
	# TODO: change to all features
	for _doc_id, _doc_features in _features_dict.items():
		_features_matrix.append(_doc_features["features_all"])  # [9:]
		_label_lst.append(_doc_features["spam"])
		_doc_id_lst.append(_doc_id)
	return _features_matrix, _label_lst, _doc_id_lst


def to_predict(_rdmforst, _features_matrix, _true_value_lst, _test_id_lst):
	_pred_value_lst = _rdmforst.predict_proba(_features_matrix)
	_test_total = len(_pred_value_lst)
	pred_rank_dict = []
	_right_pred_count = 0

	for _i in range(_test_total):
		pred_rank_dict.append([_test_id_lst[_i], _pred_value_lst[_i][1], _true_value_lst[_i]])

		if _pred_value_lst[_i][0] > _pred_value_lst[_i][1]:
			_pred_value = _rdmforst.classes_[0]
		else:
			_pred_value = _rdmforst.classes_[1]
		# exit()
		if _pred_value == _true_value_lst[_i]:
			_right_pred_count += 1

	pred_rank_dict.sort(key=lambda x: x[1], reverse=True)
	for i in range(50):
		print("{}: predict {}, ture {}".format(pred_rank_dict[i][0], pred_rank_dict[i][1], pred_rank_dict[i][2]))
	return _right_pred_count / _test_total


def feature_rank(_model, _all_features_dict):
	_feature_lst = sorted(_all_features_dict, key=_all_features_dict.get)
	_feature_scores = zip(_feature_lst, _model.feature_importances_)
	_feature_scores = sorted(_feature_scores, key=lambda x: x[1], reverse=True)
	for _feature, _score in _feature_scores[:200]:
		print(_feature, _score)


if __name__ == "__main__":
	settings.init()

	es = Elasticsearch()
	target_index = "hw7_dataset2"
	train_type = "for_train"
	test_type = "for_test"
	spam_words_test = ["free", "click"]

	path = dirname(dirname(abspath("__file__")))
	resource_path = join(path, "trec07p")
	email_true_value_file = join(resource_path, "full", "index")
	spam_words_file = "spam_words.txt"

	print("Getting spam words list......")
	spam_words = spam_words_lst(spam_words_file)
	print("Getting train features dict......")
	train_features_dict, train_feature_index_dict = get_all_features(es, target_index, train_type, spam_words, settings.stop_words_set)
	print("Getting test features dict......")
	test_features_dict, train_feature_index_dict = get_all_features(es, target_index, test_type, spam_words, settings.stop_words_set)

	train_features_matrix, train_label_lst, _train_id_lst = create_matrix(train_features_dict)
	test_features_matrix, test_label_lst, _test_id_lst = create_matrix(test_features_dict)
	print("Training module......")
	rdmforst = RandomForestClassifier(n_estimators=300)
	rdmforst.fit(train_features_matrix, train_label_lst)
	print("Label list: {}".format(rdmforst.classes_))
	print("Predict result:")
	print(to_predict(rdmforst, test_features_matrix, test_label_lst, _test_id_lst))

	print("Feature rank:")
	feature_rank(rdmforst, train_feature_index_dict)
