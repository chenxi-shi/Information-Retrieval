import pprint
from collections import defaultdict
from os.path import dirname, abspath, join

from elasticsearch import Elasticsearch
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from scipy.sparse import csr_matrix

from es_methods import get_header_features, unique_term_count, doc_termvector, generate_all_doc_list, \
	generate_all_doc_id_list
import settings


def spam_words_lst(_path):
	_spam_words_set = set()
	with open(_path, "r", errors="replace", encoding='utf8') as _f:
		for _line in _f:
			_spam_words_set.add(_line.strip())
	return list(_spam_words_set)


def get_all_features(_es_instance, _target_index, _target_type, _all_features_dict=False, _for_test=False):

	# used in creating sparse matrix
	_sparse_row_doc_id = []  # doc id as row: [1, 1, 3, 4, 6], doc_id is int(doc_name)
	_sparse_col_feature_id = []  # feature id as column: [0, 1, 0, 2, 1], name of feature found from _feature_id_idx_dict
	_sparse_data_feature_score = []  # feature values: [1, 1, 1, 3, 4]

	_all_docs_lst, _true_value_lst = generate_all_doc_id_list(_es_instance, _target_index,
	                                                           _my_type=_target_type, _int_doc_id=False)
	print("Doc count: {}".format(len(_all_docs_lst)))
	# index a list will become slower and slower

	if not _for_test:
		_all_features_dict = {}  # here hush dict is must, otherwise, it will be very slow
	for i in range(len(_all_docs_lst)):
		_doc_id = _all_docs_lst[i]
		_terms_dict = doc_termvector(_es_instance, _target_index, _target_type, "text", _doc_id)
		for _term, _score in _terms_dict.items():
			if _score != 0:

				if not _for_test:
					if _term not in _all_features_dict:  # for show out term result
						_all_features_dict[_term] = len(_all_features_dict)

				# insert value into matrix if has value
				if _term in _all_features_dict:
					_sparse_row_doc_id.append(i)  # restore code of doc_id
					_sparse_col_feature_id.append(_all_features_dict[_term])
					_sparse_data_feature_score.append(_score)
		print(_doc_id)
	return _sparse_row_doc_id, _sparse_col_feature_id, _sparse_data_feature_score, \
	       _all_docs_lst, _all_features_dict, _true_value_lst



def to_predict(_model, _sparse_matrix_features, _true_value_lst, _all_doc_id_lst):
	_pred_value_lst = _model.predict_proba(_sparse_matrix_features)
	_test_total = len(_pred_value_lst)
	pred_rank_lst = []
	_correct_pred_count = 0

	for _i in range(_test_total):

		_pred_value = _model.classes_[0] if _pred_value_lst[_i][0] >= _pred_value_lst[_i][1] else _model.classes_[1]

		if _pred_value == _true_value_lst[_i]:
			_correct_pred_count += 1

		# [doc_id, predict_value, true_value]
		pred_rank_lst.append([_all_doc_id_lst[_i], _pred_value_lst[_i][1], _true_value_lst[_i]])

	print("Prediction of SPAM:")
	pred_rank_lst.sort(key=lambda x: x[1], reverse=True)
	for i in range(50):
		print("{}: predict {}, ture {}".format(pred_rank_lst[i][0], pred_rank_lst[i][1], pred_rank_lst[i][2]))

	print("Prediction of HAM:")
	pred_rank_lst.sort(key=lambda x: x[1])
	for i in range(50):
		print("{}: predict {}, ture {}".format(pred_rank_lst[i][0], pred_rank_lst[i][1], pred_rank_lst[i][2]))
	return _correct_pred_count / _test_total


def feature_rank(_model, _all_features_dict):
	_feature_lst = sorted(_all_features_dict, key=_all_features_dict.get)
	_feature_scores = zip(_feature_lst, _model.coef_[0])
	_feature_scores = sorted(_feature_scores, key=lambda x: x[1], reverse=True)
	for _feature, _score in _feature_scores[:200]:
		print(_feature, _score)


if __name__ == "__main__":
	settings.init()

	es = Elasticsearch()
	target_index = "hw7_dataset"
	train_type = "for_train"
	test_type = "for_test"
	spam_words_test = ["free", "click"]

	path = dirname(dirname(abspath("__file__")))
	resource_path = join(path, "trec07p")
	email_true_value_file = join(resource_path, "full", "index")

	print("Getting train features dict......")
	train_sparse_row_doc_id, train_sparse_col_feature_id, train_sparse_data_feature_score, \
	train_all_docs_lst, train_all_features_dict, train_true_value_lst = get_all_features(es, target_index, train_type)

	train_features_sparse_matrix = csr_matrix((train_sparse_data_feature_score,
	                                            (train_sparse_row_doc_id, train_sparse_col_feature_id)),
	                                           shape=(len(train_all_docs_lst),
	                                                  len(train_all_features_dict)))

	print("Getting test features dict......")
	test_sparse_row_doc_id, test_sparse_col_feature_id, test_sparse_data_feature_score, \
	test_all_docs_lst, test_all_features_dict, test_true_value_lst = get_all_features(es, target_index, test_type,
	                                                                                  _all_features_dict=train_all_features_dict,
	                                                                                  _for_test=True)

	test_features_sparse_matrix = csr_matrix((test_sparse_data_feature_score,
	                                            (test_sparse_row_doc_id, test_sparse_col_feature_id)),
	                                           shape=(len(test_all_docs_lst),
	                                                  len(test_all_features_dict)))

	print("Training module......")
	logreg = LogisticRegression()
	logreg.fit(train_features_sparse_matrix, train_true_value_lst)
	print("Label list: {}".format(logreg.classes_))

	print("Feature rank:")
	feature_rank(logreg, test_all_features_dict)

	print("Predict result:")
	print(to_predict(logreg, test_features_sparse_matrix, test_true_value_lst, test_all_docs_lst))


