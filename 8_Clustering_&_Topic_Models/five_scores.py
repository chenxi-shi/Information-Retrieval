import os

from settings import *
from es_method import all_doc_generator, text_unique_term_count, doc_freq_AND_term_freq
from restore_query_es import get_query_dict


class restore_scores():
	query_dict = get_query_dict()
	docs_detail_dict = defaultdict(dict)
	shared_param = {}

	def __init__(self, _es_instance, _source_index, _source_type):

		self._es = _es_instance
		self._source_index = _source_index
		self._source_type = _source_type
		self._doc_count = 0

		restore_scores.restore_doc_len(self)
		restore_scores.shared_param = restore_scores.shared_param_preparation(self)

	def restore_doc_len(self):
		'''
		used by __init__
		:return:
		'''
		print("Starting restoring doc length...")
		_doc_detail_dict = all_doc_generator(self._es, self._source_index, ["doc_len"], _my_type=self._source_type, _doc_id_flg=False)
		# update doc_len into _result_dict
		for _doc_id, _fields in _doc_detail_dict.items():
			restore_scores.docs_detail_dict[_doc_id]["doc_len"] = _fields["doc_len"][0]
		print("Finished restoring doc length...")


	def get_average_doc_len(self):
		'''
		Used by shared_param_preparation
		:return:
		'''
		_avg_doc_len = 0
		for _doc_detail in restore_scores.docs_detail_dict.values():
			_avg_doc_len += _doc_detail['doc_len']
		_avg_doc_len /= len(restore_scores.docs_detail_dict)
		return int(_avg_doc_len)

	def shared_param_preparation(self):
		'''
		used by __init__
		:return:
		'''
		_avg_doc_len = restore_scores.get_average_doc_len(self)
		_D = len(restore_scores.docs_detail_dict)
		_ttf = sum(_doc["doc_len"] for _doc in restore_scores.docs_detail_dict.values())
		_V = text_unique_term_count(self._es, self._source_index, self._source_type)

		return {"_avg_doc_len": _avg_doc_len,
		        "_D": _D,
		        "_ttf": _ttf,
		        "_V": _V}

	@staticmethod
	def term_okapi_tf(_tf, _doc_len, _avg_doc_len):
		try:
			_one_term_okapi = _tf / (_tf + 0.5 + (1.5 * _doc_len / _avg_doc_len))
		except:
			if (_tf + 0.5 + (1.5 * _doc_len / _avg_doc_len)) == 0:
				print("(_tf + 0.5 + (1.5 * _doc_len / _avg_doc_len)) == 0")
			if _avg_doc_len == 0:
				print("_avg_doc_len == 0")
			exit(-1)
		else:
			return _one_term_okapi

	@staticmethod
	def term_tf_idf(_tf, _doc_len, _avg_doc_len, _D, _df):
		try:
			_one_term_tf_idf = restore_scores.term_okapi_tf(_tf, _doc_len, _avg_doc_len) * log(_D / _df, 2)
		except:
			if _df == 0:
				print("_df == 0")
			exit(-1)
		else:
			return _one_term_tf_idf

	@staticmethod
	def term_bm25(_tf, _doc_len, _avg_doc_len, _D, _df):
		try:
			_one_term_bm25 = 2.2 * _tf / (_tf + 1.2 * (0.25 + 0.75 * _doc_len / _avg_doc_len)) * log(
				(_D + 0.5) / (_df + 0.5))  # tf_q = 1
		except:
			if _avg_doc_len == 0:
				print("_avg_doc_len == 0")
			if (_df + 0.5) == 0:
				print("(_df + 0.5) ==0")
			if (_tf + 1.2 * (0.25 + 0.75 * _doc_len / _avg_doc_len)) * log(
							(_D + 0.5) / (_df + 0.5)) == 0:
				print("(_tf + 1.2 * (0.25 + 0.75 * _doc_len / _avg_doc_len)) * log((_D + 0.5) / (_df + 0.5)) == 0")
			exit(-1)
		else:
			return _one_term_bm25

	def one_doc_okapi_idf_bm25(self, _doc_id, _term_freq, _doc_freq):
		# write _doc_len into elasticsearch
		_avg_doc_len = restore_scores.shared_param["_avg_doc_len"]
		_D = restore_scores.shared_param["_D"]


		_doc_len = restore_scores.docs_detail_dict[_doc_id]["doc_len"]
		_one_term_okapi = restore_scores.term_okapi_tf(_term_freq, _doc_len, _avg_doc_len)
		_one_term_tf_idf = restore_scores.term_tf_idf(_term_freq, _doc_len, _avg_doc_len,
		                                              _D, _doc_freq)
		_one_term_bm25 = restore_scores.term_bm25(_term_freq, _doc_len, _avg_doc_len,
		                                          _D, _doc_freq)

		if _doc_id in self._okapi_tf:
			self._okapi_tf[_doc_id] += _one_term_okapi
		else:
			self._okapi_tf[_doc_id] = _one_term_okapi

		if _doc_id in self._tf_idf:
			self._tf_idf[_doc_id] += _one_term_tf_idf
		else:
			self._tf_idf[_doc_id] = _one_term_tf_idf

		if _doc_id in self._bm25:
			self._bm25[_doc_id] += _one_term_bm25
		else:
			self._bm25[_doc_id] = _one_term_bm25

	def prepare_laplace_jm_dict(self, _lam, _term_set):
		# for laplace
		_log_x_laplace_dict = defaultdict(lambda: 1)  # init log_x for each doc to 1   {_doc_id: doc_laplace, ...}
		_doc_jm_dict = defaultdict(dict)
		# _doc_jm_dict = {doc_id: {term1: score, term2: score,...},
		#                 doc_id: {term1: score, term2: score,...}, ...}
		# init _log_x_dict depand on doc_len

		_ttf = restore_scores.shared_param["_ttf"]
		_V = restore_scores.shared_param["_V"]
		# for jm
		_log_x_right = (1 - _lam) * _ttf / _V

		for _doc_id, _doc_detail in restore_scores.docs_detail_dict.items():
			# for laplace
			_doc_len = _doc_detail["doc_len"]
			_log_x_laplace_dict[_doc_id] /= (_doc_len + _V) ** len(_term_set)

			# for jm
			_doc_jm_dict[_doc_id] = dict.fromkeys(_term_set, _log_x_right)

		return _log_x_laplace_dict, _doc_jm_dict

	def restore_all_5_scores(self, _lam):
		self._okapi_tf = {}
		self._tf_idf = {}
		self._bm25 = {}
		self._log_x_laplace_dict, \
		self._doc_jm_dict = restore_scores.prepare_laplace_jm_dict(self, _lam, _term_set)

		for _term in _term_set:
			_doc_freq, _term_freq_dict = doc_freq_AND_term_freq(self._es, self._source_index,
			                                                    self._source_type,
			                                                    _term, "text")

			if _term_freq_dict:
				for _doc_id, _term_freq in _term_freq_dict.items():
					# for okapi_idf_bm25
					restore_scores.one_doc_okapi_idf_bm25(self, _doc_id, _term_freq, _doc_freq)

					# for laplace
					self._log_x_laplace_dict[_doc_id] *= _term_freq + 1

					# for jm
					_doc_len = restore_scores.docs_detail_dict[_doc_id]["doc_len"]
					self._doc_jm_dict[_doc_id][_term] += _lam * _term_freq / _doc_len

		for _doc_id, _terms_scores in self._doc_jm_dict.copy().items():
			self._doc_jm_dict[_doc_id] = sum(log(_s, 2) for _s in _terms_scores.values())



	@ staticmethod
	def write_one_model(_query_id, _res_path, _model_name, _doc_score_dict):
		'''
		used by write_1st_1000
		:param _query_id:
		:param _res_path:
		:param _model_name:
		:param _doc_score_dict:
		:return:
		'''
		with open(os.path.join(_res_path, _model_name), 'a') as _f:
			_i = 0
			for _doc_id in sorted(_doc_score_dict, key=_doc_score_dict.get, reverse=True)[:1000]:
				_i += 1
				# <query-number> Q0 <docno> <rank> <score> Exp
				_f.write('{} Q0 {} {} {} Exp\n'.format(_query_id, _doc_id, _i, _doc_score_dict[_doc_id]))

	@ staticmethod
	def clean_results(_dir, _model_lst):
		'''
		used by write_1st_1000
		:param _dir:
		:return:
		'''
		for _ in os.listdir(_dir):
			if _ in _model_lst:
				os.remove(os.path.join(_dir, _))


	def write_1st_1000(self):
		restore_scores.write_one_model(_query_id, "results", "okapi_tf", self._okapi_tf)
		restore_scores.write_one_model(_query_id, "results", "tf_idf", self._tf_idf)
		restore_scores.write_one_model(_query_id, "results", "bm25", self._bm25)
		restore_scores.write_one_model(_query_id, "results", "laplace", self._log_x_laplace_dict)
		restore_scores.write_one_model(_query_id, "results", "j_mercer", self._doc_jm_dict)


if __name__ == "__main__":
	start_time = time.time()
	restore_scores.clean_results("results", ["okapi_tf", "tf_idf", "bm25", "laplace", "j_mercer"])
	restore_behavior = restore_scores(es, doc_index, doc_type)
	print(restore_scores.shared_param)
	for _query_id, _term_set in restore_scores.query_dict.items():
		restore_behavior.restore_all_5_scores(0.5)
		print(_query_id)
		restore_behavior.write_1st_1000()