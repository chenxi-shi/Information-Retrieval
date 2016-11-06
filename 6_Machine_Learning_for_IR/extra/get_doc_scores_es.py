import time
from collections import defaultdict
from math import log

from elasticsearch import Elasticsearch

from restore_query_es import get_query_dict, load_qrels
from es_methods import doc_length, doc_freq_AND_term_freq, update_doc, total_num_docs, generate_all_doc, \
	text_unique_term_count, title_unique_term_count
from load_data_es import whole_prep_dataset
import settings


class restore_scores():
	query_dict = get_query_dict()
	results_dict = load_qrels(query_dict)

	def __init__(self, _es_instance, _source_index, _source_type, _target_index,
	             _index_es_flag=False, _restore_doc_len_flag=False,
	             _okapi_idf_bm25=False, _laplace=False, _jm=False,
	             _all_5_score=False, _update_doc_len=False,
	             _text_term_appeared=False, _title_term_appeared=False,
	             _text_unique_term_update=False, _title_unique_term_update=False):
		self._write_es_flag = _index_es_flag
		self._restore_doc_len_flag = _restore_doc_len_flag

		self._okapi_idf_bm25 = _okapi_idf_bm25
		self._laplace = _laplace
		self._jm = _jm
		self._all_5_score = _all_5_score
		self._update_doc_len = _update_doc_len

		if self._okapi_idf_bm25 or self._laplace or self._jm or self._all_5_score:
			self._restore_doc_len_flag = True

		self._text_term_appeared = _text_term_appeared
		self._title_term_appeared = _title_term_appeared
		self._text_unique_term_update = _text_unique_term_update
		self._title_unique_term_update = _title_unique_term_update


		self._es_instance = _es_instance
		self._source_index = _source_index
		self._target_index = _target_index
		self._source_type = _source_type
		self._doc_count = 0

	def restore_text_title_lable(self):
		'''
		restore doc_length into es,
		_result_dict = {query_id: {'doc_id': {'label": 1, 'doc_len': x}, 'doc_id': {'label": 0, 'doc_len': x}, ...},
					query_id: {'doc_id': {'label": 1, 'doc_len': x}, 'doc_id': {'label": 0, 'doc_len': x}, ...}, ...}
		the key of results_dict is query id
		:return:
		'''
		# parse and write doc text, title, and label into es
		if self._write_es_flag:
			whole_prep_dataset(self._es_instance, self._source_index, self._source_type, self._target_index, restore_scores.results_dict)

	def restore_doc_len(self):
		if self._restore_doc_len_flag:
			print("Starting restoring doc length...")
			# update doc_len into _result_dict
			for _query_id, _docs in restore_scores.results_dict.items():
				_doc_type = "{}_doc".format(_query_id)
				for _doc_id in _docs.keys():
					_doc_len = doc_length(self._es_instance, self._target_index, _doc_type, _doc_id)
					restore_scores.results_dict[_query_id][_doc_id]["doc_len"] = _doc_len
			print("Finished restoring doc length...")

	def get_average_doc_len(self, _query_id):
		_avg_doc_len = 0
		for _doc_detail in restore_scores.results_dict[_query_id].values():
			_avg_doc_len += _doc_detail['doc_len']
		_avg_doc_len /= len(restore_scores.results_dict[_query_id])
		return int(_avg_doc_len)

	def data_prepare_one_query(self, _query_id, _term_set):
		_avg_doc_len = restore_scores.get_average_doc_len(self, _query_id)
		_doc_type = "{}_doc".format(_query_id)
		_type_size = len(restore_scores.results_dict[_query_id])
		_D = total_num_docs(self._es_instance, self._target_index, _doc_type)
		_ttf = sum(_doc["doc_len"] for _doc in restore_scores.results_dict[_query_id].values())
		_V = text_unique_term_count(self._es_instance, self._target_index, _doc_type)
		_query_len = len(_term_set)

		return {"_avg_doc_len": _avg_doc_len,
		        "_doc_type": _doc_type,
		        "_type_size": _type_size,
		        "_D": _D,
		        "_ttf": _ttf,
		        "_V": _V,
		        "_query_len": _query_len}

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


	def one_doc_okapi_idf_bm25(self, _doc_id, _query_id, _term_freq, _doc_freq, _avg_doc_len, _D):
		# write _doc_len into elasticsearch
		_doc_len = restore_scores.results_dict[_query_id][_doc_id]["doc_len"]
		_one_term_okapi = restore_scores.term_okapi_tf(_term_freq, _doc_len, _avg_doc_len)
		_one_term_tf_idf = restore_scores.term_tf_idf(_term_freq, _doc_len, _avg_doc_len,
		                                              _D, _doc_freq)
		_one_term_bm25 = restore_scores.term_bm25(_term_freq, _doc_len, _avg_doc_len,
		                                          _D, _doc_freq)

		if "okapi_tf" in self._okapi_idf_bm25_dict[_doc_id]:
			self._okapi_idf_bm25_dict[_doc_id]["okapi_tf"] += _one_term_okapi
		else:
			self._okapi_idf_bm25_dict[_doc_id]["okapi_tf"] = _one_term_okapi

		if "tf_idf" in self._okapi_idf_bm25_dict[_doc_id]:
			self._okapi_idf_bm25_dict[_doc_id]["tf_idf"] += _one_term_tf_idf
		else:
			self._okapi_idf_bm25_dict[_doc_id]["tf_idf"] = _one_term_tf_idf

		if "bm25" in self._okapi_idf_bm25_dict[_doc_id]:
			self._okapi_idf_bm25_dict[_doc_id]["bm25"] += _one_term_bm25
		else:
			self._okapi_idf_bm25_dict[_doc_id]["bm25"] = _one_term_bm25

	def prepare_laplace_jm_dict(self, _lam, _ttf, _V, _doc_type, _query_id, _term_set):
		# for laplace
		_log_x_laplace_dict = defaultdict(lambda: 1)  # init log_x for each doc to 1   {_doc_id: doc_laplace, ...}

		# for jm
		_log_x_right = (1 - _lam) * _ttf / _V
		_doc_jm_dict = {}
		# _doc_jm_dict = {doc_id: {term1: score, term2: score,...},
		#                 doc_id: {term1: score, term2: score,...}, ...}
		# init _log_x_dict depand on doc_len
		for _doc in generate_all_doc(self._es_instance, self._target_index, _my_type=_doc_type):
			# for laplace
			_doc_len = restore_scores.results_dict[_query_id][_doc['_id']]["doc_len"]
			_V = text_unique_term_count(self._es_instance, self._target_index, _doc_type)
			_log_x_laplace_dict[_doc['_id']] /= (_doc_len + _V) ** len(_term_set)

			# for jm
			_doc_jm_dict[_doc['_id']] = dict.fromkeys(_term_set, _log_x_right)

		return _log_x_laplace_dict, _doc_jm_dict

	def restore_all_5_scores(self, _lam):
		if self._all_5_score:
			for _query_id, _term_set in restore_scores.query_dict.items():
				_query_param = restore_scores.data_prepare_one_query(self, _query_id, _term_set)
				self._okapi_idf_bm25_dict = defaultdict(dict)

				self._log_x_laplace_dict, \
				self._doc_jm_dict = restore_scores.prepare_laplace_jm_dict(self, _lam,
				                                                           _query_param["_ttf"],
				                                                           _query_param["_V"],
				                                                           _query_param["_doc_type"],
				                                                           _query_id,
				                                                           _term_set)
				self._text_term_appeared_dict = {}

				for _term in _term_set:
					_doc_freq, _term_freq_dict = doc_freq_AND_term_freq(self._es_instance, self._target_index,
					                                                    _query_param["_doc_type"],
					                                                    _query_param["_type_size"], _term, "text")

					if _term_freq_dict:
						for _doc_id, _term_freq in _term_freq_dict.items():
							# for okapi_idf_bm25
							restore_scores.one_doc_okapi_idf_bm25(self, _doc_id, _query_id, _term_freq, _doc_freq,
							                                      _query_param["_avg_doc_len"], _query_param["_D"])

							# for laplace
							self._log_x_laplace_dict[_doc_id] *= _term_freq + 1

							# for jm
							_doc_len = restore_scores.results_dict[_query_id][_doc_id]["doc_len"]
							self._doc_jm_dict[_doc_id][_term] += _lam * _term_freq / _doc_len

							# for text_term_appeared
							if _doc_id in self._text_term_appeared_dict:
								self._text_term_appeared_dict[_doc_id] += 1
							else:
								self._text_term_appeared_dict[_doc_id] = 1


				for _doc in generate_all_doc(self._es_instance, self._target_index, _my_type=_query_param["_doc_type"]):
					_change_script = ""

					if self._update_doc_len:
						_doc_len = restore_scores.results_dict[_query_id][_doc['_id']]["doc_len"]
						_change_script += "ctx._source.features.doc_len = {};".format(_doc_len)

					if self._okapi_idf_bm25 and _doc['_id'] in self._okapi_idf_bm25_dict:
						_one_term_okapi = self._okapi_idf_bm25_dict[_doc['_id']]["okapi_tf"]
						_one_term_tf_idf = self._okapi_idf_bm25_dict[_doc['_id']]["tf_idf"]
						_one_term_bm25 = self._okapi_idf_bm25_dict[_doc['_id']]["bm25"]
						_change_script += "ctx._source.features.okapi_tf = {};" \
						                  "ctx._source.features.tf_idf = {};" \
						                  "ctx._source.features.bm25 = {};".format(_one_term_okapi,
						                                                           _one_term_tf_idf,
						                                                           _one_term_bm25)

					if self._text_term_appeared and _doc["_id"] in self._text_term_appeared_dict:
						_change_script += "ctx._source.features.text_term_appeared_in_query_len = {};".format(
							self._text_term_appeared_dict[_doc["_id"]]/_query_param["_query_len"])

					if self._laplace:
						self._log_x_laplace_dict[_doc['_id']] = log(self._log_x_laplace_dict[_doc['_id']], 2)
						print('{} {}'.format(_doc['_id'], self._log_x_laplace_dict[_doc['_id']]))
						_change_script += "ctx._source.features.laplace = {};".format(
							self._log_x_laplace_dict[_doc['_id']])

					if self._jm:
						_jm_score = sum(log(_s, 2) for _s in self._doc_jm_dict[_doc['_id']].values())
						_change_script += "ctx._source.features.jm = {};".format(_jm_score)

					if self._text_unique_term_update:
						_text_unique_term = text_unique_term_count(self._es_instance, self._target_index, _query_param["_doc_type"], _doc_id=_doc['_id'])
						_change_script += "ctx._source.features.text_unique_term = {};".format(_text_unique_term)

					if self._title_unique_term_update:
						_title_unique_term = title_unique_term_count(self._es_instance, self._target_index, _query_param["_doc_type"], _doc_id=_doc['_id'])
						_change_script += "ctx._source.features.title_unique_term = {};".format(_title_unique_term)

					update_doc(self._es_instance, self._target_index, _query_param["_doc_type"], _doc['_id'],
					           _change_script=_change_script)

				self._es_instance.indices.refresh(index=self._target_index)


	def title_term_appeared(self):
		if self._title_term_appeared:
			for _query_id, _term_set in restore_scores.query_dict.items():
				self._title_term_appeared_dict = {}
				_query_param = {}
				_query_param["_doc_type"] = "{}_doc".format(_query_id)
				_query_param["_type_size"] = len(restore_scores.results_dict[_query_id])
				_query_param["_query_len"] = len(_term_set)

				for _term in _term_set:
					_doc_freq, _term_freq_dict = doc_freq_AND_term_freq(self._es_instance, self._target_index,
					                                                    _query_param["_doc_type"],
					                                                    _query_param["_type_size"], _term, "head")
					if _term_freq_dict:
						for _doc_id, _term_freq in _term_freq_dict.items():
							if _doc_id in self._title_term_appeared_dict:
								self._title_term_appeared_dict[_doc_id] += 1
							else:
								self._title_term_appeared_dict[_doc_id] = 1

				for _doc_id, _term_appeared_count in self._title_term_appeared_dict.items():
					_change_script = "ctx._source.features.title_term_appeared_in_query_len = {};".format(_term_appeared_count/_query_param["_query_len"])
					update_doc(self._es_instance, self._target_index, _query_param["_doc_type"], _doc_id,
					           _change_script=_change_script)
				self._es_instance.indices.refresh(index=self._target_index)


if __name__ == '__main__':
	start_time = time.time()

	settings.init()

	restore_behavior = restore_scores(settings.es, settings.source_index, settings.source_type, settings.target_index,
	                                  _index_es_flag=True,
	                                  _okapi_idf_bm25=True, _laplace=True, _jm=True,
	                                  _all_5_score=True, _update_doc_len=True,
	                                  _text_term_appeared=True, _title_term_appeared=True,
	                                  _text_unique_term_update=True, _title_unique_term_update=True)
	restore_behavior.restore_text_title_lable()  # _write_es_flag
	restore_behavior.restore_doc_len()  # _restore_doc_len_flag

	restore_behavior.restore_all_5_scores(settings.lam)
	restore_behavior.title_term_appeared()

	print("--- {0} seconds ---".format(time.time() - start_time))
