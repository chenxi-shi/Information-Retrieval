from math import log
from collections import defaultdict
from collections import OrderedDict


def read_file(_my_results_file, _qrel_file):
	# read reasult file
	with open(_my_results_file, 'r', errors='replace') as r:
		_my_results_dict = defaultdict(list)

		for _line in r:
			query_id, Q0, doc_id, rank, score, Exp = _line.strip().split()
			_my_results_dict[query_id].append(doc_id)  # dict{query_id:[doc_id_high, doc_id_low], ...}

	# read qrel.adhoc file
	_query_relev_count=defaultdict(int)
	with open(_qrel_file, 'r', errors='replace') as q:
		_qrels = defaultdict(OrderedDict)

		for _line in q:
			query_id, AssessorID, doc_id, score = _line.strip().split()
			score = float(score)
			_query_relev_count[query_id] += 1 if score >= 1 else 0  # dict{query_id: relev_count_total, ...}
			_qrels[query_id][doc_id] = score  # dict{query_id:{doc_id: score, ...}, ...}

	return _my_results_dict, _qrels, _query_relev_count  # total relev count in result




def get_eval(_my_results_dict, _qrels, _query_relev_count, _K):
	_R_precision = {}
	_Avg_Precision = {}
	_DCG = {}
	_nDCG = {}
	_precision_at_k = defaultdict(OrderedDict)
	_recall_at_k = defaultdict(OrderedDict)
	_F1_at_k = defaultdict(OrderedDict)  # k=5, 10, 20, 50, 100

	_plot_data = {}

	_sum_relev_count = 0
	_sum_total_relev = 0

	for _query_id in _my_results_dict:
		_R_precision[_query_id] = 0
		_Avg_Precision[_query_id] = 0
		_DCG[_query_id] = 0
		_nDCG[_query_id] = 0
		_plot_data[_query_id] = [(0, 1)]
		_relev_ret_count = 0
		_num_retrieved = 0

		_Sum_Precision = 0

		for _doc_id in _my_results_dict[_query_id]:
			_num_retrieved += 1

			_doc_score = _qrels[_query_id][_doc_id] if _doc_id in _qrels[_query_id] else 0

			if _doc_score >= 1:
				_relev_ret_count += 1
				_Sum_Precision += _relev_ret_count / _num_retrieved
				# _Avg_Precision[_query_id] += _relev_ret_count / _num_retrieved

			# _DCG[_query_id] += (2 ** _doc_score - 1) / log(1 + _num_retrieved, 2)
			_DCG[_query_id] += _doc_score if _num_retrieved == 1 else _doc_score / log(_num_retrieved, 2)

			_precision = _relev_ret_count / _num_retrieved
			_recall = _relev_ret_count / _query_relev_count[_query_id]

			if _precision == _recall and _precision != 0:
				_R_precision[_query_id] = _precision

			if _num_retrieved in _K:
				_precision_at_k[_query_id][_num_retrieved] = _precision
				_recall_at_k[_query_id][_num_retrieved] = _recall

				_F1_at_k[_query_id][_num_retrieved] = _precision * _recall * 2 / (_precision + _recall) if _relev_ret_count > 0 else 0

			if _precision != 0:
				_plot_data[_query_id].append((_recall, _precision))

		_num_retrieved = 0
		_nDCG_denominator = 0
		for _doc_id, _doc_score in _qrels[_query_id].items():
			_num_retrieved += 1
			_nDCG_denominator += _doc_score if _num_retrieved == 1 else _doc_score / log(_num_retrieved, 2)

		_nDCG[_query_id] = _DCG[_query_id] / _nDCG_denominator
		_Avg_Precision[_query_id] = _Sum_Precision / _query_relev_count[_query_id]
		print('{}: {} = {} / {}'.format(_query_id, _Avg_Precision[_query_id], _Sum_Precision, _query_relev_count[_query_id]))
		_sum_relev_count += _relev_ret_count
		_sum_total_relev += _query_relev_count[_query_id]

	print('Relevant_Retrieval {}, total_relev {}'.format(_sum_relev_count, _sum_total_relev))
	return _R_precision, _Avg_Precision, _nDCG, _precision_at_k, _recall_at_k, _F1_at_k, _plot_data


def write_file(_plot_data):
	for _query_id in _plot_data:
		_query_list = []
		max_val = 1
		for _recall, _precision in _plot_data[_query_id]:
			max_val = min(max_val, _precision)  # prevent _precision from increasing
			_query_list.append((_recall, max_val))

		with open('plot_data\Precision_{}'.format(_query_id), 'w', errors='replace') as p:
			with open('plot_data\Recall_{}'.format(_query_id), 'w', errors='replace') as r:
				for _recall, _precision in _query_list:
					r.write('{}\n'.format(_recall))
					p.write('{}\n'.format(_precision))


def output_info(q_flag, _my_results_dict, _Avg_Precision, _precision_at_k, _R_precision, nDCG, _recall_at_k, _F1_at_k, _K):
	if q_flag:
		for query_id in _my_results_dict:
			print("qid: {}\navg precision: \n\t{:.4f}".format(query_id, _Avg_Precision[query_id]))

			print('precision:')
			for k, v in _precision_at_k[query_id].items():
				print('\t@\t{}\t:\t{:.4f}'.format(k, v))

			print('f1:')
			for k, v in _F1_at_k[query_id].items():
				print('\t@\t{}\t:\t{:.4f}'.format(k, v))

			print('recall:')
			for k, v in _recall_at_k[query_id].items():
				print('\t@\t{}\t:\t{:.4f}'.format(k, v))

			print('nDCG:')
			print('\t{:.4f}'.format(nDCG[query_id]))

			print('r-precision :')
			print('\t{:.4f}\n\n\n'.format(_R_precision[query_id]))

	print('number of  queries num:\t{}'.format(len(_my_results_dict)))

	avg_Avg_Precision = sum(_Avg_Precision.values()) / len(_Avg_Precision)
	avg_nDCG = sum(nDCG.values()) / len(nDCG)
	avg_R_precision = sum(_R_precision.values()) / len(_R_precision)

	def calcu_avg(_at_k_score_list):
		_avg_score = {}
		for k in _K:
			score = 0
			count = 0
			for query_id in _at_k_score_list:
				if k in _at_k_score_list[query_id]:
					score += _at_k_score_list[query_id][k]
					count += 1
			_avg_score[k] = score / count
		return _avg_score

	avg_precision_at_k = calcu_avg(_precision_at_k)
	avg_recall_at_k = calcu_avg(_recall_at_k)
	avg_F1_at_k = calcu_avg(_F1_at_k)

	print('avg average precision : ')
	print('\t{:.4f}'.format(avg_Avg_Precision))

	print('avg precision at k :')
	for k in _K:
		print('\t@ {} :\t{:.4f}'.format(k, avg_precision_at_k[k]))

	print('avg r-precision :')
	print('\t{:.4f}'.format(avg_R_precision))

	print('avg nDCG :')
	print('\t{:.4f}'.format(avg_nDCG))

	print('avg recall at k :')
	for k in _K:
		print('\t@ {}  :\t{:.4f}'.format(k, avg_recall_at_k[k]))


	print('avg f1 at K :')
	for k in _K:
		print('\t@ {}:\t{:.4f}'.format(k, avg_F1_at_k[k]))


def merge_search():
	with open('search_results.txt', 'w', errors='replace') as _all:
		with open('search_152601.txt', 'r', errors='replace') as _f1:
			_count = 0
			for _ in _f1:
				_count += 1
				#60 Q0 AP890314-0305 1 15.5604676736 Exp
				_all.write('{}\t{}\t{}\t{}\t{}\t{}\n'.format(152601, 'Q0', _.strip(), _count, 201-_count, 'Exp'))
		with open('search_152602.txt', 'r', errors='replace') as _f2:
			_count = 0
			for _ in _f2:
				_count += 1
				_all.write('{}\t{}\t{}\t{}\t{}\t{}\n'.format(152602, 'Q0', _.strip(), _count, 201-_count, 'Exp'))
		with open('search_152603.txt', 'r', errors='replace') as _f3:
			_count = 0
			for _ in _f3:
				_count += 1
				_all.write('{}\t{}\t{}\t{}\t{}\t{}\n'.format(152603, 'Q0', _.strip(), _count, 201-_count, 'Exp'))


if __name__=="__main__":

	K = [5, 10, 15, 20, 30, 100, 200, 500, 1000]
	K2 = [5, 10, 20, 50, 100]

	q_flag = True
	# my_results_file = "bm25_result.txt"
	# qrel_file = "qrels.adhoc.51-100.AP89.txt"
	my_results_file = "search_results.txt"
	qrel_file = "sorted_avg_score.txt"
	my_results_dict, qrels, query_relev_count=read_file(my_results_file, qrel_file)

	# return _R_precision, _Avg_Precision, _nDCG, _precision_at_k, _recall_at_k, _F1_at_k, _plot_data
	R_precision, Avg_Precision, nDCG, precision_at_k, recall_at_k, F1_at_k, plot_data = get_eval(my_results_dict, qrels, query_relev_count, K2)
	output_info(q_flag, my_results_dict, Avg_Precision, precision_at_k, R_precision, nDCG, recall_at_k, F1_at_k, K2)
	write_file(plot_data)

