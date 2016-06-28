import os
import pprint
import re
import time
from os.path import dirname, abspath
from stemming.porter2 import stem
import math

def load_cat(_cat):
	category = {}
	with open(_cat, 'r', errors='ignore') as c:
		for line in c:
			_t_id, b, ofs = line.split()
			category[int(_t_id)] = [int(b), int(ofs)]
	return category


def get_files_name(n_pattern, _dir):
	files = []
	for _ in os.listdir(_dir):
		if re.match(n_pattern, _):
			files.append(_)
	return files


def get_term(_term):
	"""
	retreive term information from inv
	term_id:ttf|doc_id,tf,tp,tp,tp,...|doc_id,tf,tp,tp,tp,...|...
	_term_dict = {ttf: y,
				details: [{'doc_id': doc_id, 'tf': x, 'doc_len':l, 'tp': [ , ]},
							{'doc_id': doc_id, 'tf': x, 'doc_len':l, 'tp': [ , ]}, ...]}
	:param term:
	:return:
	"""
	global term_map
	global cat
	global inv
	global doc_map
	_term_dict = {}
	if _term in term_map:  # into term
		_t_id = term_map[_term]
		_t_b, _t_ofs = cat[_t_id]
		inv.seek(_t_b, 0)
		_term_all = inv.read(_t_ofs)
		_term_id, _term_detail = _term_all.split(':', 1)
		if int(_term_id) != _t_id:
			print('INV1 {}, term_map {} is not correspond.'.format(_term_id, t_id))
			exit(-1)
		# _term_dict[t_id] = {}
		_term_detail = _term_detail.split('|')
		_ttf = int(_term_detail[0])
		# _term_dict[t_id]['ttf'] = ttf
		_term_dict['ttf'] = _ttf
		_term_detail = _term_detail[1:]
		# _term_dict[t_id]['detail'] = []
		_term_dict['detail'] = []
		for _ in _term_detail:  # into one doc
			_a_doc = {}
			_ = _.split(',')  # _ is one doc
			_a_doc['doc_id'] = int(_[0])
			_a_doc['doc_len'] = doc_map[_a_doc['doc_id']][1]
			_a_doc['tf'] = int(_[1])
			_a_doc['tp'] = _[2:]
			# _term_dict[t_id]['detail'].append(a_doc)
			_term_dict['detail'].append(_a_doc)
	return _term_dict


def okapi_tf(_res_path, _q_id, _query):
	global avg_d_l
	global doc_map
	_okapiTF_docs = {}
	for _t in _query:
		_t = stem(_t)
		_term_dict = get_term(_t)
		if len(_term_dict) > 0:
			for _ in _term_dict['detail']:  # into one doc
				_doc_id = _['doc_id']
				_doc_len = _['doc_len']
				_tf = _['tf']
				if _doc_id in _okapiTF_docs:
					_okapiTF_docs[_doc_id] += _tf / (_tf + 0.5 + (1.5 * _doc_len / avg_d_l))
				else:
					_okapiTF_docs[_doc_id] = _tf / (_tf + 0.5 + (1.5 * _doc_len / avg_d_l))

	with open('{}/okapi_tf'.format(_res_path), 'a') as _f:
		_i = 1
		for _doc_id in sorted(_okapiTF_docs, key=_okapiTF_docs.get, reverse=True):
			# <query-number> Q0 <docno> <rank> <score> Exp
			_f.write('{} Q0 {} {} {} Exp\n'.format(_q_id, doc_map[_doc_id][0], _i, _okapiTF_docs[_doc_id]))
			_i += 1
			if _i > 1000:
				break


def tf_idf(_res_path, _q_id, _query):
	global avg_d_l
	global doc_map
	_ttd = len(doc_map)
	_tf_idf_docs = {}
	for _t in _query:
		_t = stem(_t)
		_term_dict = get_term(_t)
		if len(_term_dict) > 0:
			_df = len(_term_dict['detail'])
			for _ in _term_dict['detail']:  # into one doc
				_doc_id = _['doc_id']
				_doc_len = _['doc_len']
				_tf = _['tf']
				if _doc_id in _tf_idf_docs:
					_tf_idf_docs[_doc_id] += _tf / (_tf + 0.5 + (1.5 * _doc_len / avg_d_l)) * math.log(_ttd/_df)
				else:
					_tf_idf_docs[_doc_id] = _tf / (_tf + 0.5 + (1.5 * _doc_len / avg_d_l)) * math.log(_ttd/_df)

	with open('{}/tf_idf'.format(_res_path), 'a') as _f:
		_i = 1
		for _doc_id in sorted(_tf_idf_docs, key=_tf_idf_docs.get, reverse=True):
			# <query-number> Q0 <docno> <rank> <score> Exp
			_f.write('{} Q0 {} {} {} Exp\n'.format(_q_id, doc_map[_doc_id][0], _i, _tf_idf_docs[_doc_id]))
			_i += 1
			if _i > 1000:
				break


def bm25(_res_path, _q_id, _query):
	global avg_d_l
	global doc_map
	_ttd = len(doc_map)
	_bm25_docs = {}
	for _t in _query:
		_t = stem(_t)
		_term_dict = get_term(_t)
		if len(_term_dict) > 0:
			_df = len(_term_dict['detail'])
			for _ in _term_dict['detail']:  # into one doc
				_doc_id = _['doc_id']
				_doc_len = _['doc_len']
				_tf = _['tf']
				if _doc_id in _bm25_docs:
					_bm25_docs[_doc_id] += 2.2 * _tf / (_tf + 1.2 * (0.25 + 0.75 * _doc_len / avg_d_l)) * math.log(
						(_ttd + 0.5) / (_df + 0.5))  # tf_q = 1
				else:
					_bm25_docs[_doc_id] = 2.2 * _tf / (_tf + 1.2 * (0.25 + 0.75 * _doc_len / avg_d_l)) * math.log(
						(_ttd + 0.5) / (_df + 0.5))  # tf_q = 1

	with open('{}/bm25'.format(_res_path), 'a') as _f:
		_i = 1
		for _doc_id in sorted(_bm25_docs, key=_bm25_docs.get, reverse=True):
			# <query-number> Q0 <docno> <rank> <score> Exp
			_f.write('{} Q0 {} {} {} Exp\n'.format(_q_id, doc_map[_doc_id][0], _i, _bm25_docs[_doc_id]))
			_i += 1
			if _i > 1000:
				break


def laplace(_res_path, _q_id, _query):
	global avg_d_l
	global doc_map
	global term_map
	_v = len(term_map)
	_laplace_docs = {}
	for _doc_id in doc_map.keys():    # d_id: [d_name, d_length]
		_ = math.log(doc_map[_doc_id][1] + _v)
		_laplace_docs[_doc_id] = -(_ * len(_query))
	for _t in _query:
		_t = stem(_t)
		_term_dict = get_term(_t)
		if len(_term_dict) > 0:
			for _ in _term_dict['detail']:  # into one doc
				_doc_id = _['doc_id']
				_tf = _['tf']
				_laplace_docs[_doc_id] += math.log(_tf + 1)

	with open('{}/laplace'.format(_res_path), 'a') as f:
		_i = 1
		for _doc_id in sorted(_laplace_docs, key=_laplace_docs.get, reverse=True):
			# <query-number> Q0 <docno> <rank> <score> Exp
			f.write('{} Q0 {} {} {} Exp\n'.format(_q_id, doc_map[_doc_id][0], _i, _laplace_docs[_doc_id]))
			_i += 1
			if _i > 1000:
				break


def j_mercer(_res_path, _q_id, _query, _lam):
	global avg_d_l
	global doc_map
	global term_map
	global tt_len
	_v = tt_len
	_j_mercer_docs = {}
	for _doc_id in doc_map.keys():  # d_id: [d_name, d_length]
		_j_mercer_docs[_doc_id] = [False, 0]
	for _t in _query:
		_t = stem(_t)
		_term_dict = get_term(_t)

		if len(_term_dict) > 0:
			_ttf = _term_dict['ttf']
			_p2 = (1 - _lam) * _ttf / _v
			print('{} = (1 - {}) * {} / {}'.format(_p2, _lam, _ttf, _v))
			print(math.log(_p2))
			for _ in _term_dict['detail']:  # into one doc
				_doc_id = _['doc_id']
				_doc_len = _['doc_len']
				_tf = _['tf']
				_j_mercer_docs[_doc_id][0] = True
				_j_mercer_docs[_doc_id][1] += math.log(_lam * _tf / _doc_len + _p2)
			for k, v in _j_mercer_docs.items():
				if v[0]:
					v[0] = False
				else:
					v[1] += math.log(_p2)

	with open('{}/j_mercer'.format(_res_path), 'a') as f:
		_i = 1
		for _doc_id in sorted(_j_mercer_docs, key=_j_mercer_docs.get, reverse=True):
			# <query-number> Q0 <docno> <rank> <score> Exp
			f.write('{} Q0 {} {} {} Exp\n'.format(_q_id, doc_map[_doc_id][0], _i, _j_mercer_docs[_doc_id][1]))
			_i += 1
			if _i > 1000:
				break


def proxim_term(_query):
	"""
	{doc_id: {'doc_len':l, term_id:{'tf': x, 'tp': [ , ]},
							term_id:{'tf': x, 'tp': [ , ]}, ...},
	doc_id: {'doc_len':l, term_id:{'tf': x, 'tp': [ , ]},
							term_id:{'tf': x, 'tp': [ , ]}, ...},
	...}
	:param _query:
	:return:
	"""
	_doc_term = {}
	for _t in _query:
		_t = stem(_t)
		print(_t)
		_term_dict = get_term(_t)
		if len(_term_dict) > 0:
			for _d in _term_dict['detail']:  # into one doc
				_doc_id = _d['doc_id']
				if _doc_id in _doc_term:
					pass
				else:
					_doc_term[_doc_id] = {}
					_doc_term[_doc_id]['doc_len'] = _d['doc_len']

				_doc_term[_doc_id][_t] = {}
				_doc_term[_doc_id][_t]['tf'] = _d['tf']
				_doc_term[_doc_id][_t]['tp'] = []
				for _tp in _d['tp']:
					_doc_term[_doc_id][_t]['tp'].append(int(_tp))
	return _doc_term

def min_span(_doc):
	_t_num = len(_doc)
	_idx = [0] * _t_num
	_idx_max = []
	_res = []
	for _ in range(_t_num):
		_idx_max.append(len(_doc[_]) - 1)
		_res.append(_doc[_][_idx[_]])
	_min_span = max(_res) - min(_res)
	for _i in range(max(_idx_max)):
		for _ in range(_t_num):
			if _idx[_] < _idx_max[_]:
				_idx[_] += 1
				try:
					_res[_] = _doc[_][_idx[_]]
				except:
					print('out of range')
					print(_, _idx[_])
				_span = max(_res) - min(_res)
				_min_span = _span if _span < _min_span else _min_span
	return _min_span

# (C - rangeOfWindow) * numOfContainTerms / (lengthOfDocument + V)
# C = 1500
#query = ['corrupt', 'official', 'governmental', 'jurisdiction']
def cal_proxim(_res_path, _q_id, _query):
	"""
	doc_id: {'doc_len':l, term_id:{'tf': x, 'tp': [ , ]},
							term_id:{'tf': x, 'tp': [ , ]}, ...}
	:param _query:
	:return:
	"""
	global doc_map
	global term_map
	_v = len(term_map)
	_proxim_docs = {}
	_doc_term = proxim_term(_query)
	pprint.pprint(_doc_term)
	print('end')
	for _d, _detail in _doc_term.items():
		_doc = []
		for _t in _query:
			if _t in _detail:
				_doc.append(_detail[_t]['tp'])
		if len(_doc) > 1:
			_rangeOfW = min_span(_doc)
			_numOfC_T = len(_query)  # problem here, len(_query) or len(_doc)
			_lengthOfD = _detail['doc_len']
			_proxim_docs[_d] = (1500 - _rangeOfW) * _numOfC_T / (_lengthOfD + _v)

	with open('{}/proxim'.format(_res_path), 'a') as f:
		_i = 1
		for _doc_id in sorted(_proxim_docs, key=_proxim_docs.get, reverse=True):
			# <query-number> Q0 <docno> <rank> <score> Exp
			f.write('{} Q0 {} {} {} Exp\n'.format(_q_id, doc_map[_doc_id][0], _i, _proxim_docs[_doc_id]))
			_i += 1
			if _i > 1000:
				break


# term_id:ttf|docno,tf,tp,tp,tp,...|docno,tf,tp,tp,tp,...|...

start_time = time.time()

file_dir = dirname(dirname(abspath('__file__')))
res_path = file_dir + '/medium_res'
cat_files = '{}/{}'.format(res_path, get_files_name(r'^CAT_all$', res_path)[0])
inv_files = '{}/{}'.format(res_path, get_files_name(r'^INV_all$', res_path)[0])

cat = load_cat(cat_files)
inv = open(inv_files, 'r')

# get term_map
term_map = {}
with open('{}/term_map'.format(res_path), 'r') as t_m:
	for _ in t_m:
		t_id, t = _.split()
		term_map[t] = int(t_id)


# get doc length and agerage length of docs
doc_map = {}  # d_id: [d_name, d_length]
tt_len = 0
with open('{}/doc_map'.format(res_path), 'r') as d_l:
	for _ in d_l:
		d_id, d_name, l = _.split('\t')
		d_id = int(d_id)
		l = int(l)
		doc_map[d_id] = [d_name, l]
		tt_len += l
avg_d_l = tt_len / len(doc_map)


def parse_query(_query_file):
	with open(_query_file, 'r', errors='ignore') as _queries:
		for _ in _queries:
			if _ is not '\n':  # remove the \n at the end of file
				_ = _.lower().split()
				_q_id = int(_[0].replace('.', ''))
				yield _q_id, _[1:]

for _ in os.listdir(res_path):
	if re.match(r"okapi_tf|tf_idf|bm25|laplace|j_mercer|proxim", _):
		print('remove {}'.format(_))
		os.remove('{}/{}'.format(res_path, _))

for q_id, query in parse_query('query_file.txt'):
	#okapi_tf(res_path, q_id, query)
	#tf_idf(res_path, q_id, query)
	#bm25(res_path, q_id, query)
	#laplace(res_path, q_id, query)
	#j_mercer(res_path, q_id, query, 0.3)
	cal_proxim(res_path, q_id, query)


inv.close()

print('--- {} seconds ---'.format(time.time() - start_time))
