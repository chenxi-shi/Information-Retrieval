import os
import re
import time
from os.path import dirname, abspath
from stemming.porter2 import stem
import math

def get_files_name(n_pattern, _dir):
	files = []
	for _ in os.listdir(_dir):
		if re.match(n_pattern, _):
			files.append(_)
	return files

def load_cat(_cat):
	_category = {}
	with open(_cat, 'r', errors='ignore') as _c:
		for _ in _c:
			_t_id, _b, _ofs = _.split()
			_category[int(_t_id)] = [int(_b), int(_ofs)]
	return _category

# get term_map
def load_term_map(_res_path):
	_term_map = {}
	with open('{}/term_map'.format(_res_path), 'r') as _t_m:
		for _ in _t_m:
			_t_id, _t = _.split()
			_term_map[_t] = int(_t_id)
	return _term_map

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
	#global doc_map
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
			#_a_doc['doc_len'] = doc_map[_a_doc['doc_id']][1]
			_a_doc['tf'] = int(_[1])
			_a_doc['tp'] = _[2:]
			# _term_dict[t_id]['detail'].append(a_doc)
			_term_dict['detail'].append(_a_doc)
	return _term_dict

start_time = time.time()
file_dir = dirname(dirname(abspath('__file__')))
res_path = file_dir + r'\medium_res'
cat_files = '{}/{}'.format(res_path, get_files_name(r'^CAT_all$', res_path)[0])
inv_files = '{}/{}'.format(res_path, get_files_name(r'^INV_all$', res_path)[0])
cat = load_cat(cat_files)
term_map = load_term_map(res_path)
inv = open(inv_files, 'r')
f = open('res.in.0.50.txt', 'w')
res = {}
with open('in.0.50.txt', 'r') as term_lst:
	for t in term_lst:
		t_s = stem(t.split()[0].lower())
		term_dict = get_term(t_s)
		if len(term_dict) > 0:
			res[t_s] = {'df': len(term_dict['detail']), 'ttf': int(term_dict['ttf'])}
		else:
			res[t_s] = {'df': 0, 'ttf': 0}
		f.write('{} {} {}\n'.format(t.split()[0], res[t_s]['df'], res[t_s]['ttf']))

f.close()
inv.close()

