import os
from os.path import dirname, abspath
import tempfile
import re
import time


def load_cat(cat):
	category = {}
	with open(cat, 'r', errors='ignore') as c:
		for line in c:
			t, b, ofs = line.split()
			category[int(t)] = [int(b), int(ofs)]
	return category


def load_term(inv_term):
	docs = inv_term.split(':', 1)
	t_id = int(docs[0])
	docs = docs[1].split('|')  # doc details of term in inv1
	ttf = int(docs[0])
	for _ in range(1, len(docs)):
		docs[_] = docs[_].split(',', 2)
	return t_id, ttf, docs[1:]


def write_cat(file, cat):
	with open(file, 'w') as f:
		for k, v in cat.items():
			f.write('{}\t{}\t{}\n'.format(k, v[0], v[1]))


def comb_2(cat1, cat2, inv1, inv2):
	print('start combine inv1 & inv2')
	directory = dirname(abspath('__file__'))
	# read cat into dict
	category1 = load_cat(cat1)
	category2 = load_cat(cat2)
	i1 = open(inv1, 'r', errors='ignore')
	i2 = open(inv2, 'r', errors='ignore')
	with tempfile.NamedTemporaryFile('w', dir=directory, delete=False) as tf:
		for t in category1.keys():  # one term
			i1.seek(category1[t][0], 0)
			inv1_term = i1.read(category1[t][1])  # term in inv1
			if t in category2:  # term also in inv2
				i2.seek(category2[t][0], 0)
				inv2_term = i2.read(category2[t][1])  # term in inv2
				# term_id:ttf|doc_id,tf,tp,tp,tp,...|doc_id,tf,tp,tp,tp,...|...
				t1, ttf1, docs1 = load_term(inv1_term)
				t2, ttf2, docs2 = load_term(inv2_term)
				if t != t1:
					print('INV1 {}, CAT {} is not correspond.'.format(inv1_term.split(':', 1)[0], t))
				elif t != t2:
					print('INV2 {}, CAT {} is not correspond.'.format(inv2_term.split(':', 1)[0], t))
				docs1.extend(docs2)  # doc details of term in two invs
				docs1 = sorted(docs1, key=lambda d: int(d[1]), reverse=True)
				# term_id:ttf|doc_id,tf,tp,tp,tp,...|doc_id,tf,tp,tp,tp,...|...
				t_b = tf.tell()
				tf.write('{}:{}'.format(t, ttf1 + ttf2))
				for _ in docs1:
					tf.write('|{},{},{}'.format(_[0], _[1], _[2]))
				category2.pop(t)  # remove term already parsed in cat2
			else:  # for term in cat1 not in cat2
				t_b = tf.tell()
				tf.write(inv1_term)
			t_e = tf.tell()
			# tf.write('\n')
			category1[t] = [t_b, t_e - t_b]

		for t in category2.keys():  # for term in cat1 not in cat2
			i2.seek(category2[t][0], 0)
			inv2_term = i2.read(category2[t][1])  # term in inv2
			t_b = tf.tell()
			tf.write(inv2_term)
			t_e = tf.tell()
			# tf.write('\n')
			category1[t] = [t_b, t_e - t_b]
		i1.close()
		i2.close()
		os.remove('{}'.format(cat1))
		os.remove('{}'.format(cat2))
		os.remove('{}'.format(inv1))
		os.remove('{}'.format(inv2))
		tempname = tf.name
	os.rename(tempname, inv2)
	write_cat(cat2, category1)
	print('finished combing, product {}'.format(inv2))


def get_files_name(n_pattern, _dir):
	files = []
	for _ in os.listdir(_dir):
		if re.match(n_pattern, _):
			files.append(_)
	return files


def one_comb_round(_dir):
	cat_files = get_files_name(r'^CAT_[0-9]+$', _dir)
	print(cat_files)
	inv_files = get_files_name(r'^INV_[0-9]+$', _dir)
	print(inv_files)
	for _ in range(0, len(cat_files), 2):
		c1 = '{}/{}'.format(_dir, cat_files[_])
		print(c1)
		try:
			c2 = '{}/{}'.format(_dir, cat_files[_ + 1])
			print(c2)
		except:
			break
		else:
			i1 = '{}/{}'.format(_dir, inv_files[_])
			i2 = '{}/{}'.format(_dir, inv_files[_ + 1])
			comb_2(c1, c2, i1, i2)


file_dir = dirname(dirname(abspath('__file__')))
res_path = file_dir + r'\medium_res'
start_time = time.time()
while len(get_files_name(r'^CAT_[0-9]+$', res_path)) > 1:
	one_comb_round(res_path)

os.rename(r'{}\{}'.format(res_path, get_files_name(r'^CAT_[0-9]+$', res_path)[0]), r'{}\CAT_all'.format(res_path))
os.rename(r'{}\{}'.format(res_path, get_files_name(r'^INV_[0-9]+$', res_path)[0]), r'{}\INV_all'.format(res_path))

print('--- {} seconds ---'.format(time.time() - start_time))

# vcomb_2('medium_res/CAT_11000', 'medium_res/CAT_12000', 'medium_res/INV_11000', 'medium_res/INV_12000')
