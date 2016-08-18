from es_method import search_doc, all_doc_generator, create_hw8_part2_dataset, load_to_elasticsearch
from settings import *

class resultor(object):

	def __init__(self, _es, _source_index, _doc_type):

		self._es = _es
		self._source_index = _source_index
		self._doc_type = _doc_type

		self._doc_text_dict = {}

		self.doc_id_lst = []  # list of doc_id
		self.doc_text_lst = []  # list of text

		self.query_set = {85, 59, 56, 71, 64, 62, 93, 99, 58, 77, 54, 87, 94,
						  100, 89, 61, 95, 68, 57, 97, 98, 60, 80, 63, 91}
		# self.qrels_doc_dict = defaultdict(set) # dict.fromkeys(self.query_set, set())
		self.doc_query_lst = []

		resultor.train_data_dict(self)

		print("Created resultor instance.")
		# Display progress logs on stdout
		logging.basicConfig(level=logging.INFO,
							format='%(asctime)s %(levelname)s %(message)s')

	def train_data_dict(self):
		'''
		used by __init__
		:return:
		'''
		self._doc_text_dict = all_doc_generator(self._es, self._source_index, es_field, _my_type=doc_type, _doc_id_flg=False)

	def doc_content_extractor(self):
		for _doc_id, _ in self._doc_text_dict.items():
			if _["text"]:
				self.doc_id_lst.append(_doc_id)  # list of doc_id
				self.doc_text_lst.append(_["text"][0])  # list of text

		with open("doc_id_lst", "wb") as f:
			dump(self.doc_id_lst, f)

		print("Finished getting all doc text.")

	def do_lda(self):
		# def custom_tokenizer(doc):
		# 	return [stem(each.lower()) for each in re.findall(u'(?u)\\b\\w\\w+\\b', doc)]
		resultor.doc_content_extractor(self)

		t0 = time()
		self.cv = CountVectorizer(stop_words='english',
								  max_df=0.95,
								  min_df=2)

		self._doc_term_matrix = self.cv.fit_transform(self.doc_text_lst)
		print("done in {:.3f}".format(time() - t0))
		print("n_samples: {}, n_features: {}\n".format(self._doc_term_matrix.shape[0], self._doc_term_matrix.shape[1]))

		t0 = time()
		self.lda = LatentDirichletAllocation(n_topics=200,
											 max_iter=20,
		                                     learning_method='online',
											 learning_offset=50.,
											 random_state=1,
											 verbose=1)
		print('fit and transform lda')
		self._doc_term_matrix = self.lda.fit_transform(self._doc_term_matrix)
		print("done in {:.3f}".format(time() - t0))

		with open("lda_fitted", "wb") as f:
			dump(self._doc_term_matrix, f)

		print("Finished fitting")

	def score_to_prob(self):
		new_rel = []
		for score_list in self._doc_term_matrix:
			new_list = []
			total = sum(score_list)
			if total == 0:
				new_list = [float(1) / 200] * 200
				new_rel.append(new_list)
				continue
			for each in score_list:
				prob = float(each) / total
				new_list.append(prob)
			new_rel.append(new_list)
		self._doc_term_matrix = new_rel

	def do_kmean(self, file_lda):
		if file_lda:
			with open("lda_fitted", "rb") as f:
				self._doc_term_matrix = load(f)

		resultor.score_to_prob(self)

		t0 = time()
		self.km = KMeans(n_clusters=25,
						 n_init=20,
						 precompute_distances='auto',
						 n_jobs=4)
		print("Clustering sparse data with %s" % self.km)

		self._doc_cluster_matrix = self.km.fit_predict(self._doc_term_matrix)
		print("done in {:.3f}\n".format(time() - t0))

		with open("kmean_fitted", "wb") as f:
			dump(self._doc_cluster_matrix, f)

		print("Finished do kmeans")


	def restore_kmean_es(self, file_kmean):
		if file_kmean:
			with open("kmean_fitted", "rb") as f:
				self._doc_cluster_matrix = load(f)

		create_hw8_part2_dataset(self._es, "hw8_kmean_cluster_dataset", self._doc_type)

		for i in range(len(self._doc_cluster_matrix)):
			_source = {
				"kmean_cluster": int(self._doc_cluster_matrix[i])
			}
			_doc_id = self.doc_id_lst[i]


			load_to_elasticsearch(self._es, "hw8_kmean_cluster_dataset", self._doc_type,
								  _source=_source,
								  _doc_id=_doc_id)


	def restore_1831_qrel(self):
		with open("qrels.adhoc.51-100.AP89.txt", "r", errors="ignore") as _qrels:
			for _line in _qrels:
				_line = _line.strip().split()
				if int(_line[3]) > 0:
					_query = int(_line[0])
					if _query in self.query_set:
						_doc = _line[2]
						self.doc_query_lst.append((_doc, _query))

	def get_tpc_from_km(self, _idx):
		_tpc = self._doc_cluster_matrix[_idx]

		return int(_tpc)

	def evaluation(self, file_kmean):
		if file_kmean:
			with open("kmean_fitted", "rb") as f:
				self._doc_cluster_matrix = load(f)


		resultor.restore_1831_qrel(self)
		sq_sc_count = 0
		sq_dc_count = 0
		dq_sc_count = 0
		dq_dc_count = 0


		_doc_index_dict = {self.doc_id_lst[i]:i for i in range(len(self.doc_id_lst))}
		_doc_topic_dict = {}

		for i in range(len(self.doc_query_lst)):
			_doc1, _q1 = self.doc_query_lst[i]

			if _doc1 in _doc_topic_dict:
				_tpc1 = _doc_topic_dict[_doc1]
			else:
				_idx1 = _doc_index_dict[_doc1]
				_tpc1 = resultor.get_tpc_from_km(self, _idx1)
				_doc_topic_dict[_doc1] = _tpc1

			for _doc2, _q2 in self.doc_query_lst[i:]:

				if _doc2 in _doc_topic_dict:
					_tpc2 = _doc_topic_dict[_doc2]
				else:
					_idx2 = _doc_index_dict[_doc2]
					_tpc2 = resultor.get_tpc_from_km(self, _idx2)
					_doc_topic_dict[_doc2] = _tpc2

				if _q1 == _q2:  # sq
					if _tpc1 == _tpc2:  # sc
						sq_sc_count += 1
						# print(_tpc1)
					else:  # dc
						sq_dc_count += 1
				else:  # dq
					if _tpc1 == _tpc2:  # sc
						dq_sc_count += 1
						# print(_tpc1)
					else:  # df
						dq_dc_count += 1


		print("EVALUATION:")
		print("sq_sc_count {}".format(sq_sc_count))
		print("sq_dc_count {}".format(sq_dc_count))
		print("dq_sc_count {}".format(dq_sc_count))
		print("dq_dc_count {}".format(dq_dc_count))

		_right = sq_sc_count + dq_dc_count
		_total = sq_sc_count + sq_dc_count + dq_sc_count + dq_dc_count

		print("Accuracy {}".format((_right)/(_total)))

		print("Finished evaluation")



if __name__ == "__main__":
	resultor_ins = resultor(es, doc_index, doc_type)

	resultor_ins.do_lda()
	resultor_ins.do_kmean(file_lda=False)
	resultor_ins.restore_kmean_es(file_kmean=False)

	resultor_ins.evaluation(file_kmean=False)

