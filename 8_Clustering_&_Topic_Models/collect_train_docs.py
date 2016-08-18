from es_method import search_doc
from settings import *

class resultor(object):

	def __init__(self, _es, _source_index, _doc_type):

		self._es = _es
		self._source_index = _source_index
		self._doc_type = _doc_type

		self.doc_id_lst = []
		self.doc_text_lst = []
		# self.query_set = {85, 59, 56, 71, 64, 62, 93, 99, 58, 77, 54, 87, 94,
		# 				  100, 89, 61, 95, 68, 57, 97, 98, 60, 80, 63, 91}

		self.query_set = {56}

		resultor.train_data_dict(self)
		print("Created resultor instance.")

	def train_data_dict(self):
		'''
		used by __init__
		:return:
		'''
		self._train_data_set = set()
		self._query_doc_dict = defaultdict(set)

		with open("tf_idf", "r", errors="ignore") as _tf_idf:
			for _line in _tf_idf:
				_line= _line.strip().split()
				_doc = _line[2]
				_query = int(_line[0])
				if _query == 56:
					self._train_data_set.add(_doc)

					self._query_doc_dict[_query].add(_doc)

		with open("qrels.adhoc.51-100.AP89.txt", "r", errors="ignore") as _qrels:
			for _line in _qrels:
				_line = _line.strip().split()
				if int(_line[3]) > 0:
					_query = _line[0]
					if _query in self.query_set:
						_doc = _line[2]
						self._train_data_set.add(_doc)
						self._query_doc_dict[_query].add(_doc)

		return self._train_data_set

	def doc_content_extractor(self):
		for _doc_id in self._train_data_set:
			_res = search_doc(self._es, self._source_index, self._doc_type, {"_id": _doc_id}, _fields_list=["text"])
			if _res:
				self.doc_id_lst.append(_doc_id)
				self.doc_text_lst.append(_res["fields"]["text"][0])

		print("Finished getting all doc text.")

	def fit(self):
		def custom_tokenizer(doc):
			return [stem(each.lower()) for each in re.findall(u'(?u)\\b\\w\\w+\\b', doc)]

		self.tf_vectorizer = CountVectorizer(stop_words='english', max_df=0.95, min_df=2, tokenizer=custom_tokenizer)

		t0 = time()
		_doc_term_matrix = self.tf_vectorizer.fit_transform(self.doc_text_lst)
		print("done in {:.3f}".format(time() - t0))

		print("Fitting LDA models with tf features, n_samples={} and n_features={}...".format(n_samples, n_features))
		self.lda = LatentDirichletAllocation(n_topics=20, max_iter=20,
		                                     learning_method='online', learning_offset=50., random_state=1)
		t0 = time()
		self.lda.fit(_doc_term_matrix)
		print("done in {:.3f}".format(time() - t0))

		print("\nTopics in LDA model:")
		tf_feature_names = self.tf_vectorizer.get_feature_names()
		resultor.print_top_words(self.lda, tf_feature_names, "top_words", n_top_words)

		print('transform lda')
		self.topic_prob = self.lda.transform(_doc_term_matrix)
		print("Total docs {}".format(len(self.doc_id_lst)))
		print("Docs in topic_prob {}".format(len(self.topic_prob)))
		print("Total topics {}".format(len(self.topic_prob[0])))
		with open("top_topics", 'w', errors="ignore") as tt:
			for i in range(len(self.topic_prob)):
				_doc_id = self.doc_id_lst[i]
				_topic_prob_dict = {"topic{}".format(j):self.topic_prob[i][j] for j in range(len(self.topic_prob[i]))}
				_first3 = sorted(_topic_prob_dict, key=_topic_prob_dict.get, reverse=True)[:3]
				tt.write('{}: {}({:.3f})\t{}({:.3f})\t{}({:.3f})'.
						 format(_doc_id,
								_first3[0], _topic_prob_dict[_first3[0]],
								_first3[1], _topic_prob_dict[_first3[1]],
								_first3[2], _topic_prob_dict[_first3[2]]))

				for _query, _doc_set in self._query_doc_dict.items():
					if _doc_id in _doc_set:
						_doc_set.discard(_doc_id)
						tt.write("\t{}".format(_query))
				tt.write("\n")


	@staticmethod
	def print_top_words(model, feature_names, _file_name, n_top_words=20):

		with open(_file_name,'w') as fp:
			for topic_idx, topic in enumerate(model.components_):
				fp.write("Topic {}:\t".format(topic_idx))
				fp.write("\t".join([feature_names[i] for i in topic.argsort()[:-n_top_words - 1:-1]]))

				fp.write('\n')


if __name__ == "__main__":
	resultor_ins = resultor(es, doc_index, doc_type)
	resultor_ins.doc_content_extractor()
	resultor_ins.fit()

