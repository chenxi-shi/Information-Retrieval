from collections import defaultdict

import dlib
import pandas as pd

import settings

settings.init()
train_data = pd.read_csv('training_features.csv', index_col=0, encoding="ISO-8859-1")
query_id_train = train_data["query_id"].tolist()
doc_id_train = train_data["doc_id"].tolist()
train_features = train_data[settings.feature_selected]
# train_true = list(train_data["label"])
train_true = train_data["label"].tolist()

# testing
test_data = pd.read_csv('test_features.csv', index_col=0, encoding="ISO-8859-1")
query_id_test = test_data["query_id"].tolist()
doc_id_test = test_data["doc_id"].tolist()
test_features = test_data[settings.feature_selected]
test_true = test_data["label"]

data = dlib.ranking_pair()
for i in range(len(train_true)):
	if train_true[i] == 1:
		data.relevant.append(dlib.vector(train_features.iloc[i].tolist()))
	elif train_true[i] == 0:
		data.nonrelevant.append(dlib.vector(train_features.iloc[i].tolist()))

trainer = dlib.svm_rank_trainer()
trainer.c = 10
rank = trainer.train(data)

with open("svm_rank_test.txt", 'w', errors='replace') as _t:
	sorted_dict = defaultdict(list)
	for i in range(len(doc_id_test)):
		_features = dlib.vector(test_features.iloc[i].tolist())
		sorted_dict[query_id_test[i]].append([doc_id_test[i], i+1, rank(_features)])

	for q_id, docs in sorted_dict.items():
		docs.sort(key=lambda x: x[2], reverse=True)
		_count = 0
		for doc in docs:
			_count += 1
			_t.write('{} Q0 {} {} {} Exp\n'.format(q_id, doc[0], doc[1], doc[2]))

with open("svm_rank_train.txt", 'w', errors='replace') as _t:
	sorted_dict = defaultdict(list)
	for i in range(len(doc_id_train)):
		_features = dlib.vector(train_features.iloc[i].tolist())
		sorted_dict[query_id_train[i]].append([doc_id_train[i], i+1, rank(_features)])

	for q_id, docs in sorted_dict.items():
		docs.sort(key=lambda x: x[2], reverse=True)
		_count = 0
		for doc in docs:
			_count += 1
			_t.write('{} Q0 {} {} {} Exp\n'.format(q_id, doc[0], doc[1], doc[2]))



