import dlib
import pandas as pd

import settings

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
		data.relevant.append(dlib.vector(train_features[i]))
	elif train_true[i] == 0:
		data.nonrelevant.append(dlib.vector(train_features[i]))

trainer = dlib.svm_rank_trainer()
trainer.c = 10
rank = trainer.train(data)
print("Ranking score for a relevant vector:     {}".format(
    rank(data.relevant[0])))
print("Ranking score for a non-relevant vector: {}".format(
    rank(data.nonrelevant[0])))