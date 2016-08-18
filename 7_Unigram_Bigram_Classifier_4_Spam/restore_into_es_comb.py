import pprint
import re
from collections import defaultdict
from os.path import dirname, abspath, join
from random import sample

from elasticsearch import Elasticsearch

from resolve_email_files import get_all_info, resolve_features, parse_email_true_value
from es_methods import load_to_elasticsearch, create_dataset
import settings


def random_choice(_percentage, _dict):
	_sample_count = int(len(_dict) * _percentage)
	return sample(_dict.keys(), _sample_count)


def get_email_true_value(_true_value_file):
	_spam_split_dict = defaultdict(dict)
	_ham_split_dict = defaultdict(dict)
	with open(_true_value_file, "r", errors="replace", encoding='utf8') as f:
		for _line in f:
			email_true_value_dict = parse_email_true_value(_line, resource_path)
			if email_true_value_dict["spam"] == 1:
				_spam_split_dict[email_true_value_dict["doc_id"]] = {
					"path": email_true_value_dict["path"],
					"train": True
				}
			else:
				_ham_split_dict[email_true_value_dict["doc_id"]] = {
					"path": email_true_value_dict["path"],
					"train": True
				}
	_spam_test_lst = random_choice(0.2, _spam_split_dict)
	for _doc_id in _spam_test_lst:
		_spam_split_dict[_doc_id]["train"] = False

	_ham_test_lst = random_choice(0.2, _ham_split_dict)
	for _doc_id in _ham_test_lst:
		_ham_split_dict[_doc_id]["train"] = False
	return _spam_split_dict, _ham_split_dict

def clean_text(_text):
	removed_charset = re.compile(r"[^a-zA-Z0-9\\$%']")
	if _text:
		_text = re.split(removed_charset, _text.strip())
		_text = " ".join(list(filter(None, _text)))

	return _text


def update_es(_es_instance, _target_index, _train_type, _test_type, _file_dict, _spam=1):
	for _doc_id, _doc_detail in _file_dict.items():
		if _doc_id in settings.finished_doc_id:
			continue
		print("Doc {}".format(_doc_id))
		_text, email_from, email_msg_id, \
		email_to, email_sbj, email_reply, email_cc, \
		email_bcc, email_receives, \
		email_last_receive, email_sent_time = get_all_info(_doc_detail["path"])

		_weird_char, _weird_addr, _weird_sbj, \
		_weird_target, _weird_content, _weird_msg_id, \
		_servers_count, _span_time, \
		_wrong_time = resolve_features(_text, email_sbj, email_receives, email_sent_time,
									   email_last_receive,
									   email_from, email_msg_id, email_reply, email_to, email_cc,
									   email_bcc,
									   settings.greek_alphabet, settings.wired_char_set)

		# clean_behavior = Cleaning_Text(eng_words_file="English_Words")
		# text = clean_behavior.remove_un_eng_words(text)
		_text = clean_text(_text)

		_doc_dict = {
			"subject": email_sbj,
			"text": _text,
			"spam": _spam,
			"features": {
				"from": email_from,
				"to": email_to,
				"weird_char": _weird_char,
				"weird_addr": _weird_addr,
				"weird_sbj": _weird_sbj,
				"weird_target": _weird_target,
				"weird_content": _weird_content,
				"weird_msg_id": _weird_msg_id,
				"servers_count": _servers_count,
				"span_time": _span_time,
				"wrong_time": _wrong_time
			}
		}
		# print(_doc_dict)
		if _doc_detail["train"]:
			load_to_elasticsearch(_es_instance, _target_index, _train_type, _doc_dict, _doc_id)
		else:
			load_to_elasticsearch(_es_instance, _target_index, _test_type, _doc_dict, _doc_id)
		settings.finished_doc_id.add(_doc_id)
	# return _features_dict





if __name__ == "__main__":
	es = Elasticsearch()
	target_index = "hw7_dataset2"
	# test_index = "hw7_test"
	train_type = "for_train"
	test_type = "for_test"

	path = dirname(dirname(abspath("__file__")))
	resource_path = join(path, "trec07p")

	email_true_value_file = join(resource_path, "full", "index")
	spam_words_file = "spam_words.txt"

	create_dataset(es, target_index)
	settings.init()

	spam_split_dict, ham_split_dict = get_email_true_value(email_true_value_file)

	update_es(es, target_index, train_type, test_type, spam_split_dict, _spam=1)
	update_es(es, target_index, train_type, test_type, ham_split_dict, _spam=0)

