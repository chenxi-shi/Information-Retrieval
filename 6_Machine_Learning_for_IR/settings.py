from elasticsearch import Elasticsearch


def init():
	global dateline_dict
	global label_dict
	global es
	global source_index
	global lam
	global norm_feature
	global linreg_query_lst
	global feature_selected
	global source_type
	global target_index
	dateline_dict = {}
	label_dict = {}
	es = Elasticsearch()
	source_index = 'maritimeaccidents'
	source_type = "document"
	target_index = "hw6_extra_ap_dataset"
	lam = 0.5
	norm_feature = ["okapi_tf", "tf_idf", "bm25", "laplace", "jm", "title_unique_term", "text_unique_term",
	                "text_term_appeared_in_query_len", "title_term_appeared_in_query_len", "doc_len"]
	linreg_query_lst = ["152602", "152603", # train_data
	                    "152601"]  # validation data
	feature_selected = ["okapi_tf", "tf_idf", "bm25", "laplace", "jm",
	                    # "doc_len"
	                    ]