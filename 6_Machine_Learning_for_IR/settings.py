from elasticsearch import Elasticsearch


def init():
	global dateline_dict
	global es
	global source_index
	global lam
	global norm_feature
	global linreg_query_lst
	global feature_selected
	dateline_dict = {}
	es = Elasticsearch()
	source_index = 'hw6_ap_dataset'
	lam = 0.5
	norm_feature = ["okapi_tf", "tf_idf", "bm25", "laplace", "jm", "title_unique_term", "text_unique_term",
	                "text_term_appeared_in_query_len", "title_term_appeared_in_query_len", "doc_len"]
	linreg_query_lst = [54, 58, 59, 60, 61, 62, 63, 68, 77, 80, 85, 87, 89, 91, 93, 94, 95, 97, 98, 100,  # train_data
	                    56, 57, 64, 71, 99]  # validation data
	feature_selected = [
		"okapi_tf", "tf_idf", "bm25", "laplace", "jm",
	    "doc_len",
	    "month", "day", "hour", "minuts",
		"dateline",
		"title_unique_term", "text_unique_term"
	    ]