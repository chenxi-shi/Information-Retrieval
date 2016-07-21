# // P (_links_graph) is the set of all pages; |P| = N (_links_graph.number_of_nodes())
# // S is the set of sink nodes, i.e., pages that have no out links
# // M(p) is the set of pages that link to page p
# // L(q) is the number of out-links from page q
# // d is the PageRank damping/teleportation factor; use d = 0.85 as is typical

import networkx as nx
import time
import pickle
from elasticsearch import Elasticsearch, TransportError
from elasticsearch.helpers import bulk, scan
from math import fabs


def read_wt2g_inlinks(_filename='wt2g_inlinks.txt'):
	_links_graph = nx.DiGraph()
	with open(_filename, 'r', errors='replace') as _wt2g:
		for _ in _wt2g:
			_ = _.strip().split()
			_node = _[0]
			for _inlink in _[1:]:
				_links_graph.add_edge(_inlink, _node)
	for _node in _links_graph.nodes_iter():
		_links_graph.node[_node]['in_links_count'] = len(_links_graph.in_edges(_node))
	return _links_graph


def load_dict(dictname):
	print(" Loading ",dictname, " ...")
	file_path = dictname
	file_open = open(file_path, 'rb')
	dict=pickle.load(file_open)
	file_open.close()
	print(" Loading ", dictname, "finished ! ")
	return dict


def read_3000_pages(inlinks_pickle, outlinks_pickle):
	_in_links_dict=load_dict(inlinks_pickle)
	_out_links_dict = load_dict(outlinks_pickle)

	_links_graph = nx.DiGraph()

	for _node, _in_links in _in_links_dict.items():
		_links_graph.add_node(_node, in_links_count=len(_in_links_dict[_node]))
		for _in_link in _in_links:
			_links_graph.add_edge(_in_link, _node)
	for _node, _out_links in _out_links_dict.items():
		for _out_link in _out_links:
			_links_graph.add_edge(_node, _out_link)

	return _links_graph


# used by converge_pagerank
def ditermine_converged(_PageRank, _new_PageRank, _exponent=11):
	for page in _PageRank.keys():
		if fabs(_PageRank[page] - _new_PageRank[page]) > 1 / 10**_exponent:   # this spent 25min to converged,
			return False
	return True


def sink_pages(_links_graph):
	_sink_pages = set()
	for page in _links_graph.nodes():  # calculate total sink PR
		if not _links_graph.out_edges(page):
			_sink_pages.add(page)
	return _sink_pages


def converge_pagerank(_links_graph, d=0.85):
	'''
	sinkPR is an important sign for converge,
	when the change of sinkPR less than 10^-_exponent,
	it is converged
	:param _links_graph:
	:param d:
	:return:
	'''
	_PageRank = {key: 1 / len(_links_graph) for key in _links_graph.nodes()}  # initial value to 1/N
	_num_of_node = _links_graph.number_of_nodes()
	_new_PageRank = {page: _PageRank[page] * 2 for page in _links_graph.nodes()}
	_sink_pages = sink_pages(_links_graph)

	x = 0
	while True:
		x += 1
		print('Round {}'.format(x))
		sinkPR = 0
		print('-' * 30, 'calculate total sink PR', '-' * 30)

		for page in _sink_pages:  # calculate total sink PR
			sinkPR += _PageRank[page]

		print('sinkPR {}'.format(sinkPR))
		print('-' * 30, 'finish calculate total sink PR', '-' * 30)
		print('-' * 30, 'calculate each PR', '-' * 30)
		for page in _links_graph.nodes():
			_new_PageRank[page] = (1 - d) / _num_of_node  # teleportation: chance of be randomly seleted
			_new_PageRank[page] += d * sinkPR / _num_of_node  # spread remaining sink PR evenly
			for _parent_page_edge in _links_graph.in_edges_iter(page):  # pages pointing to p
				_new_PageRank[page] += d * _PageRank[_parent_page_edge[0]] / len(
					_links_graph.out_edges(_parent_page_edge[0]))  # add share of PageRank from in-links
			#print('{} {} {}'.format(page, _PageRank[page], _new_PageRank[page]))
		print('-' * 30, 'finish calculate each PR', '-' * 30, '\n')

		if not ditermine_converged(_PageRank, _new_PageRank, _exponent=9):
			for page in _PageRank.keys():
				_PageRank[page] = _new_PageRank[page]
		else:
			break

	# for page in _PageRank.keys():
	# 	print('{} {}'.format(page, _PageRank[page]))
	print('-' * 30, 'CONVERGED!!!', '-' * 30)

	return _PageRank


def top_500_pagerank(_PageRank, links_graph):
	for _good_page in sorted(_PageRank, key=_PageRank.get, reverse=True)[:500]:
		yield _good_page, _PageRank[_good_page], links_graph.node[_good_page]['in_links_count']


def sum_PageRank(_PageRank):
	return sum(_PageRank.values())


if __name__ == '__main__':
	start_time = time.time()

	# # from wt2g
	# links_graph = read_wt2g_inlinks()

	# from hw3 dataset
	links_graph = read_3000_pages('chenxi_inlinks_new.cpkl', 'chenxi_outlinks_new.cpkl')

	PageRank = converge_pagerank(links_graph)
	sum_scores = sum_PageRank(PageRank)
	print('SUM of PageRank: {}\n\n'.format(sum_scores))
	# with open('top_500_wt2g', 'w', errors='replace') as top_500:
	with open('top_500_hw3', 'w', errors='replace') as top_500:
		top_500.write('SUM of PageRank: {}\n\n'.format(sum_scores))
		rank = 0
		for good_page, good_score, in_links_num in top_500_pagerank(PageRank, links_graph):
			rank += 1
			top_500.write('{}\t{}\t{}\t{}\n'.format(rank, good_page, good_score, in_links_num))

	print("--- {} seconds ---".format(time.time() - start_time))

