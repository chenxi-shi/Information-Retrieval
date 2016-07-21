from matplotlib import pyplot


def plot_interpolated_precision(query_id, interpolated_prec_list, rec_list):
	pyplot.plot(rec_list, interpolated_prec_list)
	pyplot.ylabel("Precision")
	pyplot.xlabel("Recall")
	pyplot.title(query_id)
	pyplot.axis((0,1,0,1))
	pyplot.savefig("%s.png" % query_id)
	pyplot.close()


def open_plot(_query_id, Precision_file, Recall_file):
	with open(Precision_file, 'r', errors='replace') as p:
		precision_list = []
		for _l in p:
			precision_list.append(_l.strip())
	with open(Recall_file, 'r', errors='replace') as r:
		recall_list = []
		for _l in r:
			recall_list.append(_l.strip())

	plot_interpolated_precision(_query_id, precision_list, recall_list)


open_plot('152601', 'plot_data/Precision_152601', 'plot_data/Recall_152601')
open_plot('152602', 'plot_data/Precision_152602', 'plot_data/Recall_152602')
open_plot('152603', 'plot_data/Precision_152603', 'plot_data/Recall_152603')


