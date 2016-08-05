import math
from matplotlib import pyplot
#hw1 settings
CUTOFFS = (5, 10, 15, 20, 30, 100, 200, 500, 1000)
DOCSPERQUERY = 1000

RECALLS = (0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)
#CUTOFFS = (5, 10, 20, 50, 100)
#DOCSPERQUERY = 200
DETAIL = False
RELEVENTTHRESHOID = 1


def merge_crawled_qrel():
    qrel_file1 = 'sorted_Chenxi_Shi.txt'
    qrel_file2 = 'sorted_Ran_Qiao.txt'
    qrel_file3 = 'sorted_Yu_Wen.txt'
    write_fp = open('crawled_qrel.txt','w')
    dic = {}
    for each in [qrel_file1,qrel_file2,qrel_file3]:
        fp = open(each,'r')
        for line in fp.readlines():
            items = line.rstrip().split('\t')
            qid = items[0]
            did = items[2]
            score = items[3]
            if not dic.has_key(qid):
                dic[qid] ={}
            if not dic[qid].has_key(did):
                dic[qid][did] = []
            dic[qid][did].append(int(score))
        fp.close()
    for qid in dic:
        for did in dic[qid]:
            score_list = dic[qid][did]
            merge_score = max([(0,score_list.count(0)),(1,score_list.count(1)),(2,score_list.count(2))],key = lambda x:x[1])[0]
            if score_list.count(merge_score)==1:
                merge_score=1
            write_fp.write('\t'.join([qid, "merged",did,str(merge_score)])+'\n')
    write_fp.close()

def merge_ranked_list():
    ranked_file1 = 'search_152601.txt'
    ranked_file2 = 'search_152602.txt'
    ranked_file3 = 'search_152603.txt'
    write_fp = open('crawled_ranked_lists.txt','w')
    dic = {}
    for each in [ranked_file1,ranked_file2,ranked_file3]:
        fp = open(each,'r')
        qid = each.rstrip('.txt').split('_')[1]
        score = 210
        count = 0
        for line in fp.readlines():
            items = line.rstrip().split('\t')
            count += 1
            did = items[0]
            write_fp.write(' '.join([qid, "merged", did, str(count),str(score)]) + '\n')
            score -=1
        fp.close()
    write_fp.close()

def read_qrel(qrel_file):
    qrel_dict = {}
    for line in open(qrel_file, "r"):
        query_id, accessor_id, doc_id, rel_score = line.rstrip().split()
        rel_score = float(rel_score)
        if not qrel_dict.has_key(query_id):
            qrel_dict[query_id] = {}
        if not qrel_dict[query_id].has_key(doc_id):
            qrel_dict[query_id][doc_id] = 0
        qrel_dict[query_id][doc_id] += rel_score

    true_relevent_count = {}
    for query_id in qrel_dict:
        true_relevent_count[query_id] = 0
        for doc_id in qrel_dict[query_id]:
            if qrel_dict[query_id][doc_id] >= RELEVENTTHRESHOID:
                true_relevent_count[query_id] +=1
    return qrel_dict, true_relevent_count


def read_ranked_list(ranked_list):
    rank_dict = {}
    for line in open(ranked_list, "r"):
        items = line.rstrip().split()
        query_id = items[0]
        doc_id = items[2]
        score = items[4]
        if not rank_dict.has_key(query_id):
            rank_dict[query_id] = {}
        rank_dict[query_id][doc_id] = float(score)
    return rank_dict

def compare_query_result(query_id, qrel_dict, ranked_list_dict ,num_all_rel):
    prec_list = [0] * DOCSPERQUERY
    rec_list = [0] * DOCSPERQUERY
    num_ret = 0
    num_rel_ret = 0
    current_ranked_list = sorted([(k, v) for k, v in ranked_list_dict[query_id].iteritems()], key=lambda i: i[1],
                                 reverse=True)
    sum_prec = 0
    for docid, score in current_ranked_list:
        num_ret += 1
        if qrel_dict[query_id].has_key(docid) and qrel_dict[query_id][docid]>=RELEVENTTHRESHOID :
            num_rel_ret += 1
            sum_prec +=  float( num_rel_ret) / num_ret


        prec_list[num_ret-1] = float(num_rel_ret)/num_ret
        rec_list[num_ret-1] = float(num_rel_ret)/num_all_rel
    return prec_list,rec_list ,num_ret, num_rel_ret,sum_prec

def get_prec_f1_at_cutoffs(prec_list,rec_list):
    prec_at_cutoffs = []
    f1_at_cutoffs = []
    recall_at_cutoffs = []
    for i in range(0, len(CUTOFFS)):
        prec, rec = prec_list[CUTOFFS[i] - 1], rec_list[CUTOFFS[i] - 1]
        prec_at_cutoffs.append(prec)
        recall_at_cutoffs.append(rec)
        if prec == 0 and rec == 0:
            f1 = 0
        else:
            f1 = 2 * prec * rec / (prec + rec)
        f1_at_cutoffs.append(f1)
    return prec_at_cutoffs,f1_at_cutoffs,recall_at_cutoffs

def get_interp_prec_at_recalls(prec_list,rec_list):
    interpolated_list = []
    max_prec = -1
    for i in range(len(prec_list)-1,-1,-1):
        max_prec = max(max_prec, prec_list[i])
        interpolated_list.append(max_prec)
    interpolated_list.reverse()
    prec_at_recalls = []
    i = 0
    for recall in RECALLS:
        while i < DOCSPERQUERY and rec_list[i] < recall: i += 1
        rec = interpolated_list[i] if i < DOCSPERQUERY else 0
        prec_at_recalls.append(rec)
    return prec_at_recalls, interpolated_list

def dcg(relevent_score_list, k):
    res = relevent_score_list[0]
    for i in range(1, k):
        res += float(relevent_score_list[i]) / math.log(i + 1, 2)
    return res

def get_current_query_truth_list(query_id, qrel_dict, ranked_list_dict):
    query_truth_list = []
    current_ranked_list = sorted([(k, v) for k, v in ranked_list_dict[query_id].iteritems()], key=lambda i: i[1],
                                 reverse=True)

    for docid, score in current_ranked_list:
        if qrel_dict[query_id].has_key(docid):
            query_truth_list.append(qrel_dict[query_id][docid])
        else:
            query_truth_list.append(0)
    return query_truth_list


def evaluate(qrel_dict, ranked_list_dict, true_relevent_count):
    query_count = len(ranked_list_dict)
    total_num_ret = 0
    total_num_rel = 0
    total_num_rel_ret = 0
    sum_prec_at_cutoffs = [0] * len(CUTOFFS)
    sum_recall_at_cutoffs = [0] * len(CUTOFFS)
    sum_f1_at_cutoffs = [0] * len(CUTOFFS)
    sum_prec_at_recalls = [0] * len(RECALLS)

    sum_avg_prec = 0
    sum_r_prec = 0
    sum_ndcg = 0

    for query_id in ranked_list_dict:
        if true_relevent_count[query_id] == 0:
            print 'warning: query without positive label'
            continue
        num_rel = true_relevent_count[query_id]

        #get all lists and counts we need
        prec_list, rec_list, num_ret, num_rel_ret, sum_prec = compare_query_result(query_id, qrel_dict, ranked_list_dict,num_rel)
        prec_at_cutoffs, f1_at_cutoffs,recall_at_cutoffs = get_prec_f1_at_cutoffs(prec_list,rec_list)
        prec_at_recalls,interpolated_prec_list = get_interp_prec_at_recalls(prec_list,rec_list)

        #calculate statistics

        avg_prec = sum_prec/ num_rel

        if num_rel > num_ret:
            rp = float(num_rel_ret) / num_rel
        else:
            int_num_rel = int(num_rel)  # Integer part.
            frac_num_rel = num_rel-int_num_rel  # Fractional part.
            if frac_num_rel>0:
                rp = (1 - frac_num_rel) * prec_list[int_num_rel-1] +frac_num_rel * prec_list[int_num_rel]
            else:
                rp = prec_list[int_num_rel-1]


        current_query_truth_list = get_current_query_truth_list(query_id, qrel_dict, ranked_list_dict)
        dcg_nemerator = dcg(current_query_truth_list, num_ret)
        dcg_denominator = dcg(sorted(current_query_truth_list, reverse=True), num_ret)
        if dcg_denominator==0:
            ndcg = 0
        else:
            ndcg = float(dcg_nemerator)/ dcg_denominator

        #sum statistics to total data
        sum_r_prec += rp
        sum_avg_prec += avg_prec
        sum_ndcg += ndcg
        total_num_ret += num_ret
        total_num_rel  += num_rel
        total_num_rel_ret += num_rel_ret
        for i in range(0,len(CUTOFFS)):
            sum_prec_at_cutoffs[i] += prec_at_cutoffs[i]
            sum_f1_at_cutoffs[i] += f1_at_cutoffs[i]
            sum_recall_at_cutoffs[i] += recall_at_cutoffs[i]
        for i in range(0, len(RECALLS)):
            sum_prec_at_recalls[i] += prec_at_recalls[i]
        non_interp_list_plot= [prec_list[0]]
        prec_list_plot = [interpolated_prec_list[0]]
        recall_list_plot = [rec_list[0]]
        for i in range(1,len(rec_list)):
            if rec_list[i]>rec_list[i-1]:
                recall_list_plot.append(rec_list[i])
                prec_list_plot.append(interpolated_prec_list[i])
                non_interp_list_plot.append(prec_list[i])

        #plot_interpolated_precision(query_id, interpolated_prec_list,rec_list)
        plot_interpolated_precision(query_id, prec_list_plot, recall_list_plot)
        plot_interpolated_precision(query_id+'_noninterpolated', non_interp_list_plot, recall_list_plot)

        if DETAIL:
            print_stat(query_id, num_ret, num_rel, num_rel_ret, prec_at_recalls,
                       avg_prec, prec_at_cutoffs, recall_at_cutoffs,rp, f1_at_cutoffs, ndcg)

    avg_prec_at_cutoffs = [sum_prec_cutoff / query_count for sum_prec_cutoff in sum_prec_at_cutoffs]
    avg_recall_at_cutoffs = [sum_recall_cutoff / query_count for sum_recall_cutoff in sum_recall_at_cutoffs]
    avg_prec_at_recalls = [sum_prec_recall / query_count for sum_prec_recall in sum_prec_at_recalls]
    avg_f1_at_cutoffs = [sum_f1 / query_count for sum_f1 in sum_f1_at_cutoffs]

    mean_avg_prec = sum_avg_prec / query_count
    avg_r_prec = sum_r_prec / query_count
    avg_ndcg = sum_ndcg / query_count

    print_stat(query_count, total_num_ret, total_num_rel, total_num_rel_ret, avg_prec_at_recalls,
               mean_avg_prec, avg_prec_at_cutoffs, avg_recall_at_cutoffs, avg_r_prec, avg_f1_at_cutoffs, avg_ndcg)




def print_stat(num_queries, total_num_ret, total_num_rel, total_num_rel_ret, avg_prec_at_recalls,
               mean_avg_prec, avg_prec_at_cutoffs, avg_recall_at_cutoffs,avg_r_prec , f1_at_cutoffs, ndcg):
    print "Queryid (Num):    %5d" % int(num_queries)
    print "Total number of documents over all queries"
    print "Retrieved:    %5d" % total_num_ret
    print "Relevant:     %5d" % total_num_rel
    print "Rel_ret:      %5d" % total_num_rel_ret
    print ""

    print "Interpolated Recall - Precision Averages:"
    for i in range(len(RECALLS)):
        print "at %.2f       %.4f" % (RECALLS[i], avg_prec_at_recalls[i])
    print "Average precision (non-interpolated) for all rel docs(averaged over queries)"
    print "%.4f" % mean_avg_prec
    print ""

    print "Precision:"
    for i in range(len(CUTOFFS)):
        print "at %d       %.4f" % (CUTOFFS[i], avg_prec_at_cutoffs[i])
    print "R-Precision (precision after R (= num_rel for a query) docs retrieved):"
    print "Exact:        %.4f" % avg_r_prec
    print ""

    print "Recall:"
    for i in range(len(CUTOFFS)):
        print "at %d       %.4f" % (CUTOFFS[i], avg_recall_at_cutoffs[i])
    print "nDCG:        %.4f" % ndcg
    print ""

    print "F1 measure:"
    for i in range(len(CUTOFFS)):
        print "at %d       %.4f" % (CUTOFFS[i], f1_at_cutoffs[i])



def plot_interpolated_precision(query_id, interpolated_prec_list,rec_list):
    pyplot.plot(rec_list, interpolated_prec_list)
    pyplot.ylabel("Precision")
    pyplot.xlabel("Recall")
    pyplot.title(query_id)
    pyplot.axis((0,1,0,1))
    pyplot.savefig("%s.png" % query_id)
    pyplot.close()

def prase_hw5_test_file():
    file = 'Trec-Text-HW5.txt'
    rank_dict = {}
    for line in open(file, "r"):
        items = line.rstrip().split()
        query_id = items[0]
        doc_id = items[2]
        score = items[4]
        if not rank_dict.has_key(query_id):
            rank_dict[query_id] = {}
        rank_dict[query_id][doc_id] = float(score)
    write_fp = open('parsed_test.txt','w')
    for qid in rank_dict:
        sorted_list=sorted([(docid,score) for docid,score in rank_dict[qid].items()], key = lambda x:x[1], reverse=True)[:1000]
        for each in sorted_list:
            write_fp.write('\t'.join([qid,'something',each[0],'something',str(each[1]),'exp'])+'\n')
    write_fp.close()


if __name__ == '__main__':
    # if len(sys.argv) >= 4 and sys.argv[1]=='-q':
    #     DETAIL = True
    #     qrel_file, ranked_list_file = sys.argv[2], sys.argv[3]
    # else:
    #     qrel_file, ranked_list_file = sys.argv[1], sys.argv[2]
    DETAIL = False
    #prase_hw5_test_file()

    # qrel_file = 'qrels.adhoc.51-100.AP89.txt'
    # ranked_list_file = 'parsed_test.txt'
    # qrel_dict, true_rel_count = read_qrel(qrel_file)
    # ranked_list = read_ranked_list(ranked_list_file)
    # evaluate(qrel_dict, ranked_list, true_rel_count)

    #qrel_file = 'crawled_qrel.txt'
    qrel_file = 'train_true_values.txt'
    ranked_list_file = 'lin_train_predict.txt'
    #ranked_list_file = 'search_results.txt'
    qrel_dict, true_rel_count = read_qrel(qrel_file)
    ranked_list = read_ranked_list(ranked_list_file)
    evaluate(qrel_dict, ranked_list, true_rel_count)


