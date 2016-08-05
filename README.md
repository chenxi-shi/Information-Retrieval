# Information-Retrieval
Homework of my course "Information Retrieval", by Python 3.

 - Instructor: Virgil Pavlu
 - University: Northeastern University
 - Course: CS6200

1. Elasticsearch Index
 - index more than 80000 documents into elasticsearch
 - optimized index speed to around 15min

2. Documents Index
 - making my own "elasticsearch"
 - index data in both doc dimension, and term dimension
 - two kinds of dimension index increase the index efficiency. 

3. Web Crawler
 - topic: maritime accident
 - Breadth-first search to iterate all pages in early waves. 
 - topic module application for accurately checking the relevance of pages
 - in total 36000 pages, more than 50% is relevant to topic "maritime accident"
 - distinguish wanted pages by header content type before downloading it.
 - applied network session to restore cookies for fast and low-duty re-access.
 - sort domains according to last accessing time, so that multi threads can access different domains to speed up crawling
 - normalize href links in good method, to reduce page drop rate 

4. Web Graph Computation
 - applied pagerank and HITS to evaluate the page in whole page set
 - regard in & out links of pages as directed network graph
 - web graph computation is a kind of admitting of idea “Cream rises to the top”: 
 - good authority page can be referenced more and more, 
 - good hub page digs more and more good authority pages.

5. Web Interface Relevance Assessments
 - applied Tornado Server as a web server, which can be accessed remotely
 - server communicates with elasticsearch database for searching and extracting data
 - MongoDB restores page info to speed up web server
 - made python based html template to create search result page automatically and flexibility.
 - set log in permit to filter users
 - applied application layer info to transfer parameter between pages. 
 - after getting manual evaluation, apply query compute R-precision, Average Precision, nDCG, precision and recall and F1 to evaluate search result coming from page set.
 - drew precision & recall graphics for the visualized cooperation between search results distribution and page relevant true values.

6. Machine Learning for IR
 - with better understanding of elasticsearch, re-index the dataset, which set new analyzer with standard tokenizer, lowercase, and porter2 stemmer.
 - set nested mapping to restore features details
 - distinguish documents by different elasticsearch types
 - for a dataset with labeled data in it, split it by 80% for training, 20% for testing
 - tried different combination of feature to increase the performance of machine learning module
 - applied different machine learning modules including: Liner Regression, LogisticRegression, svm, svm rank


Some of the print screen of home work
 - [search_result.png](/3_Web_Crawler/search_result.png "Optional Title")
 - [page_showing.png](/5_Web_Interface_Relevance_Assessments/page_showing.png "Optional Title")
 - [search_interface.png](/5_Web_Interface_Relevance_Assessments/search_interface.png "Optional Title")
 - [search_results.png](/5_Web_Interface_Relevance_Assessments/search_results.png "Optional Title")
