# Information-Retrieval
Homeworks of my course "Information Retrieval", by Python 3.4.

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
 - applied Breadth-first search to iterate all pages in early wave. 
 - applied topic module to accurately check the relevance of pages.
 - distinguish page by header content type before downloading it.
 - applied network session to restore cookies for fast re-access.
 - sort domains with according to last accessing time, so that I can use multi thread access different domains
 - normalize href links in good method, to reduce page drop rate 

4. Web Graph Computation
 - applied pagerank and HITS to evaluate the page in whole page set
 - regard in & out links of pages as directed network graph
 - web graph computation is a kind of admitting of idea “Cream rises to the top”: 
good authority page can be referenced more and more, 
good hub page digs more and more good authority pages.

5. Web Interface Relevance Assessments
 - applied Tornado Server as a web server.
 - web server can be accessed remotely
 - server communicates with elasticsearch database for searching and extracting data
 - MongoDB is applied to restore page info
 - made python based html template to create search result page automatically and flexibility.
 - set log in permit to filter user
 -
