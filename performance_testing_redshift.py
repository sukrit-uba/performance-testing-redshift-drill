#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 20 16:41:40 2017

@author: uba
"""

import psycopg2
import multiprocessing
import time

'''
select count(*) from logs_csv2;
select cip,count(*) from logs_csv2 group by cip;
select count(distinct(cip)) from logs_csv2;
select count(distinct(cuseragent)) from logs_csv2;
select cuseragent,count(*) from logs_csv2 group by cuseragent;
select cip from logs_csv2 where cip='189.24.49.170';
'''
'''
for _ in range(200):
    cur.execute("select count(*) from logs_csv2;")
    print(cur.fetchall())
'''

def run_query(i):
    start = time.time()
    conn = psycopg2.connect(
    host="logdataparsing.ci9todflq6tn.us-east-1.redshift.amazonaws.com",
    user="awsuser",
    port=5439,
    password="Monotype123",
    dbname="dataparsing")
    
    cur = conn.cursor()
    query = "select cip,count(*) from logs_csv2 group by cip;"
    cur.execute(query)
    #print(str(i),cur.fetchall())
    
    conn.commit()

    cur.close()
    conn.close()
    end = time.time()
    print(str(i),end-start)
    
   
def exec_query(c,query):
    c.execute(query)
    return cur.fetchall()
    
if __name__ == '__main__':
    processes = []
    master_start = time.time()
    for i in range(4):
        p = multiprocessing.Process(target=run_query, args=(i,))
        processes.append(p)
        p.start()
        
    for p in processes:
        p.join()
    
    master_end = time.time()
    print("Total Time Taken: ",str(master_end-master_start))
#    conn = psycopg2.connect(
#    host="logdataparsing.ci9todflq6tn.us-east-1.redshift.amazonaws.com",
#    user="awsuser",
#    port=5439,
#    password="Monotype123",
#    dbname="dataparsing")
#    
#    cur = conn.cursor()
#    query = "select count(*) from logs_csv2;"
#conn.commit()
#
#cur.close()
#conn.close()