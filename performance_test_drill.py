#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 23 10:54:53 2017

@author: uba
"""

import requests
import json 
import time
import multiprocessing
import threading 
import grequests

#curl -X POST -H "Content-Type: application/json" -d '{"queryType":"SQL", "query": "select * from dfs.`/Users/joe-user/apache-drill-1.4.0/sample-data/donuts.json` where name= \u0027Cake\u0027"}' http://localhost:8047/query.json

#r = requests.post('http://localhost:8047/query.json', data = {"queryType":"SQL", "query": "select columns[0] as `timestamp`, columns[1] as `time-taken`, columns[2] as `c-ip`, columns[3] as `filesize`, columns[4] as `s-ip`, columns[5] as `s-port`, columns[6] as `sc_status`, columns[7] as `sc-bytes`, columns[8] as `cs-method`, columns[9] as `cs-uri-stem`, columns[10] as `-`, columns[11] as `rs-duration`, columns[12] as `rs-bytes`, columns[13] as `c-referrer`, columns[14] as `c_user_agent`, columns[15] as `customer_id`, columns[16] as `x_ec_custom_1` from dfs.`/media/uba/9876-4C45/wac_1AC1_20140925_0078.log` limit 20"})
#print(r.json())

"""
select count(*) from dfs.tmp.`/stats/logs/*`
select c_ip, count(*) from dfs.tmp.`/stats/logs/*` group by c_ip
select count(distinct(c_ip)) from dfs.tmp.`/stats/logs/*`
select count(distinct(c_user_agent)) from dfs.tmp.`/stats/logs/*`
select c_user_agent, count(*) from dfs.tmp.`/stats/logs/*` group by c_user_agent
select c_ip from dfs.tmp.`/stats/logs/*` where c_ip='189.24.49.170'
"""

def run_query():
    start = time.time()
    url = "http://localhost:8047/query.json"
    #query = """select columns[0] as `timestamp`, columns[1] as `time-taken`, columns[2] as `c-ip`, columns[3] as `filesize`, columns[4] as `s-ip`, columns[5] as `s-port`, columns[6] as `sc_status`, columns[7] as `sc-bytes`, columns[8] as `cs-method`, columns[9] as `cs-uri-stem`, columns[10] as `-`, columns[11] as `rs-duration`, columns[12] as `rs-bytes`, columns[13] as `c-referrer`, columns[14] as `c_user_agent`, columns[15] as `customer_id`, columns[16] as `x_ec_custom_1` from dfs.`/media/uba/9876-4C45/wac_1AC1_20140925_0078.log` limit 1"""
    #query = """select count(*) from dfs.`/media/uba/9876-4C45/wac_1AC1_20140925_0078.log`"""    
    query = "select count(distinct(c_ip)) from dfs.tmp.`/stats/logs/*`"
    
    data = {"queryType" : "SQL", "query": query}
    data_json = json.dumps(data)
    headers = {'Content-type': 'application/json'}
    response = requests.post(url, data=data_json, headers=headers)
    print(response.json())
    end = time.time()
    print(end - start)
    return 

def time_it(r, **kwargs):
    print(time.time()-float(r.request.headers['start']))
    return
    
url = "http://localhost:8047/query.json"
query = "select c_user_agent, count(*) from dfs.tmp.`/stats/logs/*` group by c_user_agent"
#query = """select count(*) from dfs.`/media/uba/9876-4C45/wac_1AC1_20140925_0078.log`"""
data = {"queryType" : "SQL", "query": query}
data_json = json.dumps(data)
headers = {'Content-type': 'application/json','start': str(time.time())}#,'start':time.time()}
urls = [url for _ in range(20)]
#for url in urls:
#(grequests.post(POST_URL, data=fgp(a_url, j=True), headers={'Accept-Encoding':'none', 'Content-Type':'application/json'}) for a_url in urls)

myrequests = (grequests.post(url,data=data_json, headers=headers, hooks=dict(response=time_it)) for url in urls) 
responses = grequests.map(myrequests)
#for r in responses:
#    #pass
#    print(r.json())
    
#for _ in range(100):
#    start = time.time()
#    response = requests.post(url, data=data_json, headers=headers)
#    end = time.time()
#    #print(response.json())
#    print(end - start)

#if __name__ == '__main__':
#    jobs = []
#    threads = []
#    s = time.time()
#    for _ in range(10):
#        p = multiprocessing.Process(target=run_query)
#        #t = threading.Thread(target=run_query)
#        jobs.append(p)
#        #threads.append(t)
##        p.start()
##        #t.start()
##        p.join()
##        #t.join()
#        
##    for thread in threads:
##        thread.start()
##    for thread in threads:
##        thread.join()
#        
#    for process in jobs:
#        process.start()
#        process.join()
#    #for process in jobs:
#    #    process.join()
#        
#    e = time.time()
#    print("total: "+str(e - s))