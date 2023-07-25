# VLDB修订的实验，比较fingerprint、sieve-1，sieve-10的效果
import os
import random
import sys
import re
import time

sys.path.append("../../../")
from index.gapListIndex.fingerprint import *
from index.learnedIndexDemo.IndexOnTheLake import LakeIndex, RootLakeIndex
from index.gapListIndex.reversedIndex import ReversedIndex
from index.gapListIndex.minMaxIndex import *

def getminmaxres(directory, column, prelog):
    minMax = MinMaxIndex(directory, [column])
    minMax.generateIndexes()
    workloads = [[] for i in range(4)]#分别是三个范围查询加一个点查询
    selecttype = ["range 1", "range 2", "range 3", "point"]
    workloadindex = 0
    f = open(prelog, "r+", encoding="UTF-8")
    for line in f:
        if "for search" in line:
            match = re.search(r"(-?\d+)-(-?\d+)", line.strip())
            workloads[workloadindex].append([int(match.group(1)), int(match.group(2))])
        elif "avg" in line:
            workloadindex += 1
    f.close()
    if column == "pagecount":#删除点查，重新构造
        workloads[3].clear()
        curpoint = 5677
        while curpoint < 186005677:
            workloads[3].append([curpoint, curpoint])
            curpoint += int(5e5)
    elif column == "lon":
        # 删除点查，重新构造
        workloads[3].clear()
        curpoint = -1800000000 + 7500000
        while curpoint <= 1800000000:
            workloads[3].append([curpoint, curpoint])
            curpoint += int(1e7)
    totallen, totaltime = 0, 0
    for selectindex in range(len(workloads)):
        for searchrange in workloads[selectindex]:
            curtime = time.time()
            minmaxrgs = minMax.range_search(searchrange, column)
            totaltime += (time.time() - curtime)
            totallen += len(minmaxrgs)
        avglen = totallen / len(workloads[selectindex])
        avgtime = totaltime / len(workloads[selectindex])
        totallen, totaltime = 0, 0
        print("for dir {}, column {}, select {}, minmax avg len is {}, avg search time is{}".format(
            directory, column, selecttype[selectindex], avglen, avgtime
        ))

if __name__=="__main__":
    directorys = ["/mydata/bigdata/data/tpcds_parquet_100/store_sales/",
                  "/mydata/bigdata/data/wikipedia/",
                  "/mydata/bigdata/data/openstreet/parquet/"]
    columns = ["ss_ticket_number", "pagecount", "lon"]
    prelogs = ["/proj/dst-PG0/VLDBR/cuckoo/sslog/store_sales0.csv.log",
               "/proj/dst-PG0/VLDBR/cuckoo/wikilog/wiki9.csv.log",
               "/proj/dst-PG0/VLDBR/cuckoo/openstreetlog/1.csv.log"]
    expindex = 0
    getminmaxres(directorys[expindex], columns[expindex], prelogs[expindex])
