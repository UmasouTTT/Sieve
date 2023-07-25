# VLDB修订的实验，比较fingerprint、sieve-1，sieve-10的效果
import os
import random
import sys
import re

sys.path.append("../../../")
from index.gapListIndex.fingerprint import *
from index.learnedIndexDemo.IndexOnTheLake import LakeIndex, RootLakeIndex
from index.gapListIndex.reversedIndex import ReversedIndex

# 那此前的查询”prelogfile“作为查询的输入
def compareCurDataset(datasetdir, column, prelogfile):
    args.partition_num = 1000
    sieve_0 = RootLakeIndex(datasetdir, [column])
    sieve_0.generateIndexes()
    args.partition_num = 10000
    args.sieve_gap_percent = 0.0001
    sieve_1 = RootLakeIndex(datasetdir, [column])
    sieve_1.generateIndexes()
    args.partition_num = 100000
    args.sieve_gap_percent = 0.00001
    sieve_10 = RootLakeIndex(datasetdir, [column])
    sieve_10.generateIndexes()
    f = open(prelogfile, "r+", encoding="UTF-8")
    search_blks = [0] * 5
    serch_times = [0] * 5
    search_exp_num, curselect = 0, ""
    for line in f:
        if "selectivity" in line or "point search start" in line:
            if search_exp_num != 0:
                print(
                    "for dataset {}, column {}, select {}, fingerprint-sieve1-sieve10 avg blks is {}, search time is {}".format(
                        datasetdir, column, curselect, [v / search_exp_num for v in search_blks],
                        [v / search_exp_num for v in serch_times]
                    ))
                search_blks = [0] * 5
                serch_times = [0] * 5
                search_exp_num = 0
            curselect = line.strip().split()[-2]
            print(line)
            if "point search start" in line:
                break
        elif "for search" in line:
            match = re.search(r"(-?\d+)-(-?\d+)", line.strip())
            searchrange = [int(match.group(1)), int(match.group(2))]
            search_exp_num += 1
            curtime = time.time()
            fingerprintans = set()
            serch_times[0] += time.time() - curtime
            search_blks[0] += len(fingerprintans)
            curtime = time.time()
            sieve_0_ans = sieve_0.range_search(searchrange, column)
            serch_times[1] += time.time() - curtime
            search_blks[1] += len(sieve_0_ans)
            curtime = time.time()
            sieve_1_ans = sieve_1.range_search(searchrange, column)
            serch_times[2] += time.time() - curtime
            search_blks[2] += len(sieve_1_ans)
            curtime = time.time()
            sieve_10_ans = sieve_10.range_search(searchrange, column)
            serch_times[3] += time.time() - curtime
            search_blks[3] += len(sieve_10_ans)
            curtime = time.time()
            fitingans = set()
            serch_times[4] += time.time() - curtime
            search_blks[4] += len(fitingans)
            print("for range {}, fingerprint res is {}, sieve0 res is {}, sieve1 res is {},"
                  "sieve10 res is {}, fiting tree res is {}".format(searchrange, len(fingerprintans),
                                                                    len(sieve_0_ans), len(sieve_1_ans),
                                                                    len(sieve_10_ans), len(fitingans)))
    if search_exp_num != 0:
        print(
            "for dataset {}, column {}, point search, fingerprint-sieve1-sieve10 avg blks is {}, search time is {}".format(
                datasetdir, column, [v / search_exp_num for v in search_blks],
                [v / search_exp_num for v in serch_times]
            ))
    f.close()
    curpoint = 5677
    while curpoint < 186005677:
        searchrange = [curpoint, curpoint]
        search_exp_num += 1
        curtime = time.time()
        fingerprintans = set()
        serch_times[0] += time.time() - curtime
        search_blks[0] += len(fingerprintans)
        curtime = time.time()
        sieve_0_ans = sieve_0.range_search(searchrange, column)
        serch_times[1] += time.time() - curtime
        search_blks[1] += len(sieve_0_ans)
        curtime = time.time()
        sieve_1_ans = sieve_1.range_search(searchrange, column)
        serch_times[2] += time.time() - curtime
        search_blks[2] += len(sieve_1_ans)
        curtime = time.time()
        sieve_10_ans = sieve_10.range_search(searchrange, column)
        serch_times[3] += time.time() - curtime
        search_blks[3] += len(sieve_10_ans)
        curtime = time.time()
        fitingans = set()
        serch_times[4] += time.time() - curtime
        search_blks[4] += len(fitingans)
        print("for range {}, fingerprint res is {}, sieve0 res is {}, sieve1 res is {},"
              "sieve10 res is {}, fiting tree res is {}".format(searchrange, len(fingerprintans),
                                                                len(sieve_0_ans), len(sieve_1_ans),
                                                                len(sieve_10_ans), len(fitingans)))
        curpoint += int(5e5)
    if search_exp_num != 0:
        print(
            "for dataset {}, column {}, point search, fingerprint-sieve1-sieve10 avg blks is {}, search time is {}".format(
                datasetdir, column, [v / search_exp_num for v in search_blks],
                [v / search_exp_num for v in serch_times]
            ))
    f.close()

if __name__=="__main__":
    datedirs = ["/mydata/bigdata/data/wikipedia/"]
    columns = ["pagecount"]
    prelogfile = ["/mydata/bigdata/learnedIndexForDataLake/index/simulator/vldbj/wiki21.csv.log"]
    for i in range(len(datedirs)):
        compareCurDataset(datedirs[i], columns[i], prelogfile[i])