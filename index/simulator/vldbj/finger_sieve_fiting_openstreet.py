# VLDB修订的实验，比较fingerprint、sieve-1，sieve-10的效果
import multiprocessing
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
    fingerprints = RootAllFileFingerPrints(datasetdir, [column])
    fingerprints.generateIndexes()
    args.partition_num = 1000
    sieve_0 = RootLakeIndex(datasetdir, [column])
    sieve_0.generateIndexes()
    args.partition_num = 10000
    sieve_1 = RootLakeIndex(datasetdir, [column])
    sieve_1.generateIndexes()
    args.partition_num = 100000
    sieve_10 = RootLakeIndex(datasetdir, [column])
    sieve_10.generateIndexes()
    fitingtree = ReversedIndex(datasetdir, [column])
    fitingtree.generateIndexes()
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
            fingerprintans = fingerprints.range_search(searchrange, column)
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
            if searchrange[0] == searchrange[1]:
                fitingans = fitingtree.point_search(searchrange[0], column)
            else:
                fitingans = fitingtree.range_search(searchrange, column)
            serch_times[4] += time.time() - curtime
            search_blks[4] += len(fitingans)
            for rg in fitingans:
                if rg not in fingerprintans:
                    print("fignerprintsRG error ! for range {}".format(searchrange))
                    print("optimizeRG:" + str(fitingans))
                    print("fingerprintans:" + str(fingerprintans))
                    break
                if rg not in sieve_0_ans:
                    print("sieve_0_ans error ! for range {}".format(searchrange))
                    print("optimizeRG:" + str(fitingans))
                    print("sieve_0_ans:" + str(sieve_0_ans))
                    break
                if rg not in sieve_1_ans:
                    print("sieve_1_ans error ! for range {}".format(searchrange))
                    print("optimizeRG:" + str(fitingans))
                    print("sieve_1_ans:" + str(sieve_1_ans))
                    break
                if rg not in sieve_10_ans:
                    print("sieve_10_ans error ! for range {}".format(searchrange))
                    print("optimizeRG:" + str(fitingans))
                    print("sieve_10_ans:" + str(sieve_10_ans))
                    break
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
    curpoint = -2147483648 + 666
    while curpoint <= 2147318852:
        searchrange = [curpoint, curpoint]
        search_exp_num += 1
        curtime = time.time()
        fingerprintans = fingerprints.range_search(searchrange, column)
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
        fitingans = fitingtree.range_search(searchrange, column)
        serch_times[4] += time.time() - curtime
        search_blks[4] += len(fitingans)
        for rg in fitingans:
            if rg not in fingerprintans:
                print("fignerprintsRG error ! for range {}".format(searchrange))
                print("optimizeRG:" + str(fitingans))
                print("fingerprintans:" + str(fingerprintans))
                break
            if rg not in sieve_0_ans:
                print("sieve_0_ans error ! for range {}".format(searchrange))
                print("optimizeRG:" + str(fitingans))
                print("sieve_0_ans:" + str(sieve_0_ans))
                break
            if rg not in sieve_1_ans:
                print("sieve_1_ans error ! for range {}".format(searchrange))
                print("optimizeRG:" + str(fitingans))
                print("sieve_1_ans:" + str(sieve_1_ans))
                break
            if rg not in sieve_10_ans:
                print("sieve_10_ans error ! for range {}".format(searchrange))
                print("optimizeRG:" + str(fitingans))
                print("sieve_10_ans:" + str(sieve_10_ans))
                break
        print("for range {}, fingerprint res is {}, sieve0 res is {}, sieve1 res is {},"
              "sieve10 res is {}, fiting tree res is {}".format(searchrange, len(fingerprintans),
                                                                len(sieve_0_ans), len(sieve_1_ans),
                                                                len(sieve_10_ans), len(fitingans)))
        curpoint += int(1e7)
    if search_exp_num != 0:
        print(
            "for dataset {}, column {}, point search, fingerprint-sieve1-sieve10 avg blks is {}, search time is {}".format(
                datasetdir, column, [v / search_exp_num for v in search_blks],
                [v / search_exp_num for v in serch_times]
            ))
    f.close()

def genFiting(parquetdir):
    fitingtree = ReversedIndex(parquetdir, ["lon"])
    fitingtree.generateIndexes()

if __name__=="__main__":
    datedirs = ["/mydata/bigdata/data/openstreet/parquet1/",
                "/mydata/bigdata/data/openstreet/parquet2/",
                "/mydata/bigdata/data/openstreet/parquet3/",
                "/mydata/bigdata/data/openstreet/parquet4/"
                ]

    # 创建进程池，最大进程数为5个
    pool = multiprocessing.Pool(processes=4)

    # 使用进程池调用myfunc()函数
    results = pool.map(genFiting, datedirs)

    # 关闭进程池
    pool.close()
    pool.join()