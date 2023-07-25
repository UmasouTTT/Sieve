# 对比zonemap、cuckoo、sieve的过滤性能
import random
import sys



sys.path.append("../..")
from index.gapListIndex.gapListIndex import *
from index.gapListIndex.GREindex import *
from index.gapListIndex.fingerprint import *
from index.optimizedIndex import *
from workload.generate_adaptive_workload import *
from index.gapListIndex.minMaxIndex import *
from index.gapListIndex.reversedAll import ReversedAllIndex
from index.learnedIndexDemo.IndexOnTheLake import LakeIndex, RootLakeIndex
from index.gapListIndex.reversedIndex import ReversedIndex
import time

def compare(cuckooresdir):
    cuckoores = "wikipast.log"
    directory = ""
    if args.wltype == "tpch" or args.wltype == "tpcds":
        directory = "/mydata/bigdata/data/{}_{}/{}/".format(args.wltype, args.sf, args.table)
    elif args.wltype == "wiki":
        directory = "/mydata/bigdata/data/wikipedia/"
    elif args.wltype == "openstreet":
        directory = "/mydata/bigdata/data/openstreet/"
        args.column = "lon"
    elif args.wltype == "worse_data":
        directory = "/mydata/bigdata/data/worse_data/"
        args.column = "liuid"
    column = args.column
    optimizeIndex = ReversedAllIndex(directory, [column])
    optimizeIndex.generateIndexes()
    minMax = MinMaxIndex(directory, [column])
    minMax.generateIndexFromFile()
    learnedIndex = RootLakeIndex(directory, [column])
    learnedIndex.generateIndexFromFile()
    blkres = [0] * 3 #存放各个的块结果
    timeres = [0] * 3 #存放各个的查询时间
    searchnum = 0
    f = open(cuckoores, "r+", encoding="UTF-8")
    for line in f:
        if "range" in line:
            searchnum += 1
            searchrange = [int(line.split(":")[1].split()[-1].split(",")[0].strip()),
                           int(line.split(":")[1].split()[-1].split(",")[1].strip())]
            print("for range {}, ".format(searchrange), end="")
            curtime = time.time()
            temp = len(minMax.range_search(searchrange, column))
            blkres[0] += temp
            timeres[0] += time.time() - curtime
            print("mimax res is {}, ".format(temp), end="")
            curtime = time.time()
            temp = len(learnedIndex.range_search(searchrange, column))
            blkres[1] += temp
            timeres[1] += time.time() - curtime
            print("sieve res is {}, ".format(temp), end="")
            curtime = time.time()
            temp = len(optimizeIndex.range_search(searchrange, column))
            blkres[2] += temp
            timeres[2] += time.time() - curtime
            print("reversed res is {}, ".format(temp))
        elif "selectivity" in line or "point search start" in line:
            if searchnum != 0:
                print("blk res is {}, time res is {}".format([v/searchnum for v in blkres], [v/searchnum for v in timeres]))
            searchnum = 0
            blkres = [0] * 3  # 存放各个的块结果
            timeres = [0] * 3  # 存放各个的查询时间
    if searchnum != 0:
        print("blk res is {}, time res is {}".format([v / searchnum for v in blkres], [v / searchnum for v in timeres]))
    f.close()

if __name__=="__main__":
    args.sf = 1000
    args.wltype = "wiki"
    args.table = "store_sales"
    args.column = "pagecount"
    cuckooresdir = "/mydata/cuckoo-index/wikilog/"

    compare(cuckooresdir)