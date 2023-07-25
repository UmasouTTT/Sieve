# 对比zonemap、cuckoo、sieve的过滤性能
import random
import sys
import re



sys.path.append("../..")
from index.gapListIndex.gapListIndex import *
from index.gapListIndex.GREindex import *
from index.gapListIndex.fingerprint import *
from index.optimizedIndex import *
from workload.generate_adaptive_workload import *
from index.gapListIndex.minMaxIndex import *
from index.learnedIndexDemo.IndexOnTheLake import LakeIndex, RootLakeIndex
from index.gapListIndex.reversedIndex import ReversedIndex
from index.gapListIndex.reversedAll import ReversedAllIndex
import time

def getMS(cuckooresdir):
    files = os.listdir(cuckooresdir)
    cuckoores = cuckooresdir + files[0]
    directory = ""
    if args.wltype == "tpch" or args.wltype == "tpcds":
        directory = "/mydata/bigdata/data/{}_{}/{}/".format(args.wltype, args.sf, args.table)
    elif args.wltype == "wiki":
        directory = "/mydata/bigdata/data/wikipedia/"
    elif args.wltype == "openstreet":
        directory = "/mydata/bigdata/data/openstreet/parquet/"
        args.column = "lon"
    elif args.wltype == "worse_data":
        directory = "/mydata/bigdata/data/worse_data/"
        args.column = "liuid"
    column = args.column
    # optimizeIndex = ReversedIndex(directory, [column])
    # optimizeIndex.generateIndexFromFile(column)
    minMax = MinMaxIndex(directory, [column])
    minMax.generateIndexFromFile()
    learnedIndex = RootLakeIndex(directory, [column])
    learnedIndex.generateIndexFromFile()
    blkres = [0] * 3 #存放各个的块结果
    timeres = [0] * 3 #存放各个的查询时间
    searchnum = 0
    f = open(cuckoores, "r+", encoding="UTF-8")
    for line in f:
        if "for search range" in line:
            searchnum += 1
            match = re.search(r"(-?\d+)-(-?\d+)", line.strip())
            searchrange = [int(match.group(1)),
                           int(match.group(2))]
            curtime = time.time()
            minmaxlen = len(minMax.range_search(searchrange, column))
            blkres[0] += minmaxlen
            timeres[0] += time.time() - curtime
            curtime = time.time()
            sievelen = len(learnedIndex.range_search(searchrange, column))
            blkres[1] += sievelen
            timeres[1] += time.time() - curtime
            curtime = time.time()
            blkres[2] += 0
            timeres[2] += time.time() - curtime
            print("for range {}, minmaxlen is {}, sievelen is {}".format(searchrange, minmaxlen, sievelen))
        elif "selectivity" in line or "point search start" in line:
            if searchnum != 0:
                print("blk res is {}, time res is {}".format([v/searchnum for v in blkres], [v/searchnum for v in timeres]))
            searchnum = 0
            blkres = [0] * 3  # 存放各个的块结果
            timeres = [0] * 3  # 存放各个的查询时间
    if searchnum != 0:
        print("blk res is {}, time res is {}".format([v / searchnum for v in blkres], [v / searchnum for v in timeres]))
    f.close()

def getcuckoo(cuckooresdir):
    files = os.listdir(cuckooresdir)
    cuckooblkres = [0] * 4
    cuckootimeres = [0] * 4
    for file in files:
        curindex = 0
        cuckoores = cuckooresdir + file
        f = open(cuckoores, "r+", encoding="UTF-8")
        for line in f:
            if "avg search" in line:
                if curindex == 2:
                    print(file)
                    print(line)
                blknum = float(line.split(";")[0].split(":")[-1].strip())
                searchtime = float(line.split(";")[1].split(":")[-1].strip())
                cuckooblkres[curindex] += blknum
                cuckootimeres[curindex] += searchtime
                curindex += 1
        f.close()
    print("for cuckoo, blk res is {}, time res is {}".format(cuckooblkres, cuckootimeres))

def getOptWikiRes():
    directory = "/mydata/bigdata/data/wikipedia/"
    column = "pagecount"
    optimizeIndex = ReversedAllIndex(directory, [column])
    optimizeIndex.generateIndexes()
    curpoint = 5677
    blkres = [0] * 3  # 存放各个的块结果
    timeres = [0] * 3  # 存放各个的查询时间
    searchnum = 0
    while curpoint < 186005677:  # 最大是1e11
        searchrange = [curpoint, curpoint]
        optl = len(optimizeIndex.range_search(searchrange, column))
        blkres[2] += optl
        print("for point {}, minmax res is {}, sieve res is {}, opt res is {}".format(
            curpoint, 0, 0, optl
        ))
        searchnum += 1
        curpoint += int(5e5)
    print("avg is {}, avgtime is {}".format([v / searchnum for v in blkres], [v / searchnum for v in timeres]))


def getsieveres(filelog):
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
    learnedIndex = RootLakeIndex(directory, [column])
    learnedIndex.generateIndexFromFile()
    countblk, snum, optcount = 0, 0, 0
    flag = False
    f = open(filelog, "r+", encoding="UTF-8")
    for line in f:#key is:40422;optlen is:11992
        if "point search start" in line:
            flag = True
        if flag and "for search range" in line:
            _key = int(line.split(",")[0].split("-")[1].strip())
            curlen = len(learnedIndex.range_search([_key, _key], column))
            countblk += curlen
            snum += 1
            print("for point {}, sieve res is {}, opt len is {}".format(_key, curlen, 0))
    f.close()
    print("avg sieve res is {}, opt res is {}".format(countblk/snum, optcount/snum))

def getwikipointres():
    # 按步长为10000生成查询，直到查询值大于90000000000()
    # 第一步随机从1-10000生成
    directory = "/mydata/bigdata/data/wikipedia/"
    column = "pagecount"
    learnedIndex = RootLakeIndex(directory, [column])
    learnedIndex.generateIndexFromFile()
    # optimizeIndex = ReversedIndex(directory, [column])
    # optimizeIndex.generateIndexFromFile(column)
    minMax = MinMaxIndex(directory, [column])
    minMax.generateIndexFromFile()
    # curpoint = random.randint(0, 9999)
    curpoint = 5677
    blkres = [0] * 3  # 存放各个的块结果
    timeres = [0] * 3  # 存放各个的查询时间
    searchnum = 0
    while curpoint < 186005677:#最大是1e11
        searchrange = [curpoint, curpoint]
        curtime = time.time()
        # minmaxl = len(minMax.range_search(searchrange, column))
        minmaxl = 0
        timeres[0] += time.time() - curtime
        blkres[0] += minmaxl
        curtime = time.time()
        sievel = len(learnedIndex.range_search(searchrange, column))
        blkres[1] += sievel
        timeres[1] += time.time() - curtime
        # optl = len(optimizeIndex.range_search(searchrange, column))
        optl = 0
        blkres[2] += optl
        print("for point {}, minmax res is {}, sieve res is {}, opt res is {}".format(
            curpoint, minmaxl, sievel, optl
        ))
        searchnum += 1
        curpoint += int(5e5)
    print("avg is {}, avgtime is {}".format([v/searchnum for v in blkres], [v/searchnum for v in timeres]))

def getOptOpenRes():
    directory = "/mydata/bigdata/data/openstreet/parquet/"
    column = "lon"
    optimizeIndex = ReversedAllIndex(directory, [column])
    optimizeIndex.generateIndexes()
    curpoint = -1800000000 + 36
    blkres = [0] * 3  # 存放各个的块结果
    timeres = [0] * 3  # 存放各个的查询时间
    searchnum = 0
    while curpoint < 1800000000:  # 范围是-1800000000-1800000000
        searchrange = [curpoint, curpoint]
        optl = len(optimizeIndex.range_search(searchrange, column))
        blkres[2] += optl
        print("for point {}, minmax res is {}, sieve res is {}, opt res is {}".format(
            curpoint, 0, 0, optl
        ))
        searchnum += 1
        curpoint += int(1e7)
    print("avg is {}, avgtime is {}".format([v / searchnum for v in blkres], [v / searchnum for v in timeres]))


def getopenstreeetpointres():
    # 按步长为10000生成查询，直到查询值大于90000000000()
    # 第一步随机从1-10000生成
    directory = "/mydata/bigdata/data/openstreet/parquet/"
    column = "lon"
    learnedIndex = RootLakeIndex(directory, [column])
    learnedIndex.generateIndexFromFile()
    # optimizeIndex = ReversedAllIndex(directory, [column])
    # optimizeIndex.generateIndexes()
    minMax = MinMaxIndex(directory, [column])
    minMax.generateIndexFromFile()
    # curpoint = random.randint(0, 9999)
    curpoint = -1800000000 + 7500000
    blkres = [0] * 3  # 存放各个的块结果
    timeres = [0] * 3  # 存放各个的查询时间
    searchnum = 0
    while curpoint < 1800000000:#范围是-1800000000-1800000000
        searchrange = [curpoint, curpoint]
        curtime = time.time()
        minmaxl = len(minMax.range_search(searchrange, column))
        timeres[0] += time.time() - curtime
        blkres[0] += minmaxl
        curtime = time.time()
        sievel = len(learnedIndex.range_search(searchrange, column))
        blkres[1] += sievel
        timeres[1] += time.time() - curtime
        # optl = len(optimizeIndex.range_search(searchrange, column))
        optl = 0
        blkres[2] += optl
        print("for point {}, minmax res is {}, sieve res is {}, opt res is {}".format(
            curpoint, minmaxl, sievel, optl
        ))
        searchnum += 1
        curpoint += int(1e7)
    print("avg is {}, avgtime is {}".format([v/searchnum for v in blkres], [v/searchnum for v in timeres]))


if __name__=="__main__":
    args.sf = 1000
    args.wltype = "openstreet"
    args.table = "store_sales"
    args.column = "lon"
    # cuckooresdir = "/proj/dst-PG0/VLDBR/cuckoo/openstreetlog/"
    # getMS(cuckooresdir)
    getcuckoo("/proj/dst-PG0/VLDBR/cuckoo/wikilog/")
    # getOptWikiRes()
    # getopenstreeetpointres()