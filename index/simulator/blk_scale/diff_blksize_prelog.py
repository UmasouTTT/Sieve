# 用于VLDB的补充实验，改变block size查看效果，本脚本主要用于：重写parquet，并测试
import copy
import os
import re
import shutil
import sys
import time

import pyarrow.parquet as pp

sys.path.append("../../..")
from index.gapListIndex.minMaxIndex import *
from index.gapListIndex.fingerprint import *
from index.learnedIndexDemo.IndexOnTheLake import LakeIndex, RootLakeIndex
from index.gapListIndex.reversedIndex import ReversedIndex
from workload.generate_adaptive_workload import *
from index.FIT.FITingTree import FIT

def transfile(originfile, writedir, dstblksize):
    # dstblksize指的是每个行组内的数据的条数，1万、5万、10万、20万
    if not os.path.exists(writedir):
        os.makedirs(writedir)
    _table = pp.read_table(originfile)
    pp.write_table(_table, writedir + str(dstblksize) + ".parquet", row_group_size=dstblksize)

# 拿前面实验的算例跑一次
def compare(parquetdir, column, recordNumPerBlock, error, oldworkload):#对比minmax，fingerprint、sieve、opt、cuckoo
    # 用于对比各个索引的效果
    # 如果1万条/块，有4000块，那么fingerprint是 200个分桶，sieve partitionnum也是200个
    # 如果5万条/块，有800块，fingerprint 1000个分桶
    # 如果10万条/块，有400块，fingerprint是 2000分桶，sieve patitionnum也是2000
    # 如果20万条/块，有200块，fingerprint是 4000个分桶，sieve partitionnum也是4000
    # 假设x块，那么总数据量为50000*x
    indexnum = 4
    args.segment_error = error
    if recordNumPerBlock == 1e4:
        args.partition_num = 200 * 2
        args.num_of_intervals = 200 * 2
    elif recordNumPerBlock == 5e4:
        args.partition_num = 1000 * 2
        args.num_of_intervals = 1000 * 2
    elif recordNumPerBlock == 1e5:
        args.partition_num = 2000 * 2
        args.num_of_intervals = 2000 * 2
    else:
        args.partition_num = 2000 * 2
        args.num_of_intervals = 2000 * 2
    minmaxindex = MinMaxIndex(parquetdir, [column])
    minmaxindex.generateIndexes()
    fingerprints = RootAllFileFingerPrints(parquetdir, [column])
    fingerprints.generateIndexes()
    sieve_0 = RootLakeIndex(parquetdir, [column])
    sieve_0.generateIndexes()
    fitIndex = FIT(parquetdir, [column])
    fitIndex.generateIndexes()

    f = open(oldworkload, "r+", encoding="UTF-8")
    search_blks = [0] * indexnum
    search_exp_num, curselect = 0, ""
    for line in f:
        if "selectivity" in line:
            if search_exp_num != 0:
                print(
                    "for {}, minmax-fingerprint-sieve-fitingtree avg blks is {}".format(
                        curselect, [v / search_exp_num for v in search_blks]))
                search_blks = [0] * indexnum
                search_exp_num = 0
            curselect = line.strip()
        elif "range" in line or "point" in line:
            tempv = line.strip().split(":")[1]
            if "range" in line:
                searchrange = [int(tempv.split(",")[0]), int(tempv.split(",")[1])]
            else:
                searchrange = [int(tempv), int(tempv)]
            search_exp_num += 1

            # minmax查询
            minmaxans = minmaxindex.range_search(searchrange, column)
            search_blks[0] += len(minmaxans)

            # finger查询
            fingerprintans = fingerprints.range_search(searchrange, column)
            search_blks[1] += len(fingerprintans)

            # sieve-0.1查询
            sieve_0_ans = sieve_0.range_search(searchrange, column)
            search_blks[2] += len(sieve_0_ans)

            # fit查询
            fitingans = fitIndex.range_search(searchrange, column)
            search_blks[3] += len(fitingans)

            # 正确性检查
            for rg in fitingans:
                if rg not in minmaxans:
                    print("minmaxindex error ! for range {}".format(searchrange))
                    print("optimizeRG:" + str(fitingans))
                    print("minmaxans:" + str(minmaxans))
                    break
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
            # print("for range {}, minmax res is {},  fingerprint res is {}, sieve0 res is {}, sieve1 res is {},"
            #       "sieve10 res is {}, fiting tree res is {}".format(searchrange, len(minmaxans), len(fingerprintans),
            #                     len(sieve_0_ans), len(sieve_1_ans), len(sieve_10_ans), len(fitingans)))
    if search_exp_num != 0:
        print(
            "for {}, minmax-fingerprint-sieve-fitingtree avg blks is {}".format(
                curselect, [v / search_exp_num for v in search_blks]))
    f.close()

def getcuckoodirinfo(cuckoodir):
    indexsizes = [0] * 4
    blknums = [[] for i in range(4)]
    _index = -1
    files = os.listdir(cuckoodir)
    for file in files:
        if "200000" in file:
            _index = 3
        elif "100000" in file:
            _index = 2
        elif "50000" in file:
            _index = 1
        elif "10000" in file:
            _index = 0
        f = open(cuckoodir+file, "r+", encoding="UTF-8")
        for line in f:
            if "byte_size is" in line:
                indexsizes[_index] += float(line.strip().split(":")[1].strip())
            if "avg blk num is" in line:
                blknums[_index].append(float(line.strip().split(":")[1].strip()))
        f.close()
    print("for blk size 1w-5w-10w-20w, cuckoo index size is {}, avg blk num is {}".format(
        indexsizes, blknums
    ))

if __name__=="__main__":
    current_path = os.getcwd()
    originfile = "../../../dataset/Maps/1.parquet"
    csvdir = "../../../cuckoo-index/diffblk/"
    if not os.path.exists(csvdir):
        os.makedirs(csvdir)
    parquet_file = pp.ParquetFile(originfile)
    df = parquet_file.read().to_pandas()
    df.to_csv('{}1.csv'.format(csvdir), index=False)
    column = "lon"
    error = 70000
    old_workload = "diffblkpre.workload"
    for v in [1e4, 5e4, 1e5, 2e5]:
        recordperblock = int(v)
        writedir = "../../../dataset/diffblock/{}/recordperblock{}/".format(column, recordperblock)
        transfile(originfile, writedir, recordperblock)
        compare(writedir, column, recordperblock, error, old_workload)
    shutil.copy2(old_workload, '../../../cuckoo-index/searchlogs/diffblk.workload')
    cuckoologdir = "../../../cuckoo-index/diffblklog/"
    if not os.path.exists(cuckoologdir):
        os.makedirs(cuckoologdir)
    os.chdir("../../../cuckoo-index/")
    os.system("chmod +x rundiffblk.sh")
    os.system("./rundiffblk.sh > temp.log 2>&1")
    os.chdir(current_path)
    getcuckoodirinfo(cuckoologdir)