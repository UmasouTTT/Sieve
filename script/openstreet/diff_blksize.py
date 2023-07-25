# 用于VLDB的补充实验，改变block size查看效果，本脚本主要用于：重写parquet，并测试
import os
import sys
import time

import pyarrow.parquet as pp

sys.path.append("../..")
from index.gapListIndex.minMaxIndex import *
from index.gapListIndex.fingerprint import *
from index.learnedIndexDemo.IndexOnTheLake import LakeIndex, RootLakeIndex
from index.gapListIndex.reversedIndex import ReversedIndex
from workload.generate_adaptive_workload import *

def transfile(originfile, writedir, dstblksize):
    # dstblksize指的是每个行组内的数据的条数，1万、5万、10万、20万
    if not os.path.exists(writedir):
        os.makedirs(writedir)
    _table = pp.read_table(originfile)
    pp.write_table(_table, writedir + str(dstblksize) + ".parquet", row_group_size=dstblksize)

def compare(parquetdir, column, recordNumPerBlock):#对比minmax，fingerprint、sieve、opt、cuckoo
    print("test on openstreet'lon, recordNumPerBlock of {}".format(recordNumPerBlock))
    # 用于对比各个索引的效果
    # 如果1万条/块，有4000块，那么fingerprint是 200个分桶，sieve partitionnum也是200个
    # 如果5万条/块，有800块，fingerprint 1000个分桶
    # 如果10万条/块，有400块，fingerprint是 2000分桶，sieve patitionnum也是2000
    # 如果20万条/块，有200块，fingerprint是 4000个分桶，sieve partitionnum也是4000
    args.segment_error = 70000
    if recordNumPerBlock == 1e4:
        args.partition_num = 200
        args.num_of_intervals = 200
    elif recordNumPerBlock == 5e4:
        args.partition_num = 1000
        args.num_of_intervals = 1000
    elif recordNumPerBlock == 1e5:
        args.partition_num = 2000
        args.num_of_intervals = 2000
    else:
        args.partition_num = 2000
        args.num_of_intervals = 2000
    minMax = MinMaxIndex(parquetdir, [column])
    minMax.generateIndexFromFile()
    fingerprints = RootAllFileFingerPrints(parquetdir, [column])
    fingerprints.generateIndexFromFile()
    learnedIndex = RootLakeIndex(parquetdir, [column])
    learnedIndex.generateIndexes()
    optimizeIndex = ReversedIndex(parquetdir, [column])
    optimizeIndex.generateIndexFromFile(column)
    workload = WorkLoad(parquetdir)
    workload.init(column)
    each_selectivity_expnum = 200
    for selectivity in [0.00001]:
        each_index_search_times = [0] * 4
        each_index_blk_num = [0] * 4
        _ranges = workload.genseries_range(selectivity, each_selectivity_expnum)
        wf = open("/proj/dst-PG0/VLDBR/blockscalelog/{}.txt".format(recordNumPerBlock), "w+", encoding="UTF-8")
        for _range in _ranges:
            wf.write("range:{},{}\n".format(_range[0], _range[1]))
        wf.close()
        print("exp search nums is {}, real search num is {}".format(each_selectivity_expnum, len(_ranges)))
        for _range in _ranges:
            curtime = time.time()
            optimizeRowGroups = optimizeIndex.range_search(_range, column)
            each_index_search_times[0] += (time.time() - curtime)
            each_index_blk_num[0] += len(optimizeRowGroups)
            curtime = time.time()
            minMaxRowGroups = minMax.range_search(_range, column)
            each_index_search_times[1] += (time.time() - curtime)
            each_index_blk_num[1] += len(minMaxRowGroups)
            curtime = time.time()
            fingerprintsRG = fingerprints.range_search(_range, column)
            each_index_search_times[2] += (time.time() - curtime)
            each_index_blk_num[2] += len(fingerprintsRG)
            curtime = time.time()
            learnedRowGroups = learnedIndex.range_search(_range, column)
            each_index_search_times[3] += (time.time() - curtime)
            each_index_blk_num[3] += len(learnedRowGroups)
            for rg in optimizeRowGroups:
                if rg not in minMaxRowGroups:
                    print("min max error ! for range {}".format(_range))
                    print("optimizeRG:" + str(optimizeRowGroups))
                    print("minMaxRowGroups:" + str(minMaxRowGroups))
                    break
                if rg not in fingerprintsRG:
                    print("fignerprintsRG error ! for range {}".format(_range))
                    print("optimizeRG:" + str(optimizeRowGroups))
                    print("fingerprintsRG:" + str(fingerprintsRG))
                    break
                if rg not in learnedRowGroups:
                    print("roughLearnedRowGroups error ! for range {}".format(_range))
                    print("optimizeRG:" + str(optimizeRowGroups))
                    print("roughLearnedRowGroups:" + str(learnedRowGroups))
                    break
            print("for search range {}, minmaxlen is {}, fingerprint len is {},"
                  "sieve len is {}, opt len is {}".format(_range, len(minMaxRowGroups),
                   len(fingerprintsRG), len(learnedRowGroups), len(optimizeRowGroups)))
        print("for selectivity {}, opt-minmax-finger-sieve avg blk num is {}, avg search time is {}".format(
            selectivity, [v/len(_ranges) for v in each_index_blk_num], [v/len(_ranges) for v in each_index_search_times]
        ))
    return

def compare_point(parquetdir, column, recordNumPerBlock):
    print("test on openstreet'lon, recordNumPerBlock of {}".format(recordNumPerBlock))
    # 用于对比各个索引的效果
    # 如果1万条/块，有4000块，那么fingerprint是 200个分桶，sieve partitionnum也是200个
    # 如果5万条/块，有800块，fingerprint 1000个分桶
    # 如果10万条/块，有400块，fingerprint是 2000分桶，sieve patitionnum也是2000
    # 如果20万条/块，有200块，fingerprint是 4000个分桶，sieve partitionnum也是4000
    args.segment_error = 70000
    if recordNumPerBlock == 1e4:
        args.partition_num = 200
        args.num_of_intervals = 200
    elif recordNumPerBlock == 5e4:
        args.partition_num = 1000
        args.num_of_intervals = 1000
    elif recordNumPerBlock == 1e5:
        args.partition_num = 2000
        args.num_of_intervals = 2000
    else:
        args.partition_num = 2000
        args.num_of_intervals = 2000
    minMax = MinMaxIndex(parquetdir, [column])
    minMax.generateIndexFromFile()
    fingerprints = RootAllFileFingerPrints(parquetdir, [column])
    fingerprints.generateIndexFromFile()
    learnedIndex = RootLakeIndex(parquetdir, [column])
    learnedIndex.generateIndexes()
    optimizeIndex = ReversedIndex(parquetdir, [column])
    optimizeIndex.generateIndexFromFile(column)
    workload = WorkLoad(parquetdir)
    workload.init(column)

    each_index_search_times = [0] * 4
    each_index_blk_num = [0] * 4

    # each_selectivity_expnum = 200
    # points = workload.genseries_point(each_selectivity_expnum)
    # wf = open("/proj/dst-PG0/VLDBR/blockscalelog/point{}.txt".format(recordNumPerBlock), "w+", encoding="UTF-8")
    # for point in points:
    #     wf.write("point:{}\n".format(point))
    # wf.close()

    points = []
    curpoint = -1800000000 + 7500000
    while curpoint <= 1800000000:
        points.append(curpoint)
        curpoint += int(1e7)
    # 加一些sieve捕获的全局间隙
    files = os.listdir(parquetdir)
    segementnum = len(learnedIndex.lakeindexs[column][parquetdir + files[0]].segments)
    for i in range(1, segementnum):
        pregap = learnedIndex.lakeindexs[column][parquetdir + files[0]].segments[i-1].segment_range[1] + 1
        curgap = learnedIndex.lakeindexs[column][parquetdir + files[0]].segments[i].segment_range[0] - 1
        if pregap < curgap:
            points.append(int((pregap + curgap)/2))
    wf = open("/proj/dst-PG0/VLDBR/blockscalelog/point{}.txt".format(recordNumPerBlock), "w+", encoding="UTF-8")
    for point in points:
        wf.write("point:{}\n".format(point))
    wf.close()
    each_selectivity_expnum = 0
    for point in points:
        each_selectivity_expnum += 1
        _range = [point, point]
        curtime = time.time()
        optimizeRowGroups = optimizeIndex.point_search(point, column)
        each_index_search_times[0] += (time.time() - curtime)
        each_index_blk_num[0] += len(optimizeRowGroups)
        curtime = time.time()
        minMaxRowGroups = minMax.range_search(_range, column)
        each_index_search_times[1] += (time.time() - curtime)
        each_index_blk_num[1] += len(minMaxRowGroups)
        curtime = time.time()
        fingerprintsRG = fingerprints.range_search(_range, column)
        each_index_search_times[2] += (time.time() - curtime)
        each_index_blk_num[2] += len(fingerprintsRG)
        curtime = time.time()
        learnedRowGroups = learnedIndex.range_search(_range, column)
        each_index_search_times[3] += (time.time() - curtime)
        each_index_blk_num[3] += len(learnedRowGroups)
        for rg in optimizeRowGroups:
            if rg not in minMaxRowGroups:
                print("min max error ! for range {}".format(_range))
                print("optimizeRG:" + str(optimizeRowGroups))
                print("minMaxRowGroups:" + str(minMaxRowGroups))
                break
            if rg not in fingerprintsRG:
                print("fignerprintsRG error ! for range {}".format(_range))
                print("optimizeRG:" + str(optimizeRowGroups))
                print("fingerprintsRG:" + str(fingerprintsRG))
                break
            if rg not in learnedRowGroups:
                print("roughLearnedRowGroups error ! for range {}".format(_range))
                print("optimizeRG:" + str(optimizeRowGroups))
                print("roughLearnedRowGroups:" + str(learnedRowGroups))
                break
        print("for search range {}, minmaxlen is {}, fingerprint len is {},"
              "sieve len is {}, opt len is {}".format(_range, len(minMaxRowGroups),
                                                      len(fingerprintsRG), len(learnedRowGroups),
                                                      len(optimizeRowGroups)))
    print("for selectivity {}, opt-minmax-finger-sieve avg blk num is {}, avg search time is {}".format(
        "point", [v / len(points) for v in each_index_blk_num], [v / len(points) for v in each_index_search_times]
    ))
    return

def comparetemp(parquetdir, column, recordNumPerBlock):#对比minmax，fingerprint、sieve、opt、cuckoo
    print("test on openstreet'lon, recordNumPerBlock of {}".format(recordNumPerBlock))
    # 用于对比各个索引的效果
    # 如果1万条/块，有4000块，那么fingerprint是 200个分桶，sieve partitionnum也是200个
    # 如果5万条/块，有800块，fingerprint 1000个分桶
    # 如果10万条/块，有400块，fingerprint是 2000分桶，sieve patitionnum也是2000
    # 如果20万条/块，有200块，fingerprint是 4000个分桶，sieve partitionnum也是4000
    args.segment_error = 70000
    if recordNumPerBlock == 1e4:
        args.partition_num = 200
        args.num_of_intervals = 200
    elif recordNumPerBlock == 5e4:
        args.partition_num = 1000
        args.num_of_intervals = 1000
    elif recordNumPerBlock == 1e5:
        args.partition_num = 2000
        args.num_of_intervals = 2000
    else:
        args.partition_num = 2000
        args.num_of_intervals = 2000
    minMax = MinMaxIndex(parquetdir, [column])
    minMax.generateIndexFromFile()
    fingerprints = RootAllFileFingerPrints(parquetdir, [column])
    fingerprints.generateIndexFromFile()
    learnedIndex = RootLakeIndex(parquetdir, [column])
    learnedIndex.generateIndexFromFile()


    each_selectivity_expnum = 200
    for selectivity in [0.00001]:
        each_index_search_times = [0] * 4
        each_index_blk_num = [0] * 4
        _ranges = []
        f = open("/proj/dst-PG0/VLDBR/blockscalelog/{}.txt".format(recordNumPerBlock), "r+", encoding="UTF-8")
        for line in f:
            _ss = int(line.strip().split(":")[1].split(",")[0].strip())
            _se = int(line.strip().split(":")[1].split(",")[1].strip())
            _ranges.append((_ss, _se))
        f.close()
        print("exp search nums is {}, real search num is {}".format(each_selectivity_expnum, len(_ranges)))
        for _range in _ranges:
            curtime = time.time()
            optimizeRowGroups = set()
            each_index_search_times[0] += (time.time() - curtime)
            each_index_blk_num[0] += len(optimizeRowGroups)
            curtime = time.time()
            minMaxRowGroups = minMax.range_search(_range, column)
            each_index_search_times[1] += (time.time() - curtime)
            each_index_blk_num[1] += len(minMaxRowGroups)
            curtime = time.time()
            fingerprintsRG = fingerprints.range_search(_range, column)
            each_index_search_times[2] += (time.time() - curtime)
            each_index_blk_num[2] += len(fingerprintsRG)
            curtime = time.time()
            learnedRowGroups = learnedIndex.range_search(_range, column)
            each_index_search_times[3] += (time.time() - curtime)
            each_index_blk_num[3] += len(learnedRowGroups)
            print("for search range {}, minmaxlen is {}, fingerprint len is {},"
                  "sieve len is {}, opt len is {}".format(_range, len(minMaxRowGroups),
                   len(fingerprintsRG), len(learnedRowGroups), len(optimizeRowGroups)))
        print("for selectivity {}, opt-minmax-finger-sieve avg blk num is {}, avg search time is {}".format(
            selectivity, [v/len(_ranges) for v in each_index_blk_num], [v/len(_ranges) for v in each_index_search_times]
        ))
    return

if __name__=="__main__":
    originfile = "/mydata/bigdata/data/openstreet/1.parquet"
    column = "lon"
    for v in [1e4, 5e4, 1e5, 2e5]:
        recordperblock = int(v)
        writedir = "/mydata/bigdata/diffblock/recordperblock{}/".format(recordperblock)
        # transfile(originfile, writedir, recordperblock)
        comparetemp(writedir, column, recordperblock)

