# 用之前随机跑出来过结果的日志进行实验，各索引对比、包括：minmax、finger、GRT、sieve-0.1、sieve-1、sieve-10、opt
import os
import random
import shutil
import sys
import re
import psutil

sys.path.append("../../../")
from index.gapListIndex.minMaxIndex import *
from index.gapListIndex.fingerprint import *
from index.learnedIndexDemo.IndexOnTheLake import *
from index.FIT.FITingTree import *
from workload.generate_adaptive_workload import WorkLoad

def genworkload(parquetdir, column, logname):
    workload = WorkLoad(parquetdir)
    workload.init(column)
    wf = open(logname, "w+", encoding="UTF-8")
    for selectivity in [0.00001, 0.0001, 0.001]:
        wf.write("selectivity {} start\n".format(selectivity))
        _ranges = workload.genseries_range(selectivity, 2)
        for _range in _ranges:
            wf.write("range:{},{}\n".format(_range[0], _range[1]))
    if column == "ss_ticket_number":
        _points = workload.genseries_point(500, 1)
    else:
        _points = workload.genseries_point(2)
    wf.write("selectivity point start\n")
    for _point in _points:
        wf.write("point:{}\n".format(_point))
    wf.close()
    shutil.copy2(logname, '../../../cuckoo-index/searchlogs/{}'.format(logname))


def compareCurDataset(datasetdir, column, prelogfile):
    indexnum = 6
    indexdumpdirs = [args.minmaxIndexDir, args.fingerprintIndexDir, args.learnedIndexDir, args.fittreeDir]
    for indexdumpdir in indexdumpdirs:
        if not os.path.exists(indexdumpdir):
            os.makedirs(indexdumpdir)
    # 创建各种索引
    minmaxindex = MinMaxIndex(datasetdir, [column])
    curtime = time.time()
    minmaxindex.generateIndexes()
    print("zonemap initialization time is {} s".format(time.time() - curtime))
    fingerprints = RootAllFileFingerPrints(datasetdir, [column])
    curtime = time.time()
    fingerprints.generateIndexes()
    print("fingerprint initialization time is {} s".format(time.time() - curtime))
    args.partition_num = 1000
    sieve_0 = RootLakeIndex(datasetdir, [column])
    curtime = time.time()
    sieve_0.generateIndexes("sieve-0.1")
    print("sieve-0.1 initialization time is {} s".format(time.time() - curtime))
    args.partition_num = 10000
    sieve_1 = RootLakeIndex(datasetdir, [column])
    curtime = time.time()
    sieve_1.generateIndexes("sieve-1")
    print("sieve-1 initialization time is {} s".format(time.time() - curtime))
    args.partition_num = 100000
    sieve_10 = RootLakeIndex(datasetdir, [column])
    curtime = time.time()
    sieve_10.generateIndexes("sieve-10")
    print("sieve-10 initialization time is {} s".format(time.time() - curtime))
    fitingtree = FIT(datasetdir, [column])
    curtime = time.time()
    fitingtree.generateIndexes()
    print("fiting(learned) initialization time is {} s".format(time.time() - curtime))
    f = open(prelogfile, "r+", encoding="UTF-8")
    search_blks = [0] * indexnum
    serch_times = [0] * indexnum
    search_exp_num, curselect = 0, ""
    for line in f:
        if "selectivity" in line:
            if search_exp_num != 0:
                print(
                    "for dataset {}, column {}, {} rangesearch, minmax-fingerprint-sieve0.1-sieve1-sieve10-fitingtree avg blks is {}, search time is {}".format(
                        curselect, datasetdir, column, [v / search_exp_num for v in search_blks],
                        [v / search_exp_num for v in serch_times]
                    ))
                search_blks = [0] * indexnum
                serch_times = [0] * indexnum
                search_exp_num = 0
            curselect = line.strip().split()[-2]
        elif "range" in line or "point" in line:
            tempv = line.strip().split(":")[1]
            if "range" in line:
                searchrange = [int(tempv.split(",")[0]), int(tempv.split(",")[1])]
            else:
                searchrange = [int(tempv), int(tempv)]
            search_exp_num += 1

            # minmax查询
            curtime = time.time()
            minmaxans = minmaxindex.range_search(searchrange, column)

            serch_times[0] += time.time() - curtime
            search_blks[0] += len(minmaxans)

            # finger查询
            curtime = time.time()
            fingerprintans = fingerprints.range_search(searchrange, column)
            serch_times[1] += time.time() - curtime
            search_blks[1] += len(fingerprintans)

            # sieve-0.1查询
            curtime = time.time()
            sieve_0_ans = sieve_0.range_search(searchrange, column)
            serch_times[2] += time.time() - curtime
            search_blks[2] += len(sieve_0_ans)

            # sieve-1查询
            curtime = time.time()
            sieve_1_ans = sieve_1.range_search(searchrange, column)
            serch_times[3] += time.time() - curtime
            search_blks[3] += len(sieve_1_ans)

            # sieve-10查询
            curtime = time.time()
            sieve_10_ans = sieve_10.range_search(searchrange, column)
            serch_times[4] += time.time() - curtime
            search_blks[4] += len(sieve_10_ans)

            #fit查询
            curtime = time.time()
            fitingans = fitingtree.range_search(searchrange, column)
            serch_times[5] += time.time() - curtime
            search_blks[5] += len(fitingans)

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
            # print("for range {}, minmax res is {},  fingerprint res is {}, sieve0 res is {}, sieve1 res is {},"
            #       "sieve10 res is {}, fiting tree res is {}".format(searchrange, len(minmaxans), len(fingerprintans),
            #                     len(sieve_0_ans), len(sieve_1_ans), len(sieve_10_ans), len(fitingans)))
    if search_exp_num != 0:
        print(
            "for dataset {}, column {}, point search, minmax-fingerprint-sieve0.1-sieve1-sieve10-fitingtree avg blks is {}, search time is {}".format(
                datasetdir, column, [v / search_exp_num for v in search_blks],
                [v / search_exp_num for v in serch_times]
            ))
    f.close()

def parquets2csvs(parquetdir, csvdir):
    if not os.path.exists(csvdir):
        os.makedirs(csvdir)
    parquetfiles = os.listdir(parquetdir)
    for parquetfile in parquetfiles:
        parquet_file = pp.ParquetFile(parquetdir + parquetfile)
        df = parquet_file.read().to_pandas()
        df.to_csv('{}{}.csv'.format(csvdir, parquetfile.split(".")[0]), index=False)

def getcuckooinfo(cuckoologdir):
    files = os.listdir(cuckoologdir)
    indexsize = 0
    indexinittime = 0
    blknums = [0] * 4
    searchtimes = [0] * 4
    for file in files:
        f = open(cuckoologdir + file, "r+", encoding="UTF-8")
        _index = 0
        for line in f:
            if "byte_size is" in line:
                indexsize += float(line.strip().split(":")[1].strip())
            if "cuckoo create time" in line:
                indexinittime += float(line.strip().split(":")[1].strip())
            if "avg blk num is" in line:
                blknums[_index] += float(line.strip().split(":")[1].strip())
            if "avg search time is" in line:
                searchtimes[_index] += float(line.strip().split(":")[1].strip())
                _index += 1
        f.close()
    print("cuckoo index init time is {}, size is {}, avg blk num is {}, avg search time is {}".
          format(indexinittime, indexsize, blknums, searchtimes))

if __name__=="__main__":
    current_path = os.getcwd()
    directorys = ["../../../dataset/StoreSales/",
                  "../../../dataset/Wikipedia/",
                  "../../../dataset/Maps/"]
    columns = ["ss_ticket_number", "pagecount", "lon"]
    logs = ["store_sales_100.workload",
               "wiki.workload",
               "maps.workload"]
    cuckoo_scripts = ["./runstoresales.sh",
                      "./runwiki.sh",
                      "./runmaps.sh"]
    cuckoo_workloads = ["../../../cuckoo-index/searchlogs/store_sales_100.workload",
                        "../../../cuckoo-index/searchlogs/wiki.workload",
                        "../../../cuckoo-index/searchlogs/maps.workload"]
    cuckoo_datadir = ["../../../cuckoo-index/storesales/",
               "../../../cuckoo-index/wiki/",
               "../../../cuckoo-index/maps/"]
    cuckoo_logdir = ["../../../cuckoo-index/sslog/",
                      "../../../cuckoo-index/wikilog/",
                      "../../../cuckoo-index/mapslog/"]

    errors = [100, 100, 70000]
    for i in range(len(directorys)):
        print("test on column {}".format(columns[i]))
        args.segment_error = errors[i]
        genworkload(directorys[i], columns[i], logs[i])
        compareCurDataset(directorys[i], columns[i], logs[i])
        parquets2csvs(directorys[i], cuckoo_datadir[i])
        if not os.path.exists(cuckoo_logdir[i]):
            os.makedirs(cuckoo_logdir[i])
        os.chdir("../../../cuckoo-index/")
        os.system("chmod +x {}".format(cuckoo_scripts[i]))
        os.system("{} > temp.log 2>&1".format(cuckoo_scripts[i]))
        os.chdir(current_path)
        getcuckooinfo(cuckoo_logdir[i])