# 自动化的测试脚本
import os
import sys
import time

sys.path.append("../..")
from param import args
from index.p2phadoop.writeFilesFromExistIndex import run_main
from index.gapListIndex.GREindex import *
from index.gapListIndex.fingerprint import *
from workload.generate_adaptive_workload import *
from index.gapListIndex.minMaxIndex import *
from index.learnedIndexDemo.IndexOnTheLake import LakeIndex, RootLakeIndex
import shutil


def write_file(index, rowgroups):
    start_time = time.time()
    _dict = dict()
    for rowgroup in rowgroups:
        file_path = rowgroup.split("-")[0]
        row_group = int(rowgroup.split("-")[1])
        if file_path not in _dict:
            _dict[file_path] = set()
        _dict[file_path].add(row_group)
    write_file_dir = "/mydata/bigdata/index/{}/{}/{}1/".format(index, args.wltype, args.table) #本地临时保存中间文件的目录
    if not os.path.exists(write_file_dir):
        os.makedirs(write_file_dir)
    else:
        shutil.rmtree(write_file_dir)
        os.makedirs(write_file_dir)
    print(index + " local dir deleted!")
    file_count = 0
    for file_path in _dict:
        for rowgroupid in _dict[file_path]:
            write_file_path = write_file_dir + args.table + str(file_count)
            file_count += 1
            origin_parquet_file = pp.ParquetFile(file_path)
            dest_rowgroups_table = origin_parquet_file.read_row_group(rowgroupid)
            pp.write_table(dest_rowgroups_table, write_file_path, row_group_size=50000)

def perindexfunc(index, indextype, _range):
    start_time = time.time()
    rowGroups = index.range_search(_range, args.column)
    end_time = time.time()
    print(indextype + " search time is:" + str(end_time - start_time) + "\n")
    write_file(indextype, rowGroups)

args.wltype = "tpcds"
args.table = "store_sales"
args.column = "ss_ticket_number"
simulate_file = "tpcds-ss_ticket_number.log"
isFirstRun = args.isFirstRun

if args.wltype == "tpch" or args.wltype == "tpcds":
    directory = "/mydata/bigdata/data/{}_parquet_100_rewrite/{}/".format(args.wltype, args.table)
elif args.wltype == "wiki":
    directory = "/mydata/bigdata/data/wikipedia/"
elif args.wltype == "SSB":
    directory = "/mydata/bigdata/data/SSBData/"
elif args.wltype == "tpch-order":
    directory = "/mydata/bigdata/data/tpch_order/{}/".format(args.table)


fingerprints = RootAllFileFingerPrints(directory, [args.column])
start_time = time.time()
if isFirstRun:
    fingerprints.generateIndex()
else:
    fingerprints.generateIndexFromFile()
end_time = time.time()
print("finger_index create time is:" + str(end_time - start_time) + "\n")

sieve = RootLakeIndex(directory, [args.column])
start_time = time.time()
if isFirstRun:
    sieve.generateIndexes()
else:
    sieve.generateIndexesFromFile()
end_time = time.time()
print("sieve_index create time is:" + str(end_time - start_time) + "\n")

# 第一步，获得查询范围
_range_list = []
_sqls = []
f = open(simulate_file)
for line in f:
    if "for range (" in line:
        tempstr = line.split(")")[0]
        tempstr = tempstr.split("(")[-1]
        values = tempstr.split(",")
        _range_list.append([int(values[0].strip()), int(values[1].strip())])
    elif "select" in line:
        _sqls.append(line.strip())
f.close()

for _range in _range_list:
    # 第二步，各索引生产的子副本，上传到
    args.range_s = _range[0]
    args.range_e = _range[1]

    with ThreadPoolExecutor(32, thread_name_prefix='autorun_ss_ticket_number_pool') as pools:
        all_task = []
        all_task.append(pools.submit(perindexfunc, fingerprints, "finger", _range))
        all_task.append(pools.submit(perindexfunc, sieve, "sieve", _range))
        wait(all_task, return_when=ALL_COMPLETED)
    time.sleep(5)
    print("all index file prepared!")

