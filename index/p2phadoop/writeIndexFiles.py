import pickle
import random
import sys

sys.path.append("../..")
from index.gapListIndex.GREindex import *
from index.gapListIndex.fingerprint import *
from workload.generate_adaptive_workload import *
from index.gapListIndex.minMaxIndex import *
from index.learnedIndexDemo.IndexOnTheLake import LakeIndex, RootLakeIndex
import time


workload = None
directory = ""
column = ""
selectivity = 0.005
_range = None

def writeNewParquet(origin_file, dest_file, row_groups):
    if len(row_groups) == 0:
        return
    origin_parquet_file = pp.ParquetFile(origin_file)
    dest_rowgroups_table = origin_parquet_file.read_row_groups(row_groups=row_groups)
    pp.write_table(dest_rowgroups_table, dest_file, row_group_size=50000)

# 先按file_path-rowgroupid来
def write_file(index, rowgroups):
    _dict = dict()
    for rowgroup in rowgroups:
        file_path = rowgroup.split("-")[0]
        row_group = int(rowgroup.split("-")[1])
        if file_path not in _dict:
            _dict[file_path] = set()
        _dict[file_path].add(row_group)
    for file_path in _dict:
        write_file_dir = "/mydata/bigdata/index/{}/{}/{}/".format(index, args.wltype, args.table)
        if not os.path.exists(write_file_dir):
            os.makedirs(write_file_dir)
        write_file_path = write_file_dir + file_path.strip().split("/")[-1]
        writeNewParquet(file_path, write_file_path, list(_dict[file_path]))

def write_minmax_files():
    start_time = time.time()
    minMax = MinMaxIndex(directory, [column])
    end_time = time.time()
    print("minmax_index create time is:" + str(end_time - start_time))
    start_time = time.time()
    minMaxRowGroups = minMax.range_search(_range, column)
    end_time = time.time()
    print("minmax_index search time is:" + str(end_time - start_time))
    write_file("minmax", minMaxRowGroups)

def write_finger_files():
    fingerprints = RootAllFileFingerPrints(directory, [column])
    start_time = time.time()
    fingerprints.generateIndex()
    end_time = time.time()
    print("finger_index create time is:" + str(end_time - start_time))
    start_time = time.time()
    fingerRowGroups = fingerprints.range_search(_range, column)
    with open("fingerRowGroups", "wb+") as f:
        pickle.dump(fingerRowGroups, f)
    end_time = time.time()
    print("finger_index search time is:" + str(end_time - start_time))
    write_file("finger", fingerRowGroups)

def write_gre_files():
    gre = RootGREindex(directory, [column])
    start_time = time.time()
    gre.generateIndex()
    end_time = time.time()
    print("gre_index create time is:" + str(end_time - start_time))
    start_time = time.time()
    greRowGroups = gre.range_search(_range, column)
    with open("greRowGroups", "wb+") as f:
        pickle.dump(greRowGroups, f)
    end_time = time.time()
    print("gre_index search time is:" + str(end_time - start_time))
    write_file("gre", greRowGroups)

def write_sieve_files():
    sieve = RootLakeIndex(directory, [column])
    start_time = time.time()
    sieve.generateIndexes()
    end_time = time.time()
    print("sieve_index create time is:" + str(end_time - start_time))
    start_time = time.time()
    sieveRowGroups = sieve.range_search(_range, column)
    with open("sieveRowGroups", "wb+") as f:
        pickle.dump(sieveRowGroups, f)
    end_time = time.time()
    print("sieve_index search time is:" + str(end_time - start_time))
    write_file("sieve", sieveRowGroups)

if __name__=="__main__":
    if args.wltype == "tpch" or args.wltype == "tpcds":
        directory = "/mydata/bigdata/data/{}_parquet_100_rewrite/{}/".format(args.wltype, args.table)
    elif args.wltype == "wiki":
        directory = "/mydata/bigdata/data/wikipedia/"
    elif args.wltype == "SSB":
        directory = "/mydata/bigdata/data/SSBData/"
    elif args.wltype == "tpch-order":
        directory = "/mydata/bigdata/data/tpch_order/{}/".format(args.table)
    workload = WorkLoad(directory)
    column = args.column
    workload.init(column)
    _range = workload.random_generate_range(selectivity)
    with open("_range", "wb+") as f:
        pickle.dump(_range, f)
    print("search range is {}".format(str(_range)))

    write_minmax_files()
    write_gre_files()
    write_finger_files()
    write_sieve_files()