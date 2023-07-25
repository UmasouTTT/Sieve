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
import shutil

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
    write_file_dir = "/mydata/bigdata/index/{}/{}/{}/".format(index, args.wltype, args.table)
    if not os.path.exists(write_file_dir):
        os.makedirs(write_file_dir)
    else:
        shutil.rmtree(write_file_dir)
        os.makedirs(write_file_dir)
    for file_path in _dict:
        write_file_path = write_file_dir + file_path.strip().split("/")[-1]
        writeNewParquet(file_path, write_file_path, list(_dict[file_path]))
    hdfs_delete_dir = "/index/{}/{}/{}/".format(index, args.wltype, args.table)
    hdfs_write_dir = "/index/{}/{}/".format(index, args.wltype)
    os.system("hadoop fs -rm -r -skipTrash " + hdfs_delete_dir)#删除原有文件夹
    os.system("hadoop dfs -put " + write_file_dir + " " + hdfs_write_dir)#上传新的文件夹

def write_minmax_files(directory, _range):
    start_time = time.time()
    minMax = MinMaxIndex(directory, [args.column])
    end_time = time.time()
    print("minmax_index create time is:" + str(end_time - start_time))
    start_time = time.time()
    minMaxRowGroups = minMax.range_search(_range, args.column)
    end_time = time.time()
    print("minmax_index search time is:" + str(end_time - start_time))
    write_file("minmax", minMaxRowGroups)

def write_finger_files(directory, _range):
    fingerprints = RootAllFileFingerPrints(directory, [args.column])
    start_time = time.time()
    fingerprints.generateIndexFromFile()
    end_time = time.time()
    print("finger_index create time is:" + str(end_time - start_time))
    start_time = time.time()
    fingerRowGroups = fingerprints.range_search(_range, args.column)
    end_time = time.time()
    with open("fingerRowGroups", "wb+") as f:
        pickle.dump(fingerRowGroups, f)
    print("finger_index search time is:" + str(end_time - start_time))
    write_file("finger", fingerRowGroups)

def write_gre_files(directory, _range):
    gre = RootGREindex(directory, [args.column])
    start_time = time.time()
    gre.generateIndexFromFile()
    end_time = time.time()
    print("gre_index create time is:" + str(end_time - start_time))
    start_time = time.time()
    greRowGroups = gre.range_search(_range, args.column)
    end_time = time.time()
    with open("greRowGroups", "wb+") as f:
        pickle.dump(greRowGroups, f)
    print("gre_index search time is:" + str(end_time - start_time))
    write_file("gre", greRowGroups)

def write_sieve_files(directory, _range):
    sieve = RootLakeIndex(directory, [args.column])
    start_time = time.time()
    sieve.generateIndexesFromFile()
    end_time = time.time()
    print("sieve_index create time is:" + str(end_time - start_time))
    start_time = time.time()
    sieveRowGroups = sieve.range_search( _range, args.column)
    end_time = time.time()
    with open("sieveRowGroups", "wb+") as f:
        pickle.dump(sieveRowGroups, f)
    print("sieve_index search time is:" + str(end_time - start_time))
    write_file("sieve", sieveRowGroups)

def run_main():
    _range = [args.range_s, args.range_e]
    if args.wltype == "tpch" or args.wltype == "tpcds":
        directory = "/mydata/bigdata/data/{}_parquet_100_rewrite/{}/".format(args.wltype, args.table)
    elif args.wltype == "wiki":
        directory = "/mydata/bigdata/data/wikipedia/"
    elif args.wltype == "SSB":
        directory = "/mydata/bigdata/data/SSBData/"
    elif args.wltype == "tpch-order":
        directory = "/mydata/bigdata/data/tpch_order/{}/".format(args.table)
    with open("_range", "wb+") as f:
        pickle.dump(_range, f)
    print("search range is {}".format(str(_range)))

    write_minmax_files(directory, _range)
    write_gre_files(directory, _range)
    write_finger_files(directory, _range)
    write_sieve_files(directory, _range)

if __name__=="__main__":
    run_main()