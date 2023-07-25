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

def writeNewParquet(origin_file, dest_file, row_groups):
    if len(row_groups) == 0:
        return
    origin_parquet_file = pp.ParquetFile(origin_file)
    dest_rowgroups_table = origin_parquet_file.read_row_groups(row_groups=row_groups)
    pp.write_table(dest_rowgroups_table, dest_file, row_group_size=50000)

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

args.wltype = "wiki"
args.table = "wikipedia"
args.column = "pagecount"
bandwith = "30000"
simulate_file = "wiki-pagecount.log"
isFirstRun = args.isFirstRun

if args.wltype == "tpch" or args.wltype == "tpcds":
    directory = "/mydata/bigdata/data/{}_parquet_100_rewrite/{}/".format(args.wltype, args.table)
elif args.wltype == "wiki":
    directory = "/mydata/bigdata/data/wikipedia/"
elif args.wltype == "SSB":
    directory = "/mydata/bigdata/data/SSBData/"
elif args.wltype == "tpch-order":
    directory = "/mydata/bigdata/data/tpch_order/{}/".format(args.table)

hdfs_delete_dir = "/wiki_origin/wikipedia"
hdfs_write_dir = "/wiki_origin/"
os.system("hadoop fs -rm -r -skipTrash " + hdfs_delete_dir)#删除原有文件夹
os.system("hadoop dfs -put " + directory + " " + hdfs_write_dir)#上传新的文件夹

start_time = time.time()
minMax = MinMaxIndex(directory, [args.column])
end_time = time.time()
print("minmax_index create time is:" + str(end_time - start_time) + "\n")
fingerprints = RootAllFileFingerPrints(directory, [args.column])
start_time = time.time()
if isFirstRun:
    fingerprints.generateIndex()
else:
    fingerprints.generateIndexFromFile()
end_time = time.time()
print("finger_index create time is:" + str(end_time - start_time) + "\n")
gre = RootGREindex(directory, [args.column])
start_time = time.time()
if isFirstRun:
    gre.generateIndex()
else:
    gre.generateIndexFromFile()
end_time = time.time()
print("gre_index create time is:" + str(end_time - start_time) + "\n")
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
    if "For range (" in line:
        tempstr = line.split(")")[0]
        tempstr = tempstr.split("(")[-1]
        values = tempstr.split(",")
        _range_list.append([int(values[0].strip()), int(values[1].strip())])
    elif "select" in line:
        _sqls.append(line.strip())
f.close()

os.system("ssh node0 \"rm -f /proj/ccjs-PG0/p2plog/wiki_pagecount.log\"".format(args.wltype, args.table)) #删除原有的结果
for _range in _range_list:
    # 第二步，各索引生产的子副本，上传到hdfs
    args.range_s = _range[0]
    args.range_e = _range[1]
    # minmax
    start_time = time.time()
    minMaxRowGroups = minMax.range_search(_range, args.column)
    end_time = time.time()
    print("minmax_index search time is:" + str(end_time - start_time) + "\n")
    write_file("minmax", minMaxRowGroups)

    # finger
    start_time = time.time()
    fingerRowGroups = fingerprints.range_search(_range, args.column)
    end_time = time.time()
    print("finger_index search time is:" + str(end_time - start_time) + "\n")
    write_file("finger", fingerRowGroups)

    # gre
    start_time = time.time()
    greRowGroups = gre.range_search(_range, args.column)
    end_time = time.time()
    print("gre_index search time is:" + str(end_time - start_time) + "\n")
    write_file("gre", greRowGroups)

    # sieve
    start_time = time.time()
    sieveRowGroups = sieve.range_search(_range, args.column)
    end_time = time.time()
    print("sieve_index search time is:" + str(end_time - start_time) + "\n")
    write_file("sieve", sieveRowGroups)

    time.sleep(20)

    # 第三步，通知node0运行脚本
    #远程删除sql
    remote_sql_dir = "/mydata/bigdata/p2pevaluation/{}/{}/sqls".format(args.wltype, args.table)
    origin_sql = _sqls[0].format(_range[0], _range[1])
    minmax_sql = _sqls[1].format(_range[0], _range[1])
    finger_sql = _sqls[2].format(_range[0], _range[1])
    gre_sql = _sqls[3].format(_range[0], _range[1])
    sieve_sql = _sqls[4].format(_range[0], _range[1])
    os.system("ssh node0 \"rm -f  {}/*\"".format(remote_sql_dir))
    os.system("ssh node0 \"touch {}/origin.sql; echo \\\"{}\\\" >> {}/origin.sql; \"".format(remote_sql_dir, origin_sql, remote_sql_dir))
    os.system("ssh node0 \"touch {}/minmax.sql; echo \\\"{}\\\" >> {}/minmax.sql; \"".format(remote_sql_dir, minmax_sql, remote_sql_dir))
    os.system("ssh node0 \"touch {}/finger.sql; echo \\\"{}\\\" >> {}/finger.sql; \"".format(remote_sql_dir, finger_sql, remote_sql_dir))
    os.system("ssh node0 \"touch {}/gre.sql; echo \\\"{}\\\" >> {}/gre.sql; \"".format(remote_sql_dir, gre_sql, remote_sql_dir))
    os.system("ssh node0 \"touch {}/sieve.sql; echo \\\"{}\\\" >> {}/sieve.sql; \"".format(remote_sql_dir, sieve_sql, remote_sql_dir))
    # 限制带宽
    os.system("/mydata/bigdata/consbandwidth.sh {}".format(bandwith))
    # 运行查询
    os.system("ssh node0 \"cd /mydata/bigdata/p2pevaluation/{}/{}; ./run.pl\"".format(args.wltype, args.table))
    #解除带宽限制
    os.system("/mydata/bigdata/clearconsband.sh")