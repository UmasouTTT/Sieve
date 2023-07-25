import os
import sys
import time

sys.path.append("../..")
from param import args
from index.p2phadoop.writeFilesFromExistIndex import run_main
from index.gapListIndex.GRTindex import *
from index.gapListIndex.fingerprint import *
from workload.generate_adaptive_workload import *
from index.gapListIndex.minMaxIndex import *
from index.learnedIndexDemo.IndexOnTheLake import LakeIndex, RootLakeIndex
import shutil

def write_hdfs(local_file, hdfs_file):
    os.system("hadoop dfs -put " + local_file + " " + hdfs_file)
    for i in range(1, 10):
        os.system("hadoop dfs -cp " + hdfs_file + " " + hdfs_file.split("-")[0] + "-" + str(i))

def write_file(index, rowgroups):
    start_time = time.time()
    _dict = dict()
    for rowgroup in rowgroups:
        file_path = rowgroup.split("-")[0]
        row_group = int(rowgroup.split("-")[1])
        if file_path not in _dict:
            _dict[file_path] = set()
        _dict[file_path].add(row_group)
    write_file_dir = "/mydata/bigdata/index/{}/{}/{}/".format(index, args.wltype, args.table) #本地临时保存中间文件的目录
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
    end_time = time.time()
    print(index + " write local down, process time is " + str(end_time-start_time))
    hdfs_dir = "/index/{}/{}/{}/".format(index, args.wltype, args.table)
    os.system("hadoop fs -rm -r -skipTrash " + hdfs_dir)  # 删除原有文件夹
    print(index + " hdfs dir deleted!")
    os.system("hadoop fs -mkdir -p " + hdfs_dir)  # 删除原有文件夹
    #复制10份上传文件
    start_time = time.time()
    files = os.listdir(write_file_dir)
    hdfs_put_tasks = []
    with ThreadPoolExecutor(5, thread_name_prefix='hdfs_put_pools') as pools:
        for file in files:
            local_file = write_file_dir + file
            hdfs_file = hdfs_dir + file + "-0"
            cur_task = pools.submit(write_hdfs, local_file, hdfs_file)
            hdfs_put_tasks.append(cur_task)
        wait(hdfs_put_tasks, return_when=ALL_COMPLETED)
    end_time = time.time()
    print(index + " write hdfs down, process time is" + str(end_time-start_time))

def perindexfunc(index, indextype, _range):
    start_time = time.time()
    rowGroups = index.range_search(_range, args.column)
    end_time = time.time()
    print(indextype + " search result blk num is:" + str(len(rowGroups)) + "\n")
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

start_time = time.time()
minMax = MinMaxIndex(directory, [args.column])
if isFirstRun:
    minMax.generateIndexes()
else:
    minMax.generateIndexesFromFile()
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
grt = RootGRTindex(directory, [args.column])
start_time = time.time()
if isFirstRun:
    grt.generateIndex()
else:
    grt.generateIndexFromFile()
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
    if "for range (" in line:
        tempstr = line.split(")")[0]
        tempstr = tempstr.split("(")[-1]
        values = tempstr.split(",")
        _range_list.append([int(values[0].strip()), int(values[1].strip())])
    elif "select" in line:
        _sqls.append(line.strip())
f.close()

os.system("ssh node0 \"rm -f /proj/dst-PG0/p2plog/ss_ticket_number.log\"".format(args.wltype, args.table)) #删除原有的结果
for _range in _range_list:
    # 第二步，各索引生产的子副本，上传到hdfs
    args.range_s = _range[0]
    args.range_e = _range[1]

    with ThreadPoolExecutor(5, thread_name_prefix='autorun_ss_ticket_number_pool') as pools:
        all_task = []
        all_task.append(pools.submit(perindexfunc, minMax, "minmax", _range))
        all_task.append(pools.submit(perindexfunc, fingerprints, "finger", _range))
        all_task.append(pools.submit(perindexfunc, grt, "gre", _range))
        all_task.append(pools.submit(perindexfunc, sieve, "sieve", _range))
        wait(all_task, return_when=ALL_COMPLETED)
    time.sleep(5)
    print("all index file prepared!")

    # 第三步，通知node0运行脚本
    #远程删除sql
    remote_sql_dir = "/mydata/bigdata/p2pevaluation/{}/{}/sqls".format(args.wltype, args.table)
    minmax_sql = _sqls[1].format(_range[0], _range[1])
    finger_sql = _sqls[2].format(_range[0], _range[1])
    gre_sql = _sqls[3].format(_range[0], _range[1])
    sieve_sql = _sqls[4].format(_range[0], _range[1])
    os.system("ssh node0 \"rm -f  {}/*\"".format(remote_sql_dir))
    os.system("ssh node0 \"touch {}/minmax.sql; echo \\\"{}\\\" >> {}/minmax.sql; \"".format(remote_sql_dir, minmax_sql, remote_sql_dir))
    os.system("ssh node0 \"touch {}/finger.sql; echo \\\"{}\\\" >> {}/finger.sql; \"".format(remote_sql_dir, finger_sql, remote_sql_dir))
    os.system("ssh node0 \"touch {}/gre.sql; echo \\\"{}\\\" >> {}/gre.sql; \"".format(remote_sql_dir, gre_sql, remote_sql_dir))
    os.system("ssh node0 \"touch {}/sieve.sql; echo \\\"{}\\\" >> {}/sieve.sql; \"".format(remote_sql_dir, sieve_sql, remote_sql_dir))
    # 运行查询
    os.system("ssh node0 \"cd /mydata/bigdata/p2pevaluation/{}/{}; ./run.pl\"".format(args.wltype, args.table))
