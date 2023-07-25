# 自动化的测试脚本
# 输入是一系列的range，希望得到每个range在每种索引下的执行时间。所以，外层循环是ranges，内层循环是索引。每层循环逻辑如下:
# 重新启动trino
# node4：把当前索引过滤的hdfs块起始位置集合，rowgroup序号集合写入指定文件（/proj/dst-PG0）
# node0:准备好查询的sql（select xxx from table where xxx and xxx）
# 运行查询
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

# 给定parquet文件路径及行组集合，返回其对应的hdfs块的起始位置集合（4MB/块）
def getValidHdfs(file, rgs):
    result_set = set()
    metadata = pp.read_metadata(file)
    for rg in rgs:
        cur_data_page_offset = metadata.row_group(rg).column(0).data_page_offset
        result_set.add(int(cur_data_page_offset/(4*1024*1024)) * 4*1024*1024)
    return result_set

def writeVaildInfo(index, indextype, _range):
    write_path = "/proj/dst-PG0/p2pevaluate/validinfo.txt"
    hdfscount = 0
    f = open(write_path, "w+", encoding="UTF-8")
    rowGroups = index.range_search(_range, args.column)
    rgcount = len(rowGroups)
    validRGdict = dict()
    for validRG in rowGroups:
        filename = validRG.split("-")[0]
        RGindex = int(validRG.split("-")[1])
        if filename not in validRGdict:
            validRGdict[filename] = set()
        validRGdict[filename].add(RGindex)
    for file_path in validRGdict:
        curValidHdfs = getValidHdfs(file_path, validRGdict[file_path])
        hdfscount += len(curValidHdfs)
        file_name = file_path.split("/")[-1].strip()
        f.write("{}:{};{}\n".format(file_name, str(curValidHdfs)[1:-1], str(validRGdict[file_path])[1:-1]))
    f.close()
    print("for range {}, for index {}, hdfscount is {}, rgcount is {}".format(_range, indextype, hdfscount, rgcount))

if __name__=="__main__":
    args.wltype = "tpch"
    args.table = "lineitem"
    args.column = "orderkey"
    simulate_file = "tpch-lineitem-orderkey.log"
    isFirstRun = args.isFirstRun

    if args.wltype == "tpch" or args.wltype == "tpcds":
        directory = "/mydata/bigdata/data/{}_parquet_100_rewrite/{}/".format(args.wltype, args.table)
    elif args.wltype == "wiki":
        directory = "/mydata/bigdata/data/wikipedia/"
    elif args.wltype == "SSB":
        directory = "/mydata/bigdata/data/SSBData/"
    elif args.wltype == "tpch-order":
        directory = "/mydata/bigdata/data/tpch_order/{}/".format(args.table)

    minMax = MinMaxIndex(directory, [args.column])
    if isFirstRun:
        minMax.generateIndexes()
    else:
        minMax.generateIndexesFromFile()

    fingerprints = RootAllFileFingerPrints(directory, [args.column])
    if isFirstRun:
        fingerprints.generateIndexes()
    else:
        fingerprints.generateIndexFromFile()

    gre = RootGREindex(directory, [args.column])
    if isFirstRun:
        gre.generateIndexes()
    else:
        gre.generateIndexFromFile()

    sieve = RootLakeIndex(directory, [args.column])
    if isFirstRun:
        sieve.generateIndexes()
    else:
        sieve.generateIndexesFromFile()

    # 第一步，获得查询范围
    _range_list = []
    sql_template = None
    f = open(simulate_file, "r+", encoding="UTF-8")
    for line in f:
        if "for range (" in line:
            tempstr = line.split(")")[0]
            tempstr = tempstr.split("(")[-1]
            values = tempstr.split(",")
            _range_list.append([int(values[0].strip()), int(values[1].strip())])
        elif "select" in line:
            sql_template = line.strip()
    f.close()

    index_list = [minMax, fingerprints, sieve]
    indexname_list = ["minmax", "finger", "sieve"]
    os.system("rm -f /proj/dst-PG0/p2pevaluate/logs/lineitem_orderkey.log".format(args.wltype, args.table))#删除原有的结果
    for _range in _range_list:
        _sql = sql_template.strip().format(_range[0], _range[1])
        os.system("rm -f /proj/dst-PG0/p2pevaluate/sqls/lineitem_orderkey.sql; touch /proj/dst-PG0/p2pevaluate/sqls/lineitem_orderkey.sql; echo \"{}\" >> /proj/dst-PG0/p2pevaluate/sqls/lineitem_orderkey.sql;".format(_sql))
        for i in range(len(index_list)):
            os.system("ssh node0 \"/mydata/bigdata/restarttrino.sh\"")
            time.sleep(30)
            writeVaildInfo(index_list[i], indexname_list[i], _range)
            os.system("ssh node0 \"cd /proj/dst-PG0/p2pevaluate/; ./run_lineitem_orderkey.pl\"")