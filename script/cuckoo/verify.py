# 用于验证cuckoo结果的正确性，即必须包含真正有的结果
# 验证结果：没问题，主要注意：第一，cuckoo不管最后一个没有满5w个数据的元组；第二，表示数据是否存在的1/0是倒着的，就第一个表示的是最后一个元组有没有。
import random
import sys

sys.path.append("../..")
from workload.generate_adaptive_workload import *
from index.gapListIndex.minMaxIndex import *
from index.gapListIndex.reversedIndex import ReversedIndex
import time

# print("finish read workload ...")
optimizeIndex = None
# print("finish init optimized index ...")
greIndex = None
minMax = None
learnedIndex = None
fingerprints = None
workload = None

if __name__=="__main__":
    directory = "/mydata/bigdata/data/tpch_parquet_100/partsuppsub/"
    column = "partkey"
    optimizeIndex = ReversedIndex(directory, [column])
    optimizeIndex.generateIndexFromFile(column)
    f = open("/mydata/cuckoo-index/cuckoolog/partsupp1.csv.log", "r+", encoding="UTF-8")
    for line in f:
        if "for search range" in line:
            searchrange = line.split(",")[0].split()[-1]
            ss = int(searchrange.split("-")[0])
            se = int(searchrange.split("-")[1])
            trueres = optimizeIndex.range_search([ss, se], column)
            cuckoores = line.strip().split(":")[-1].strip()
            for truere in trueres:
                if cuckoores[740-int(truere.strip().split("-")[-1])] == "1":
                    continue
                else:
                    print("error for range {}".format([ss, se]))
                    print(truere)
                    print(cuckoores)
    f.close()