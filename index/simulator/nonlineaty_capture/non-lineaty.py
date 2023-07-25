# 日志直接丢进draw pic项目里面
import os
import random
import sys

sys.path.append("../../../")
from index.gapListIndex.reversedIndex import ReversedIndex
from workload.generate_adaptive_workload import *
from index.gapListIndex.minMaxIndex import *
from index.learnedIndexDemo.IndexOnTheLake import LakeIndex, RootLakeIndex
import time

# print("finish read workload ...")
optimizeIndex = None
# print("finish init optimized index ...")
learnedIndex = None
workload = None

def experiment(directory, column, num_of_exp, selectivity):
    num_of_experiments = num_of_exp

    total_sieve = 0
    total_opt = 0

    #print("finish init min max index ...")
    for exp in range(num_of_experiments):
        _range = workload.random_generate_range(selectivity)
        optimizeRowGroups = optimizeIndex.range_search(_range, column)
        roughLearnedRowGroups = learnedIndex.range_search(_range, column)


        # verify result
        for rg in optimizeRowGroups:
            if rg not in roughLearnedRowGroups:
                print("roughLearnedRowGroups error ! for range {}".format(_range))
                print("optimizeRG:" + str(optimizeRowGroups))
                print("roughLearnedRowGroups:" + str(roughLearnedRowGroups))
                break

        total_opt += len(optimizeRowGroups)
        total_sieve += len(roughLearnedRowGroups)

    print("avg sieve is {} and avg opt is {}".format(total_sieve/num_of_exp, total_opt/num_of_exp))


if __name__=="__main__":
    num_of_exp = 200
    firstdirs = os.listdir("/mydata/bigdata/data/nonline/")
    for i in range(len(firstdirs)):
        firstdirs[i] = "/mydata/bigdata/data/nonline/" + firstdirs[i] + "/"
    for firstdir in firstdirs:
        directory = firstdir
        print("cur directory is {}".format(directory))
        workload = WorkLoad(directory)
        columns = []
        if "openstreet" in directory:#105.parquet
            columns.append("lon")
        elif "ss" in directory:#0.parquet
            columns.append("ss_ticket_number")
        elif "wiki" in directory:#wiki0.parquet
            columns.append("pagecount")
        elif "cs" in directory:
            columns.append("cs_order_number")
        elif "lo" in directory:
            columns.append("orderkey")
        elif "partsupp" in directory:
            columns.append("partkey")
        for column in columns:
            # init workload
            workload.init(column)
            for zhishu in range(1, 6):
                for xishu in range(10):
                    args.segment_error = int(pow(10, zhishu) * xishu)
                    print("segment_error is {} start".format(args.segment_error))
                    # learnde index
                    learnedIndex = RootLakeIndex(directory, [column])
                    learnedIndex.generateIndexes()
                    # selectivities = [0.00001, 0.0001, 0.001]
                    # for selectivity in selectivities:
                    #     print("**************test on selectivity {} *********************".format(selectivity))
                    #     experiment(directory, column, num_of_exp, selectivity)
            args.segment_error = int(pow(10, 6))
            print("segment_error is {} start".format(args.segment_error))
            # learnde index
            learnedIndex = RootLakeIndex(directory, [column])
            learnedIndex.generateIndexes()