import random
import sys

sys.path.append("../..")
from index.gapListIndex.gapListIndex import *
from index.gapListIndex.GRTindex import *
from index.gapListIndex.fingerprint import *
from index.optimizedIndex import *
from workload.generate_adaptive_workload import *
from index.gapListIndex.minMaxIndex import *
from index.learnedIndexDemo.IndexOnTheLake import LakeIndex, RootLakeIndex
from index.gapListIndex.reversedIndex import ReversedIndex
import time

# print("finish read workload ...")
reversedIndex = None
# print("finish init optimized index ...")
grtIndex = None
workload = None

def experiment(directory, column, num_of_exp, selectivity):

    #print("Experiment start ...")

    num_of_experiments = num_of_exp

    saved_io_than_greindex = 0
    r_saved_io_than_greindex = 0

    #print("finish init min max index ...")
    for exp in range(num_of_experiments):
        _range = workload.random_generate_range(selectivity)
        optimizeRowGroups = reversedIndex.range_search(_range, column)
        greRowGroups = grtIndex.range_search(_range, column)

        # verify result
        for rg in optimizeRowGroups:
            if rg not in greRowGroups:
                print("greRowGroups error ! for range {}".format(_range))
                print("optimizeRG:" + str(optimizeRowGroups))
                print("greRowGroups:" + str(greRowGroups))
                break

        saved_io_than_greindex += ((len(greRowGroups) - len(optimizeRowGroups)) / len(greRowGroups) if len(greRowGroups) != 0 else 0)

        r_saved_io_than_greindex += ((len(greRowGroups) - len(optimizeRowGroups)) / len(optimizeRowGroups) if len(optimizeRowGroups) != 0 else 0)

        print(
            "For range {}, optimize result is {}, greinedx is {}".format(
                _range, len(optimizeRowGroups),
                len(greRowGroups)
                ))

    print("saved greindex: {}".
          format(
                 saved_io_than_greindex / num_of_experiments,
                ))
    print(
        "r saved greindex: {}".
        format(
               r_saved_io_than_greindex / num_of_experiments))

if __name__=="__main__":
    num_of_exp = 100
    sizes = [50000]

    for size in reversed(sizes):
        is_single_file = args.singleFile
        if args.wltype == "tpch" or args.wltype == "tpcds":
            if is_single_file:
                directory = "/mydata/bigdata/data/{}_parquet_100_rewrite/{}sub/".format(args.wltype, args.table)
            else:
                directory = "/mydata/bigdata/data/{}_parquet_100_rewrite/{}/".format(args.wltype, args.table)
        elif args.wltype == "wiki":
            if is_single_file:
                directory = "/mydata/bigdata/data/wikipediasub/"
            else:
                directory = "/mydata/bigdata/data/wikipedia/"
        elif args.wltype == "SSB":
            if is_single_file:
                directory = "/mydata/bigdata/data/SSBSub/"
            else:
                directory = "/mydata/bigdata/data/SSBData/"
        elif args.wltype == "tpch-order":
            if is_single_file:
                directory = "/mydata/bigdata/data/tpch_order/{}sub/".format(args.table)
            else:
                directory = "/mydata/bigdata/data/tpch_order/{}/".format(args.table)

        workload = WorkLoad(directory)
        columns = []
        columns.append(args.column)
        for column in columns:
            # init workload
            start_time = time.time()
            grtIndex = RootGRTindex(directory, [column])
            grtIndex.generateIndex()
            end_time = time.time()
            print("grtindex create time:" + str(end_time - start_time))

            reversedIndex = ReversedIndex(directory, [column])
            reversedIndex.generateIndexFromFile()
            workload.init(column)
            selectivities = [0.00001, 0.0001, 0.001, 0.01]
            for selectivity in selectivities:
                print("**************test on selectivity {} *********************".format(selectivity))
                experiment(directory, column, num_of_exp, selectivity)