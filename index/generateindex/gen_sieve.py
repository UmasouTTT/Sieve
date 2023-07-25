import random
import sys
import time

sys.path.append("../..")
from workload.generate_adaptive_workload import *
from index.gapListIndex.minMaxIndex import *
from index.learnedIndexDemo.IndexOnTheLake import LakeIndex, RootLakeIndex

# print("finish read workload ...")
optimizeIndex = None
# print("finish init optimized index ...")
greIndex = None
minMax = None
learnedIndex = None
fingerprints = None
workload = None

if __name__=="__main__":
    is_single_file = args.singleFile
    directory = ""
    if args.wltype == "tpch" or args.wltype == "tpcds":
        if is_single_file:
            directory = "/mydata/bigdata/data/{}_{}/{}sub/".format(args.wltype, args.sf, args.table)
        else:
            directory = "/mydata/bigdata/data/{}_{}/{}/".format(args.wltype, args.sf, args.table)
    elif args.wltype == "wiki":
        if is_single_file:
            directory = "/mydata/bigdata/data/wikipediasub/"
        else:
            directory = "/mydata/bigdata/data/wikipedia/"
    elif args.wltype == "openstreet":
            directory = "/mydata/bigdata/data/openstreet/parquet/"
            args.column = "lon"
    columns = []
    columns.append(args.column)

    for column in columns:
        # init workload
        starttime = time.time()
        learnedIndex = RootLakeIndex(directory, [column])
        learnedIndex.generateIndexes()
        endtime = time.time()
        print("for dir {}, column {}, gen sieve time is {}".format(directory, column, endtime - starttime))