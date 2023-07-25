import random
import sys

sys.path.append("../..")
from index.gapListIndex.fingerprint import *
from index.gapListIndex.minMaxIndex import *
from index.learnedIndexDemo.IndexOnTheLake import LakeIndex, RootLakeIndex
from index.gapListIndex.reversedIndex import ReversedIndex
import time

optimizeIndex = None
minMax = None
learnedIndex = None
fingerprints = None

if __name__ == "__main__":
    is_single_file = args.singleFile
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
        directory = "/mydata/bigdata/data/openstreet/".format(args.sf)
        args.column = "lon"
    elif args.wltype == "worse_data":
        directory = "/mydata/bigdata/data/worse_data/"
        args.column = "liuid"

    columns = []
    columns.append(args.column)
    for column in columns:
        # init workload
        fingerprints = RootAllFileFingerPrints(directory, [column])
        start_time = time.time()
        fingerprints.generateIndexes()
        end_time = time.time()
        print("fingerprint gen time is:" + str(end_time - start_time))