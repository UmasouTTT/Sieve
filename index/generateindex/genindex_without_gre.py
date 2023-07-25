import random
import sys



sys.path.append("../..")
from index.gapListIndex.fingerprint import *
from index.gapListIndex.minMaxIndex import *
from index.learnedIndexDemo.IndexOnTheLake import LakeIndex, RootLakeIndex
from index.gapListIndex.reversedIndex import ReversedIndex
from index.gapListIndex.GREindex import RootGREindex
import time

optimizeIndex = None
minMax = None
learnedIndex = None
fingerprints = None

if __name__ == "__main__":
    args.segment_error = 70000
    args.num_of_gre_gap = 100
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
        if is_single_file:
            directory = "/mydata/bigdata/data/openstreetsub/"
        else:
            directory = "/mydata/bigdata/data/openstreet/"
        args.column = "lon"
    elif args.wltype == "worse_data":
        directory = "/mydata/bigdata/data/worse_data/"
        args.column = "liuid"

    columns = []
    columns.append(args.column)
    for column in columns:
        # init workload
        # learnedIndex = RootLakeIndex(directory, [column])
        # start_time = time.time()
        # learnedIndex.generateIndexes()
        # end_time = time.time()
        # print("sieve gen time is:" + str(end_time - start_time))

        fingerprints = RootAllFileFingerPrints(directory, [column])
        start_time = time.time()
        fingerprints.generateIndexes()
        end_time = time.time()
        print("fingerprint gen time is:" + str(end_time - start_time))

        # minMax = MinMaxIndex(directory, [column])
        # start_time = time.time()
        # minMax.generateIndexes()
        # end_time = time.time()
        # print("minmax create time:" + str(end_time - start_time))

        # greIndex = RootGREindex(directory, [column])
        # start_time = time.time()
        # greIndex.generateIndexes()
        # end_time = time.time()
        # print("gre create time:" + str(end_time - start_time))