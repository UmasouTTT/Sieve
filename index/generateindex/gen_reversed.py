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
    directory = ""
    if args.wltype == "tpch" or args.wltype == "tpcds":
        if is_single_file:
            directory = "/mydata/bigdata/data/{}_parquet_{}/{}sub/".format(args.wltype, args.sf, args.table)
        else:
            directory = "/mydata/bigdata/data/{}_parquet_{}/{}/".format(args.wltype, args.sf, args.table)
    elif args.wltype == "wiki":
        if is_single_file:
            directory = "/mydata/bigdata/data/wikipediasub/"
        else:
            directory = "/mydata/bigdata/data/wikipedia/"
    elif args.wltype == "openstreet":
        directory = "/mydata/bigdata/data/openstreet/parquet/"
        args.column = "lon"
    elif args.wltype == "worse_data":
        directory = "/mydata/bigdata/data/worse_data/"
        args.column = "liuid"
    directory = "/mydata/bigdata/data/tpcds_1000/store_sales/"
    args.column = "ss_ticket_number"
    columns = []
    columns.append(args.column)
    for column in columns:
        starttime = time.time()
        optimizeIndex = ReversedIndex(directory, [column])
        optimizeIndex.generateIndexes()
        endtime = time.time()
        print("for dir {}, column {}, gen reversed time is {}".format(directory, column, endtime - starttime))