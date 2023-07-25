import random
import sys

sys.path.append("../..")
from index.gapListIndex.minMaxIndex import *
from index.gapListIndex.two_birds import TwoBirds
import time

greIndex = None

if __name__=="__main__":
    is_single_file = args.singleFile
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

    columns = []
    columns.append(args.column)
    for column in columns:
        twobirdsIndex = TwoBirds(directory, [column])
        twobirdsIndex.generateIndexes()