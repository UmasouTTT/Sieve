import random
import sys



sys.path.append("../..")
from index.gapListIndex.minMaxIndex import *
from index.gapListIndex.GREindex import RootGREindex
import time

greIndex = None

if __name__=="__main__":
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
        directory = "/mydata/bigdata/data/openstreet/"
        args.column = "lon"
    elif args.wltype == "worse_data":
        directory = "/mydata/bigdata/data/worse_data/"
        args.column = "liuid"

    columns = []
    columns.append(args.column)
    for column in columns:
        for num_of_gre_gap in range(80, 500, 10):
            args.num_of_gre_gap = num_of_gre_gap
            print("args.num_of_gre_gap is {}".format(args.num_of_gre_gap))
            # init workload
            greIndex = RootGREindex(directory, [column])
            start_time = time.time()
            greIndex.generateIndexes()
            end_time = time.time()
            print("For num_of_gre_gap {}, grt_table_len is {}, gre gen time is:{}".format(args.num_of_gre_gap, len(greIndex.greIndexs[column].GRT_ranges), str(end_time - start_time)))
            if len(greIndex.greIndexs[column].GRT_ranges) >= args.num_of_sub_ranges:
                break