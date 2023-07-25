# 验证通过generateFromFile的正确性
import pickle
import random
import sys

sys.path.append("../..")
from index.gapListIndex.GREindex import *
from index.gapListIndex.fingerprint import *
from workload.generate_adaptive_workload import *
from index.gapListIndex.minMaxIndex import *
from index.learnedIndexDemo.IndexOnTheLake import LakeIndex, RootLakeIndex
import time

if __name__=="__main__":
    directory = ""
    if args.wltype == "tpch" or args.wltype == "tpcds":
        directory = "/mydata/bigdata/data/{}_parquet_100_rewrite/{}/".format(args.wltype, args.table)
    elif args.wltype == "wiki":
        directory = "/mydata/bigdata/data/wikipedia/"
    elif args.wltype == "SSB":
        directory = "/mydata/bigdata/data/SSBData/"
    elif args.wltype == "tpch-order":
        directory = "/mydata/bigdata/data/tpch_order/{}/".format(args.table)
    column = args.column
    with open("_range", 'rb+') as f:
        _range = pickle.load(f)
    fingerprints = RootAllFileFingerPrints(directory, [column])
    fingerprints.generateIndexFromFile()
    gre = RootGREindex(directory, [column])
    gre.generateIndexFromFile()
    sieve = RootLakeIndex(directory, [column])
    sieve.generateIndexesFromFile()
    with open("fingerRowGroups", 'rb+') as f:
        prefingerRowGroups = pickle.load(f)
    with open("greRowGroups", 'rb+') as f:
        pregreRowGroups = pickle.load(f)
    with open("sieveRowGroups", 'rb+') as f:
        presieveRowGroups = pickle.load(f)
    fingerRowGroups = fingerprints.range_search(_range, column)
    assert prefingerRowGroups == fingerRowGroups
    greRowGroups = gre.range_search(_range, column)
    assert greRowGroups == pregreRowGroups
    sieveRowGroups = sieve.range_search(_range, column)
    assert sieveRowGroups == presieveRowGroups