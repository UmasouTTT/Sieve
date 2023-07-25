import pickle
import random
import sys



sys.path.append("../..")

from index.pointIndex.bloomIndex import RootBloomIndex
from index.gapListIndex.reversedIndex import ReversedIndex
from index.gapListIndex.gapListIndex import *
from index.gapListIndex.GREindex import *
from index.gapListIndex.fingerprint import *
from index.optimizedIndex import *
from workload.generate_adaptive_workload import *
from index.gapListIndex.minMaxIndex import *
from index.learnedIndexDemo.IndexOnTheLake import LakeIndex
import time

if __name__=="__main__":
    reversedIndex = ReversedIndex("/mydata/bigdata/data/temp/", ["pagecount"])
    reversedIndex.generateIndexFromFile()
    bloomIndex = RootBloomIndex("/mydata/bigdata/data/temp/", ["pagecount"])
    dump_path = "bloom.temp"
    with open(dump_path, 'wb+') as f:
        pickle.dump(bloomIndex, f)
    file_path = "/mydata/bigdata/data/temp/wiki0"
    table = pp.ParquetFile(file_path)
    num_of_row_groups = table.num_row_groups
    max_value = 130603440306
    count = 0
    for i in range(100):
        rand_value = random.randint(0, max_value)
        truergs = reversedIndex.point_search(rand_value, "pagecount")
        _resrgs = bloomIndex.point_search(rand_value, "pagecount")
        assert truergs.issubset(_resrgs)
        count += (len(_resrgs) - len(truergs))/num_of_row_groups
    print(count/100)