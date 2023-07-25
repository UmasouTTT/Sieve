# 改进算法中ideal size的估计部分，看看openstreet的点查会不会有提升
import sys
sys.path.append("../../..")
from index.gapListIndex.minMaxIndex import *
from index.gapListIndex.fingerprint import *
from index.learnedIndexDemo.IndexOnTheLake import LakeIndex, RootLakeIndex
from index.gapListIndex.reversedIndex import ReversedIndex
from workload.generate_adaptive_workload import *
def testopenstreet():
    directory = "/mydata/bigdata/data/openstreetsub/"
    column = "lon"
    args.partition_num = 100000
    args.segment_error = 1000
    args.sieve_gap_percent = 0.00003
    learnedIndex = RootLakeIndex(directory, [column])
    learnedIndex.generateIndexes()
    totallen = 0
    exptime = 0
    curpoint = -1800000000 + 7500000
    while curpoint <= 1800000000:
        exptime += 1
        totallen += len(learnedIndex.range_search([curpoint, curpoint], column))
        curpoint += int(1e7)
    print("avg len is {}".format(totallen/exptime))

def testwiki():
    directory = "/mydata/bigdata/data/wikipediasub/"
    column = "pagecount"
    args.partition_num = 100000
    args.segment_error = 100
    args.sieve_gap_percent = 0.00001
    learnedIndex = RootLakeIndex(directory, [column])
    learnedIndex.generateIndexes()
    totallen = 0
    exptime = 0
    curpoint = 5677
    while curpoint < 186005677:
        exptime += 1
        totallen += len(learnedIndex.range_search([curpoint, curpoint], column))
        curpoint += int(5e5)
    print("avg len is {}".format(totallen / exptime))

if __name__=="__main__":
    testopenstreet()
    testwiki()