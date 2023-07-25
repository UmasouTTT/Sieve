import shutil
import sys

sys.path.append("../../..")
from workload.generate_adaptive_workload import *
from workload.insert_workload import *
from index.gapListIndex.minMaxIndex import *
from index.learnedIndexDemo.IndexOnTheLake import LakeIndex, RootLakeIndex
from index.FIT.FITingTree import *
import time

def indexInsert(testindexes, key_rgs, filepath, column, pointsearchvs):
    inserttimes = []
    searchtimes = []
    for testindex in testindexes:
        ctime = time.time()
        insertexperiment(testindex, key_rgs, column, filepath)
        inserttimes.append(time.time() - ctime)
        ctime = time.time()
        for pointsearchv in pointsearchvs:
            testindex.point_search(pointsearchv, column)
        searchtimes.append((time.time() - ctime)/len(pointsearchvs))
    print("for insert percent {}, index insert time is {}, search time is {}".format(
        len(key_rgs)/(50000*800), inserttimes, searchtimes
    ))

if __name__=="__main__":
    datasetdir = "../../../dataset/insertdata/"
    if not os.path.exists(datasetdir):
        os.mkdir(datasetdir)
    shutil.copy("../../../dataset/Maps/0.parquet", datasetdir)
    column = "lon"
    minmaxindex = MinMaxIndex(datasetdir, [column])
    minmaxindex.generateIndexes()
    fingerprints = RootAllFileFingerPrints(datasetdir, [column])
    fingerprints.generateIndexes()
    args.isinsert = True
    args.segment_error = 70000
    sieve_0 = RootLakeIndex(datasetdir, [column])
    sieve_0.generateIndexes()
    fitingtree = FIT(datasetdir, [column])
    fitingtree.generateIndexes()
    searhnum = 200
    workload = WorkLoad(datasetdir)
    workload.init(column)
    pointsearchvs = workload.genseries_point(searhnum)
    gennums = [400, 4000, 40000, 400000, 4000000, 8000000, 20000000]
    filepath = datasetdir + os.listdir(datasetdir)[0]
    for gennum in gennums:
        testindexes = [copy.deepcopy(minmaxindex), copy.deepcopy(fingerprints), copy.deepcopy(sieve_0), copy.deepcopy(fitingtree)]
        key_rgs = generate_keys(workload, sieve_0, gennum, 799, 50000, 50000)
        indexInsert(testindexes, key_rgs, filepath, column, pointsearchvs)