# 从openstreet里面获取数据，然后变成连续的不中断的数据，再映射回去
import os
import random
import shutil
import sys
import pyarrow.parquet as pp
import pyarrow as pa

sys.path.append("../../../")
from index.gapListIndex.minMaxIndex import *
from index.gapListIndex.fingerprint import *
from index.learnedIndexDemo.IndexOnTheLake import LakeIndex, RootLakeIndex
from workload.generate_adaptive_workload import *
from index.FIT.FITingTree import FIT

minMax = None
fingerprints = None
fitIndex = None

def getCurdensFile(originfile, column_name, gapsize, writedir, csvdir):
    table = pp.ParquetFile(originfile)
    num_of_row_groups = table.num_row_groups
    recordset = set()
    for row_group_index in range(num_of_row_groups):
        row_group_contents = table.read_row_group(row_group_index, columns=[column_name])
        for record in row_group_contents.column(column_name):
            recordValue = int(str(record))
            recordset.add(recordValue)
    recordlist = list(recordset)
    recordlist.sort()
    # 目前recordlist是排序好的原的数组，维护一个原数据->排序编号的map
    recordindexmap = {}
    for i in range(len(recordlist)):
        recordindexmap[recordlist[i]] = i
    ids, lons, ya = [], [], gapsize
    curid = 0
    for row_group_index in range(num_of_row_groups):
        row_group_contents = table.read_row_group(row_group_index, columns=[column_name])
        for record in row_group_contents.column(column_name):
            curindex = recordindexmap[int(str(record))]
            if gapsize >= 10:
                ya = random.randint(int(gapsize * 0.5), int(gapsize * 1.5) - 1)
            newlon = curindex * ya
            ids.append(curid)
            lons.append(newlon)
            curid += 1
    schema = pa.schema([
        ('id', pa.int64()),
        ('lon', pa.int64())
    ])
    batch = pa.RecordBatch.from_arrays([pa.array(ids, pa.int64()),
                                        pa.array(lons, pa.int64())],
                                       schema=schema)
    table = pa.Table.from_batches([batch])
    pp.write_table(table, '{}sparseworstdata.parquet'.format(writedir), row_group_size=50000)
    parquet_file = pp.ParquetFile('{}sparseworstdata.parquet'.format(writedir))
    df = parquet_file.read().to_pandas()
    df.to_csv('{}sparseworstdata.csv'.format(csvdir), index=False)

def genpointworkload(parquetdir, column):
    shutil.copy2('sparseworstpre.workload', '../../../cuckoo-index/searchlogs/sparseworst.workload')

def compare(parquetdir, column):
    global minMax, fingerprints, fitIndex
    if minMax == None:
        minMax = MinMaxIndex(parquetdir, [column])
        minMax.generateIndexes()
    if fingerprints == None:
        fingerprints = RootAllFileFingerPrints(parquetdir, [column])
        fingerprints.generateIndexes()
    learnedIndex = RootLakeIndex(parquetdir, [column])
    learnedIndex.generateIndexes()
    if fitIndex == None:
        fitIndex = FIT(parquetdir, [column], 10000, 3)
        fitIndex.generateIndexes()

    expnum = 0

    each_index_search_times = [0] * 4
    each_index_blk_num = [0] * 4
    f = open("sparseworstpre.workload", "r+", encoding="UTF-8")
    for line in f:
        expnum += 1
        _point = int(line.strip().split(":")[1])
        _range = [_point, _point]
        curtime = time.time()
        minMaxRowGroups = minMax.range_search(_range, column)
        each_index_search_times[0] += (time.time() - curtime)
        each_index_blk_num[0] += len(minMaxRowGroups)
        curtime = time.time()
        fingerprintsRG = fingerprints.range_search(_range, column)
        each_index_search_times[1] += (time.time() - curtime)
        each_index_blk_num[1] += len(fingerprintsRG)
        curtime = time.time()
        learnedRowGroups = learnedIndex.range_search(_range, column)
        each_index_search_times[2] += (time.time() - curtime)
        each_index_blk_num[2] += len(learnedRowGroups)
        curtime = time.time()
        optimizeRowGroups = fitIndex.range_search(_range, column)
        each_index_search_times[3] += (time.time() - curtime)
        each_index_blk_num[3] += len(optimizeRowGroups)
        for rg in optimizeRowGroups:
            if rg not in minMaxRowGroups:
                print("min max error ! for range {}".format(_range))
                print("optimizeRG:" + str(optimizeRowGroups))
                print("minMaxRowGroups:" + str(minMaxRowGroups))
                break
            if rg not in fingerprintsRG:
                print("fignerprintsRG error ! for range {}".format(_range))
                print("optimizeRG:" + str(optimizeRowGroups))
                print("fingerprintsRG:" + str(fingerprintsRG))
                break
            if rg not in learnedRowGroups:
                print("roughLearnedRowGroups error ! for range {}".format(_range))
                print("optimizeRG:" + str(optimizeRowGroups))
                print("roughLearnedRowGroups:" + str(learnedRowGroups))
                break
    print("minmax-finger-sieve-opt avg blk num is {}, avg search time is {}".format(
        [v/expnum for v in each_index_blk_num], [v/expnum for v in each_index_search_times]
    ))
    return

def getcuckooinfo(cuckoologdir):
    indexsize, blknum = 0, 0
    files = os.listdir(cuckoologdir)
    for file in files:
        f = open(cuckoologdir+file, "r+", encoding="UTF-8")
        for line in f:
            if "byte_size is" in line:
                indexsize += float(line.strip().split(":")[1].strip())
            if "avg blk num is" in line:
                blknum += float(line.strip().split(":")[1].strip())
        f.close()
    return indexsize, blknum

if __name__=="__main__":
    current_path = os.getcwd()
    # 密度测试如下几组：100%，50%，10%，1%，0.01%
    gapsize = 100
    writedir = "../../../dataset/sparseworstdata/"
    if not os.path.exists(writedir):
        os.makedirs(writedir)
    csvdir = "../../../cuckoo-index/sparseworstdata/"
    if not os.path.exists(csvdir):
        os.makedirs(csvdir)
    getCurdensFile("../../../dataset/Maps/1.parquet", "lon", gapsize, writedir, csvdir)
    genpointworkload(writedir, "lon")
    for error in [1, 10, 100, 1000, 10000]:
        args.segment_error = error
        compare(writedir, "lon")
    cuckoologdir = "../../../cuckoo-index/sparseworstlog/"
    if not os.path.exists(cuckoologdir):
        os.makedirs(cuckoologdir)
    os.chdir("../../../cuckoo-index/")
    os.system("chmod +x runsparseworst.sh")
    os.system("./runsparseworst.sh > temp.log 2>&1")
    os.chdir(current_path)
    cindexsize, cblknum = getcuckooinfo(cuckoologdir)
    print("cuckoo index size is:{}, avg blk num is:{}".format(cindexsize, cblknum))