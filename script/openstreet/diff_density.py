# 从openstreet里面获取数据，然后变成连续的不中断的数据，再映射回去
import os
import random
import sys
import pyarrow.parquet as pp
import pyarrow as pa
sys.path.append("../..")
from index.gapListIndex.minMaxIndex import *
from index.gapListIndex.fingerprint import *
from index.learnedIndexDemo.IndexOnTheLake import LakeIndex, RootLakeIndex
from index.gapListIndex.reversedAll import ReversedAllIndex
from workload.generate_adaptive_workload import *

def getCurdensFile(originfile, column_name, gapsize):
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
    # 写入新的parquet文件
    newparquetdir = "/mydata/bigdata/data/diff_density/{}/".format(gapsize)
    if not os.path.exists(newparquetdir):
        os.makedirs(newparquetdir)
    schema = pa.schema([
        ('id', pa.int64()),
        ('lon', pa.int64())
    ])
    batch = pa.RecordBatch.from_arrays([pa.array(ids, pa.int64()),
                                        pa.array(lons, pa.int64())],
                                       schema=schema)
    table = pa.Table.from_batches([batch])
    pp.write_table(table, newparquetdir + str(gapsize) + ".parquet", row_group_size=50000)

# 把上一个函数写的parquet文件转csv文件
def parquet2csv(gapsize):
    parquetdir = "/mydata/bigdata/data/diff_density/{}/".format(gapsize)

def compare(parquetdir, column):
    minMax = MinMaxIndex(parquetdir, [column])
    minMax.generateIndexFromFile()
    fingerprints = RootAllFileFingerPrints(parquetdir, [column])
    fingerprints.generateIndexFromFile()
    learnedIndex = RootLakeIndex(parquetdir, [column])
    learnedIndex.generateIndexes()
    optimizeIndex = ReversedAllIndex(parquetdir, [column])
    optimizeIndex.generateIndexFromFile()
    workload = WorkLoad(parquetdir)
    workload.init(column)
    each_selectivity_expnum = 200
    for selectivity in [0.00001]:
        each_index_search_times = [0] * 4
        each_index_blk_num = [0] * 4
        # _ranges = workload.genseries_range(selectivity, each_selectivity_expnum)
        _points = workload.genseries_point(500)
        for _point in _points:
            _range = [_point, _point]
            curtime = time.time()
            optimizeRowGroups = optimizeIndex.point_search(_point, column)
            each_index_search_times[0] += (time.time() - curtime)
            each_index_blk_num[0] += len(optimizeRowGroups)
            curtime = time.time()
            minMaxRowGroups = minMax.range_search(_range, column)
            each_index_search_times[1] += (time.time() - curtime)
            each_index_blk_num[1] += len(minMaxRowGroups)
            curtime = time.time()
            fingerprintsRG = fingerprints.range_search(_range, column)
            each_index_search_times[2] += (time.time() - curtime)
            each_index_blk_num[2] += len(fingerprintsRG)
            curtime = time.time()
            learnedRowGroups = learnedIndex.range_search(_range, column)
            each_index_search_times[3] += (time.time() - curtime)
            each_index_blk_num[3] += len(learnedRowGroups)
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
            print("for search range {}, minmaxlen is {}, fingerprint len is {},"
                  "sieve len is {}, opt len is {}".format(_range, len(minMaxRowGroups),
                   len(fingerprintsRG), len(learnedRowGroups), len(optimizeRowGroups)))
        print("for selectivity {}, opt-minmax-finger-sieve avg blk num is {}, avg search time is {}".format(
            selectivity, [v/len(_points) for v in each_index_blk_num], [v/len(_points) for v in each_index_blk_num]
        ))
    return

if __name__=="__main__":
    # 密度测试如下几组：100%，50%，10%，1%，0.01%
    # gapsizes = [1, 10, 100, 1000]
    gapsizes = [100]
    # for gapsize in gapsizes:
    #     getCurdensFile("/mydata/bigdata/data/openstreet/parquet/1.parquet", "lon", gapsize)
    for gapsize in gapsizes:
        for error in [1, 10, 100, 1000, 10000, 100000]:
            args.segment_error = error
            parquetdir = "/mydata/bigdata/data/diff_density/{}/".format(gapsize)
            compare(parquetdir, "lon")