# 生成不同分段数目的数据集，仿照FITing TREE 7.6节进行实验
import math
import os
import random
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import sys

sys.path.append("../../..")
from index.FIT.FITingTree import FIT
from index.gapListIndex.minMaxIndex import *
from index.gapListIndex.fingerprint import *
from index.learnedIndexDemo.IndexOnTheLake import LakeIndex, RootLakeIndex
from workload.generate_adaptive_workload import *

minMax = None
fingerprints = None
fitIndex = None

def gen_worse_data(blen, writedir, csvdir):#变化的步长，选择度范围的变化长度
    """
    seg.error设置成100，即下面的n为100
    a = n / (1 - n/b)，a是斜率为1的横向长度，b是斜率为0的横向长度
    假设800个行组，每个行组50000个元组
    :param step_len: 变化步长，即多久变化一次row group，最小为1
    :param percent: 在0.01%选择度下行组的占比，定义为变化周期
    :param seg_error：约定的segment.error
    """
    total_seg = 0
    # alen = math.ceil(seg_error / (1 - seg_error/blen))
    alen = 200
    print(alen)
    row_group_num = 800
    per_rg_num = 50000
    min_selectivity = 0.0001
    value_num = row_group_num * per_rg_num
    cur_value = 0 # 当前value的值
    select_len = math.ceil(row_group_num * per_rg_num * min_selectivity) # 0.01%选择度下的大致长度 4000
    # select_rgnum = math.ceil(percent * row_group_num) # 在percent选择度下，行组的数量
    select_rgnum = 8 # 在percent选择度下，行组的数量
    row_groups_content = [[] for i in range(row_group_num)]
    temp_rgids = [] #本周期选中的行组号
    cur_rdid_index = 0 #目前填充行组在temp_rgids中的下标（如果有一个满了，直接用一个新的顶替temp_rgids中该位置）
    row_groups_valid_len = dict()
    for i in range(row_group_num):
        row_groups_valid_len[i] = per_rg_num
    for i in range(select_rgnum):
        temp_rgids.append(i)
    for i in range(value_num): #需要造800*5000个数据，循环这么多次
        if i != 0 and i % (alen + 1) == 0:
            cur_value += blen
            total_seg += 1
        else:
            cur_value += 1
        if i != 0 and int(i % select_len) == 0:
            # 重新选择select_rgnum个行组，但是保留cur_rdid_index那个不变
            dic1SortList = sorted(row_groups_valid_len.items(), key=lambda x: x[1], reverse=True)
            pre_rg_id = temp_rgids[cur_rdid_index % len(temp_rgids)]
            temp_rgids.clear()
            for _i in range(len(dic1SortList)):
                temp_rgids.append(dic1SortList[_i][0])
                if len(temp_rgids) >= select_rgnum:
                    break
            if temp_rgids[0] == pre_rg_id:
                cur_rdid_index = 1 % len(temp_rgids)
            else:
                cur_rdid_index = 0
            dic1SortList.clear()
        else:
            cur_rdid_index = (cur_rdid_index + 1) % len(temp_rgids)

        if row_groups_valid_len[temp_rgids[cur_rdid_index]] <= 0:
            row_groups_valid_len.pop(temp_rgids[cur_rdid_index])
            temp_rgids.remove(temp_rgids[cur_rdid_index])
            if len(temp_rgids) < 2:
                dic1SortList = sorted(row_groups_valid_len.items(), key=lambda x: x[1], reverse=True)
                for _i in range(len(dic1SortList)):
                    if dic1SortList[_i][0] not in temp_rgids:
                        temp_rgids.append(dic1SortList[_i][0])
        row_groups_content[temp_rgids[cur_rdid_index]].append(cur_value)
        row_groups_valid_len[temp_rgids[cur_rdid_index]] -= 1
    for i in range(row_group_num):
        assert len(row_groups_content[i]) == per_rg_num
    print(total_seg)
    all_rowgroups_content = []
    ids = []
    curid = 0
    for per_rowgroup_content in row_groups_content:
        all_rowgroups_content.extend(per_rowgroup_content)
        for i in range(len(per_rowgroup_content)):
            ids.append(curid)
            curid += 1

    # 定义 Schema
    schema = pa.schema([
        ('id', pa.int64()),
        ('lon', pa.int64())
    ])

    batch = pa.RecordBatch.from_arrays([pa.array(ids, pa.int64()),
                                        pa.array(all_rowgroups_content, pa.int64())],
                                       schema=schema)
    table = pa.Table.from_batches([batch])
    pq.write_table(table, '{}denseworstdata.parquet'.format(writedir),
                   row_group_size=50000)
    parquet_file = pq.ParquetFile('{}denseworstdata.parquet'.format(writedir))
    df = parquet_file.read().to_pandas()
    df.to_csv('{}denseworstdata.csv'.format(csvdir), index=False)

def genpointworkload(parquetdir, column):
    workload = WorkLoad(parquetdir)
    workload.init(column)
    _points = workload.genseries_point(500, 0.5)
    wf = open("denseworst.workload", "w+", encoding="UTF-8")
    for _point in _points:
        wf.write("point:{}\n".format(_point))
    wf.close()
    shutil.copy2('denseworst.workload', '../../../cuckoo-index/searchlogs/denseworst.workload')

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
        fitIndex = FIT(parquetdir, [column], 1000, 50)
        fitIndex.generateIndexes()
    expnum = 0

    each_index_blk_num = [0] * 4
    f = open("denseworst.workload", "r+", encoding="UTF-8")
    for line in f:
        expnum += 1
        _point = int(line.strip().split(":")[1])
        _range = [_point, _point]
        minMaxRowGroups = minMax.range_search(_range, column)
        each_index_blk_num[0] += len(minMaxRowGroups)
        fingerprintsRG = fingerprints.range_search(_range, column)
        each_index_blk_num[1] += len(fingerprintsRG)
        learnedRowGroups = learnedIndex.range_search(_range, column)
        each_index_blk_num[2] += len(learnedRowGroups)
        optimizeRowGroups = fitIndex.range_search(_range, column)
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
    print("minmax-finger-sieve-opt avg blk num is {}".format(
        [v/expnum for v in each_index_blk_num]
    ))
    f.close()
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


if __name__ == "__main__":
    current_path = os.getcwd()
    writedir = "../../../dataset/denseworstdata/"
    if not os.path.exists(writedir):
        os.makedirs(writedir)
    csvdir = "../../../cuckoo-index/denseworstdata/"
    if not os.path.exists(csvdir):
        os.makedirs(csvdir)
    gen_worse_data(200, writedir, csvdir)
    genpointworkload(writedir, "lon")
    for error in [10, 100, 1000, 10000]:
        args.segment_error = error
        compare(writedir, "lon")
    cuckoologdir = "../../../cuckoo-index/denseworstlog/"
    if not os.path.exists(cuckoologdir):
        os.makedirs(cuckoologdir)
    os.chdir("../../../cuckoo-index/")
    os.system("chmod +x rundenseworst.sh")
    os.system("./rundenseworst.sh > temp.log 2>&1")
    os.chdir(current_path)
    cindexsize, cblknum = getcuckooinfo(cuckoologdir)
    print("cuckoo index size is:{}, avg blk num is:{}".format(cindexsize, cblknum))