import numpy as np
from datasketch import MinHash
import datetime
import os
import pyarrow.parquet as pp
import functools
# 不大于 cardinality 不小于 block num
# 对每个parquet文件构建
def jaccard(sets, range):
    # todo 采样只采样指定range的
    if 1 == len(sets):
        return 0
    # 随机len(sets)次采样
    sampled_jaccards = 0
    for _ in range(len(sets)):
        sampled_sets = np.random.choice(range(len(sets)), size=2, replace=False)
        # 计算min hash
        m1, m2 = MinHash(), MinHash()
        for d in sets[sampled_sets[0]]:
            m1.update(d.encode('utf8'))
        for d in sets[sampled_sets[1]]:
            m2.update(d.encode('utf8'))
        sampled_jaccards += m1.jaccard(m2)
    return sampled_jaccards/len(sets)

def iou(sets):
    return

def block_join(set, min, max):
    if set.max < min:
        return False
    if set.min > max:
        return False
    return True

def point_cover(range, value):
    if range[0] <= value and range[1] >= value:
        return True
    return False

def point_cover_left_close_right_open(range, value):
    return range[0] <= value < range[1]

def point_cover_without_include(range, value):
    if range[0] < value and range[1] > value:
        return True
    return False

def range_overlap(range, min, max):
    if range[1] < min:
        return False
    if range[0] > max:
        return False
    return True

def range_overlap_left_close_right_open(range, min, max):
    if range[1] <= min:
        return False
    if range[0] > max:
        return False
    return True

def range_in(range, _range):
    if range[0] < _range[0] and range[1] > _range[1]:
        return True
    else:
        return False

def compare_gaps(gap1, gap2):
    gap1_size = gap1[-1] - gap1[0]
    gap2_size = gap2[-1] - gap2[0]
    if gap1_size > gap2_size:
        return -1
    return 1

def compare_gaps_start(gap1, gap2):
    gap1_size = gap1[-1] - gap1[0]
    gap2_size = gap2[-1] - gap2[0]
    if gap1[0] < gap2[0]:
        return -1
    if gap1[0] == gap2[0] and gap1_size < gap2_size:
        return -1
    return 1



def value_in(num_range, value):
    if value >= num_range[0] and value <= num_range[1]:
        return True
    return False

def binary_search(sorted_regions, value):
    search_regions = sorted_regions
    while True:
        region_index = int(len(search_regions) / 2)
        if value_in(search_regions[region_index].range, value):
            return search_regions[region_index]
        else:
            if value < search_regions[region_index].min:
                search_regions = search_regions[:region_index]
            else:
                search_regions = search_regions[region_index + 1:]
            # check
            if len(search_regions) == 1:
                print("value not in region!, error!")
                exit(0)

def sparse_degree(row_groups, region_range):
    avg_cardinality = 0
    for row_group in row_groups:
        cardinality = row_group.cardinality_in_range(region_range)
        avg_cardinality += cardinality / (region_range[1] - region_range[0])
    avg_cardinality /= len(row_groups)
    return avg_cardinality

def read_data(directory, attribute):
    record_num = {}
    data = set()
    num_of_records = 0

    files = os.listdir(directory)
    files = [directory + _ for _ in files]

    whole_cardinality = set()

    cardinality_per_block = {}

    for file in files:
        table = pp.ParquetFile(file)
        metadata = pp.read_metadata(file)
        # print(metadata)
        # print(metadata.row_group(0))
        num_of_row_groups = table.num_row_groups
        for row_group_index in range(num_of_row_groups):
            cardinality_per_block[row_group_index] = set()
            row_group_contents = table.read_row_group(row_group_index, columns=[attribute])
            for record in row_group_contents.column(attribute):
                # evaluate on int first todo: add other type
                if str(record) == 'None':
                    continue
                # record = int(str(record))
                record = getRecord(record)
                whole_cardinality.add(record)
                cardinality_per_block[row_group_index].add(record)
                data.add(record)
                if record not in record_num:
                    record_num[record] = 0
                record_num[record] += 1
                num_of_records += 1

    data = list(data)
    data.sort()
    return record_num, data, num_of_records

def read_row_group(directory, attribute, row_group_index):

    data = set()


    files = os.listdir(directory)
    files = [directory + _ for _ in files]


    for file in files:
        table = pp.ParquetFile(file)
        row_group_contents = table.read_row_group(row_group_index, columns=[attribute])
        for record in row_group_contents.column(attribute):
            # evaluate on int first todo: add other type
            if str(record) == 'None':
                continue
            # record = int(str(record))
            record = getRecord(record)
            data.add(record)

    data = list(data)
    data.sort()
    # min = data[0]
    # max = data[-1]
    # tmp_data = list()
    # for _ in data:
    #     if 327798900 < int(_) < 328417000:
    #         tmp_data.append(_)
    #
    # a = 1
    return data

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass

    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass

    return False

def getRecord(record):
    record = str(record)
    if is_number(record):
        record = int(record)
    else:
        cur_date = datetime.date(int(record.split("-")[0]), int(record.split("-")[1]),
                                 int(record.split("-")[2]))
        start_date = datetime.date(1970, 1, 1)
        record = (cur_date - start_date).days
    return record
#read_data("/Users/umasou/workSpace/pyProjects/learnedIndexForDataLake/dataManager/tpch/data_50000/", "quantity")

#read_row_group("/Users/umasou/workSpace/pyProjects/learnedIndexForDataLake/dataManager/tpch/data_50000/", "orderkey", 544)


