import math
import random
import sys
import time

from index.FIT.FITingTree import FIT
from index.gapListIndex.FITMap import FITMAP
from index.gapListIndex.FITTree import FITTREE
from index.learnedIndexDemo.IndexOnTheLake import *
from index.gapListIndex.fingerprint import *
from index.gapListIndex.minMaxIndex import *

def gen_fit_key(fitindex, number_of_keys, last_rg_id, rg_bound=50000, last_rg_record_num=1):
    generated_keys = []
    records = None
    if isinstance(fitindex, FITMAP):
        for filekey in fitindex.indexs:
            records = fitindex.indexs[filekey].keys()
    elif isinstance(fitindex, FITTREE):
        for filekey in fitindex.indexs:
            records = fitindex.indexs[filekey][0]
    recordlen = len(records)
    for _ in range(number_of_keys):
        if last_rg_record_num >= rg_bound:
            last_rg_id += 1
            last_rg_record_num = 0
        _value = records[random.randint(0, recordlen)]
        generated_keys.append([_value, last_rg_id])
        last_rg_record_num += 1
    return generated_keys

def generate_finger_keys(finger, number_of_keys, last_rg_id, rg_bound=50000, last_rg_record_num=1):
    """
    实验中发现有些情况finger表现的非常好，猜测是因为生成的值不够均匀，都在前面，所以finger遍历的开销就没了
    :param finger: finger索引
    :param number_of_keys: 需要生成多少个数据
    :param last_rg_id: 插入文件的最后一个行组编号
    :param rg_bound: 一个行组的记录数阈值
    :param last_rg_record_num: 插入文件最后一个行组已有的记录数量
    :return:生成的插入负载，是一个数组，数组里面每个变量是一个数组，第一个值是插入值，第二个值是插入的行组id
    """
    for column in finger.fingerprints:
        _min = finger.fingerprints[column].minvalue
        _max = finger.fingerprints[column].maxvalue
        generated_keys = []
        for _ in range(number_of_keys):
            if last_rg_record_num >= rg_bound:
                last_rg_id += 1
                last_rg_record_num = 0
            _value = random.randint(_min, _max)
            generated_keys.append([_value, last_rg_id])
            last_rg_record_num += 1
        return generated_keys

def generate_minmax_keys(minmax, number_of_keys, last_rg_id, rg_bound=50000, last_rg_record_num=1):
    """
    :param minmax: minmax索引
    :param number_of_keys: 需要生成多少个数据
    :param last_rg_id: 插入文件的最后一个行组编号
    :param rg_bound: 一个行组的记录数阈值
    :param last_rg_record_num: 插入文件最后一个行组已有的记录数量
    :return:生成的插入负载，是一个数组，数组里面每个变量是一个数组，第一个值是插入值，第二个值是插入的行组id
    """
    _min, _max = sys.maxsize, -sys.maxsize
    for column in minmax.minmaxIndexs:
        for file in minmax.minmaxIndexs[column]:
            for rg in minmax.minmaxIndexs[column][file].columns_inf:
                _min = min(minmax.minmaxIndexs[column][file].columns_inf[rg]["range"][0], _min)
                _max = max(minmax.minmaxIndexs[column][file].columns_inf[rg]["range"][1], _max)
    generated_keys = []
    for _ in range(number_of_keys):
        if last_rg_record_num >= rg_bound:
            last_rg_id += 1
            last_rg_record_num = 0
        _value = random.randint(_min, _max)
        generated_keys.append([_value, last_rg_id])
        last_rg_record_num += 1
    return generated_keys

def generate_keys(workload, sieve, number_of_keys, last_rg_id, rg_bound=50000, last_rg_record_num=1):
    """
    :param number_of_keys: 需要生成多少个数据
    :param last_rg_id: 插入文件的最后一个行组编号
    :param rg_bound: 一个行组的记录数阈值
    :param last_rg_record_num:插入文件最后一个行组已有的记录数量
    :return:生成的插入负载，是一个数组，数组里面每个变量是一个数组，第一个值是插入值，第二个值是插入的行组id
    """
    cur_segs = []
    for column in sieve.lakeindexs:
        for file in sieve.lakeindexs[column]:
            for segement in sieve.lakeindexs[column][file].segments:
                if segement.segment_range[0] + 50 < segement.segment_range[1]:
                    cur_segs.append([segement.segment_range[0], segement.segment_range[1]])
    generated_keys = []
    innum = 20
    for _ in range(number_of_keys):
        if last_rg_record_num >= rg_bound:
            last_rg_id += 1
            last_rg_record_num = 0
        if _ % innum == 0:
            _value = workload.random_generate_point(False, cur_segs)
        else:
            _value = workload.random_generate_point()
        generated_keys.append([_value, last_rg_id])
        last_rg_record_num += 1
    return generated_keys

def insertexperiment(index, generated_keys, column, file):
    if isinstance(index, RootLakeIndex):
        for _ in generated_keys:
            index.insert(_[0], _[1], column, file)
    elif isinstance(index, RootAllFileFingerPrints):
        for _ in generated_keys:
            index.insert(_[0], _[1], column, file)
    elif isinstance(index, MinMaxIndex):
        for _ in generated_keys:
            index.insert(_[0], _[1], column, file)
    elif isinstance(index, FIT):
        for _ in generated_keys:
            index.insert(_[0], _[1], column, file)
