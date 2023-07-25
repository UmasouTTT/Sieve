# 复现2016 VLDB Two Birds, One Stone: A Fast, yet Lightweight, Indexing Scheme for Modern Database Systems
# bucket：每个bucket命中的概率是一致的，论文3.1章节标题左列的文本说明
# 桶数目：bucket_num
# 索引项数目：index_entry_num(由阈值决定，论文5.1章节标题左列对应的公式)
# 大小粗略估计：index_entry_num * (64 * 2 + bucket_num) + bucket_num * (64 * 3)
import math
import os
import pickle
import collections
import sys
import time

from bitarray import bitarray

from index.index import *
from param import *
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, as_completed
from index.util import *

class TwoBirds(Index):
    def __init__(self, directory, columns):
        self.directory = directory
        self.hippos = {}
        self.columns = columns

    def generateIndexes(self):
        for column in self.columns:
            self.hippos[column] = Hippo(self.directory, column)
            dump_path = args.hippoDir + self.directory[1:].replace("/", "-") + column
            with open(dump_path, 'wb+') as f:
                pickle.dump(self.hippos[column], f)
            indexsize = os.path.getsize(dump_path)
            print("hippo index physic size is:{}".format(indexsize))
            print("hippo index logic size is:{}".format((len(self.hippos[column].hippo_index) * (64 * 2 + args.bucket_num) + args.bucket_num * (64 * 3)) / 8))

    def generateIndexFromFile(self):
        for column in self.columns:
            dump_path = args.hippoDir + self.directory[1:].replace("/", "-") + column
            with open(dump_path, 'rb+') as f:
                self.hippos[column] = pickle.load(f)

    def point_search(self, value, column):
        searched_blocks = self.hippos[column].point_search(self.directory, value)
        return searched_blocks

    def range_search(self, data_range, column):
        searched_blocks = self.hippos[column].range_search(self.directory,data_range)
        return searched_blocks

class Hippo():
    def __init__(self, directory, column):
        self.chb_his = dict() #完整直方图，key是bucket_id，value是范围
        self.hippo_index = list() # hippo索引结构，里面每个value的结构为 "起始行组，结束行组,起始行组到结束行组中元素所在的bucketid的bitmap"
        self.file_rg_num = dict() # 保存的各文件的行组数目
        self.init_hippo(directory, column)
        print("hippo_index len is {}".format(len(self.hippo_index)))


    def getOneFileMinMax(self, file, column):
        _min = sys.maxsize
        _max = -sys.maxsize
        table = pp.ParquetFile(file)
        # min max
        num_of_row_groups = table.num_row_groups
        for row_group_index in range(num_of_row_groups):
            row_group_contents = table.read_row_group(row_group_index, columns=[column])
            for record in row_group_contents.column(column):
                # evaluate on int first todo: add other type
                if str(record) == 'None':
                    continue
                record = getRecord(record)
                _min = min(_min, record)
                _max = max(_max, record)
        return _min, _max, file, num_of_row_groups

    def getOneFileHis(self, file, column, _min, per_bucket_len):
        table = pp.ParquetFile(file)
        # min max
        all_partial_his = []
        num_of_row_groups = table.num_row_groups
        for row_group_index in range(num_of_row_groups):
            partial_his = bitarray(args.bucket_num)
            partial_his.setall(0)
            row_group_contents = table.read_row_group(row_group_index, columns=[column])
            for record in row_group_contents.column(column):
                # evaluate on int first todo: add other type
                if str(record) == 'None':
                    continue
                record = getRecord(record)
                partial_his[int((record-_min)/per_bucket_len)] = 1
                # for key in self.chb_his:
                #     if record >= self.chb_his[key][0] and record <= self.chb_his[key][1]:
                #         partial_his[key] = 1
                #         break
            all_partial_his.append(partial_his)
        return file, all_partial_his


    def init_hippo(self, directory, column):
        # 第一步：遍历文件夹下所有文件，获取指定属性的最小值，最大值，从而构建完整直方图chb_his
        print("hippo step1 start at {}".format(time.time()))
        _min = sys.maxsize
        _max = -sys.maxsize
        all_task = []
        files = sorted(os.listdir(directory))
        with ThreadPoolExecutor(20, thread_name_prefix='two_birds_step1') as pools:
            for file in files:
                cur_task = pools.submit(self.getOneFileMinMax, directory + file, column)
                all_task.append(cur_task)
            wait(all_task, return_when=ALL_COMPLETED)
            for future in as_completed(all_task):
                _cur_min, _cur_max, file, file_rg_num = future.result()
                _min = min(_cur_min, _min)
                _max = max(_cur_max, _max)
                self.file_rg_num[file] = file_rg_num
        all_task.clear()

        per_bucket_len = math.ceil((_max - _min + 1) / args.bucket_num)
        cur_step_start = _min
        for i in range(args.bucket_num):
            self.chb_his[i] = [cur_step_start, min(cur_step_start + per_bucket_len -1, _max)]
            cur_step_start += per_bucket_len
        assert cur_step_start > _max
        print("hippo step2 start at {}".format(time.time()))
        # 第二步，每个页搞一个bitarray
        file_bitarrays = dict()
        with ThreadPoolExecutor(32, thread_name_prefix='two_birds_step2') as pools:
            for file in files:
                cur_task = pools.submit(self.getOneFileHis, directory + file, column, _min, per_bucket_len)
                all_task.append(cur_task)
            wait(all_task, return_when=ALL_COMPLETED)
            for future in as_completed(all_task):
                file_path, file_partial_hises = future.result()
                file_bitarrays[file_path] = file_partial_hises
        all_task.clear()
        # 第三步，构建hippo索引
        print("hippo step3 start at {}".format(time.time()))
        startrgid = 0
        endrgid = 0
        andbitarray = None


        for file in files:
            for cur_bitarray in file_bitarrays[directory+file]:
                if startrgid == endrgid:
                    andbitarray = cur_bitarray
                else:
                    andbitarray |= cur_bitarray
                if andbitarray.count(1) / len(andbitarray) > args.partial_histogram_density:
                    self.hippo_index.append([startrgid, endrgid, andbitarray])
                    startrgid = endrgid + 1
                    endrgid = startrgid
                else:
                    endrgid += 1
        if startrgid != endrgid:
            self.hippo_index.append([startrgid, endrgid, andbitarray])


    def point_search(self, directory, value):
        rowgroups = set()
        # 构建搜索bitarray
        search_bitarray = bitarray(args.bucket_num)
        search_bitarray.setall(0)
        for key in self.chb_his:
            if value >= self.chb_his[key][0] and value >= self.chb_his[key][1]:
                search_bitarray[key] = 1
                break
        # 获得可能存在的rgid范围列表
        valid_rgid_ranges = []
        for index_entry in self.hippo_index:
            tempbitarray = search_bitarray & index_entry[2]
            if tempbitarray.count(1) > 0:
                valid_rgid_ranges.append([index_entry[0], index_entry[1]])
        # 转换成最终需要的结果格式
        files = [directory + _file for _file in sorted(os.listdir(directory))]
        for valid_rgid_range in valid_rgid_ranges:
            for rgid in range(valid_rgid_range[0], valid_rgid_range[1] + 1):
                for key in files:
                    if rgid < self.file_rg_num[key]:
                        rowgroups.add(key + "-" + str(rgid))
                        break
                    else:
                        rgid -= self.file_rg_num[key]
        return rowgroups



    def range_search(self, directory, _range):
        rowgroups = set()
        # 构建搜索bitarray
        search_bitarray = bitarray(args.bucket_num)
        search_bitarray.setall(0)
        for key in self.chb_his:
            if range_overlap(_range, self.chb_his[key][0], self.chb_his[key][1]):
                search_bitarray[key] = 1
        # 获得可能存在的rgid范围列表
        valid_rgid_ranges = []
        for index_entry in self.hippo_index:
            tempbitarray = search_bitarray & index_entry[2]
            if tempbitarray.count(1) > 0:
                valid_rgid_ranges.append([index_entry[0], index_entry[1]])
        # 转换成最终需要的结果格式
        files = [directory + _file for _file in sorted(os.listdir(directory))]
        for valid_rgid_range in valid_rgid_ranges:
            for rgid in range(valid_rgid_range[0], valid_rgid_range[1] + 1):
                for key in files:
                    if rgid < self.file_rg_num[key]:
                        rowgroups.add(key + "-" + str(rgid))
                        break
                    else:
                        rgid -= self.file_rg_num[key]
        return rowgroups
