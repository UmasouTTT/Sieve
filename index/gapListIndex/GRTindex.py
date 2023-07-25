# GRTindex.py 每个文件作为一个创建索引的单位，属于改良版本
import copy
import pickle
import sys
import bisect

import pyarrow.parquet as pp
import os
from index.index import *
from param import *
from index.util import *
import functools
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, as_completed
import time

class RootGRTindex(Index):
    def __init__(self, directory, columns):
        super(RootGRTindex, self).__init__(directory, columns)
        self.directory = directory
        self.greIndexs = {column: {} for column in columns}
        self.columns = columns

    def generatePerFile(self, column, file):
        self.greIndexs[column][file] = GRTindex(file, column)
        dump_path = args.grtIndexDir + file[1:].replace("/", "-") + column
        with open(dump_path, 'wb+') as f:
            pickle.dump(self.greIndexs[column][file], f)


    def generateIndex(self):
        files = os.listdir(self.directory)
        files = [self.directory + _ for _ in files]
        all_task = []
        with ThreadPoolExecutor(20, thread_name_prefix='GRT_gen_thread_pool') as pools:
            for column in self.columns:
                for file in files:
                    cur_task = pools.submit(self.generatePerFile, column, file)
                    all_task.append(cur_task)
            wait(all_task, return_when=ALL_COMPLETED)

    def generateIndexFromFile(self):
        files = os.listdir(self.directory)
        files = [self.directory + _ for _ in files]
        for column in self.columns:
            for file in files:
                dump_path = args.grtIndexDir + file[1:].replace("/", "-") + column
                with open(dump_path, 'rb+') as f:
                    self.greIndexs[column][file] = pickle.load(f)

    def point_search(self, value, column):
        searched_blocks = set()
        files = os.listdir(self.directory)
        files = [self.directory + _ for _ in files]
        for file in files:
            searched_blocks = searched_blocks.union(self.greIndexs[column][file].point_search(value))
        return searched_blocks

    def range_search(self, data_range, column):
        searched_blocks = set()
        files = os.listdir(self.directory)
        files = [self.directory + _ for _ in files]
        for file in files:
            searched_blocks = searched_blocks.union(self.greIndexs[column][file].range_search(data_range))
        return searched_blocks

class GRTindex:
    def __init__(self, file, column):
        self.file = file
        self.column = column
        self.column_inf = {}
        self.GRT_ranges = []
        self.GRT_row_group = {}
        self.init_gap_list()

    def init_gap_list(self):
        # todo get range by query process
        if args.file_type == "Parquet":
            self.extractGRT()
        else:
            print("Wrong file type : {}".format(parser.file_type))

    def extractGRT(self):
        GRT_ranges = [] #候选子区间集合
        table = pp.ParquetFile(self.file)
        num_of_row_groups = table.num_row_groups
        for row_group_index in range(num_of_row_groups):
            row_group_contents = table.read_row_group(row_group_index, columns=[self.column])
            row_group_records = set()
            for record in row_group_contents.column(self.column):
                if str(record) == 'None':
                    continue
                record = getRecord(record)
                row_group_records.add(record)
            self.column_inf[row_group_index] = {}
            row_group_records = list(row_group_records)
            row_group_records.sort()
            self.column_inf[row_group_index]["range"] = [row_group_records[0], row_group_records[-1]]
            sub_partitions = []
            num_of_gap_lists = args.num_of_gre_gap  # 为了加快生成
            gaps = []
            for _ in range(len(row_group_records) - 1):
                # todo: for simple, present is only int
                if row_group_records[_ + 1] - row_group_records[_] > args.int_interval:
                    gaps.append([row_group_records[_], row_group_records[_ + 1]])
            gaps.sort(key=functools.cmp_to_key(compare_gaps))
            gaps = gaps[0:num_of_gap_lists]
            start = row_group_records[0]
            gaps.sort(key=functools.cmp_to_key(compare_gaps_start))
            for gap in gaps:
                sub_partitions.append([start, gap[0]])
                start = gap[-1]
            sub_partitions.append([start, row_group_records[-1]])
            GRT_ranges.extend(sub_partitions)
            self.column_inf[row_group_index]["record"] = copy.deepcopy(row_group_records)

        # 整体去交集，获得一个不相交的区间集合
        GRT_ranges.sort(key=functools.cmp_to_key(compare_gaps_start))
        new_GRT_ranges = []
        for _range in GRT_ranges:
            if self.rangeWithin(_range, new_GRT_ranges):
                continue
            elif self.rangeOverLap(_range, new_GRT_ranges):
                new_GRT_ranges.extend(self.gengerate_new_gaps(_range, new_GRT_ranges))
            else:
                new_GRT_ranges.append(_range)
        del GRT_ranges
        # 合并，当前不相交区间集合n，获得k个不相交的区间
        new_GRT_ranges.sort(key=functools.cmp_to_key(compare_gaps_start))
        index_gaps = []
        if len(new_GRT_ranges) <= args.num_of_sub_ranges:
            self.GRT_ranges = copy.deepcopy(new_GRT_ranges)
            print("warning, ")
        else:
            for i in range(len(new_GRT_ranges)-1):
                assert new_GRT_ranges[i][1] <= new_GRT_ranges[i+1][0]
                index_gaps.append([i+1, new_GRT_ranges[i+1][0] - new_GRT_ranges[i][1]])
            index_gaps.sort(key=lambda x: (x[1]))
            index_gaps = index_gaps[0: len(new_GRT_ranges)-args.num_of_sub_ranges]
            tomerge_index = []
            for value in index_gaps:
                tomerge_index.append(value[0])
            for i in range(len(new_GRT_ranges)):
                if i not in tomerge_index:
                    self.GRT_ranges.append(new_GRT_ranges[i])
                else:
                    self.GRT_ranges[-1][1] = new_GRT_ranges[i][1]
        print("new_GRT_ranges len is {}, self.GRT_ranges len is {}".format(len(new_GRT_ranges), len(self.GRT_ranges)))
        del new_GRT_ranges

        # reassign gaps
        for row_group_index in self.column_inf:
            self.GRT_row_group[row_group_index] = set()
            for _range in self.GRT_ranges:
                start = max(bisect.bisect_left(self.column_inf[row_group_index]["record"], _range[0]) - 1, 0)
                end = min(bisect.bisect_right(self.column_inf[row_group_index]["record"], _range[1]) + 1, len(self.column_inf[row_group_index]["record"]))
                for _index in range(start, end):
                    record = self.column_inf[row_group_index]["record"][_index]
                    if _range[0] <= record <= _range[1]:
                        self.GRT_row_group[row_group_index].add((_range[0], _range[1]))
                        break
                    elif record > _range[1]:
                        break
            del self.column_inf[row_group_index]["record"]

        # for row_group_index in self.column_inf:
        #     self.GRT_row_group[row_group_index] = set()
        #     for _range in self.GRT_ranges:
        #         for record in self.column_inf[row_group_index]["record"]:
        #             if _range[0] <= record <= _range[1]:
        #                 self.GRT_row_group[row_group_index].add((_range[0], _range[1]))
        #                 break
        #             elif record > _range[1]:
        #                 break
        #     del self.column_inf[row_group_index]["record"]


    def gengerate_new_gaps(self, range, ranges):
        """
        对于范围集合 ranges，和单个范围range，将range不在ranges里面的子区间作为新的元素加入ranges
        :param range:
        :param ranges:
        :return:
        """
        # new subs
        new_subs = []
        left_range = range
        for _ in ranges:
            if _[0] <= left_range[0] <= _[1]:
                left_range = (_[1], left_range[1])
            elif _[0] > left_range[0]:
                new_subs.append((left_range[0], _[0]))
                left_range = (_[1], left_range[1])
            if left_range[0] >= left_range[1]:
                break
        if left_range[0] < left_range[1]:
            new_subs.append(left_range)
        return new_subs


    def rangeWithin(self, range, ranges):
        for _ in ranges:
            if range[0] >= _[0] and range[1] <= _[1]:
                return True
        return False

    def rangeOverLap(self, range, ranges):
        for _ in ranges:
            if range[1] <= _[0]:
                continue
            if range[0] >= _[1]:
                continue
            return True
        return False

    def point_search(self, value):
        rowgroups = set()
        for rg in self.column_inf.keys():
            if point_cover(self.column_inf[rg]["range"], value):
                included = False
                for interval in self.GRT_row_group[rg]:
                    if interval[0] <= value <= interval[1]:
                        included = True
                        break
                if included:
                    rowgroups.add(self.file + "-" + str(rg))
        return rowgroups

    def range_search(self, range):
        rowgroups = set()
        for rg in self.column_inf.keys():
            if range_overlap(self.column_inf[rg]["range"], range[0], range[1]):
                included = False
                for interval in self.GRT_row_group[rg]:
                    if interval[1] < range[0]:
                        continue
                    if interval[0] > range[1]:
                        continue
                    included = True
                    break
                if included:
                    rowgroups.add(self.file + "-" + str(rg))
        return rowgroups

if __name__ == "__main__":
    columns = ["orderkey"]
    columns_inf = {column: {} for column in columns}
    columns_inf["orderkey"]["file1"] = {}
    columns_inf["orderkey"]["file1"][0] = {}
    columns_inf["orderkey"]["file1"][0]["records"] = [0]
    print(columns_inf)
    print(columns_inf["orderkey"])
    print(columns_inf["orderkey"]["file1"].keys())
    print(columns_inf["orderkey"]["file1"][0])
    print(columns_inf["orderkey"]["file1"][0]["records"])
