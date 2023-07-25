# GREindex 所有文件作为一个整体去创建GRT索引
import copy
import pickle
import sys

import pyarrow.parquet as pp
import os
from index.index import *
from param import *
from index.util import *
import functools
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, as_completed

class RootGREindex(Index):
    def __init__(self, directory, columns):
        super(RootGREindex, self).__init__(directory, columns)
        self.directory = directory
        self.greIndexs = {}
        self.columns = columns

    def generateIndexes(self):
        files = os.listdir(self.directory)
        files = [self.directory + _ for _ in files]
        for column in self.columns:
            self.greIndexs[column] = GREindex(files, column)
            dump_path = args.greIndexDir + self.directory[1:].replace("/", "-") + column
            with open(dump_path, 'wb+') as f:
                pickle.dump(self.greIndexs[column], f)

    def generateIndexFromFile(self):
        for column in self.columns:
            dump_path = args.greIndexDir + self.directory[1:].replace("/", "-") + column
            with open(dump_path, 'rb+') as f:
                self.greIndexs[column] = pickle.load(f)

    def point_search(self, value, column):
        searched_blocks = self.greIndexs[column].point_search(value)
        return searched_blocks

    def range_search(self, data_range, column):
        searched_blocks = self.greIndexs[column].range_search(data_range)
        return searched_blocks

class GREindex:
    def __init__(self, files, column):
        self.files = files
        self.column = column
        # {column:{rowgroup:[gap1, gap2...]}}
        self.init_gap_list()

    def init_gap_list(self):
        # todo get range by query process
        if args.file_type == "Parquet":
            self.extractAllGRT()
        else:
            print("Wrong file type : {}".format(parser.file_type))

    def extractOneGRTfun(self, file):
        GRT_ranges = []
        table = pp.ParquetFile(file)
        num_of_row_groups = table.num_row_groups
        for row_group_index in range(num_of_row_groups):
            row_group_contents = table.read_row_group(row_group_index, columns=[self.column])
            row_group_records = list()
            for record in row_group_contents.column(self.column):
                if str(record) == 'None':
                    continue
                record = getRecord(record)
                row_group_records.append(record)
            row_group_records.sort()
            sub_partitions = []
            num_of_gap_lists = args.num_of_gre_gap  # 为了加快生成
            gaps = []
            for _ in range(len(row_group_records) - 1):
                # todo: for simple, present is only int
                if row_group_records[_ + 1] - row_group_records[_] > args.int_interval:
                    gaps.append((row_group_records[_], row_group_records[_ + 1]))
            gaps.sort(key=functools.cmp_to_key(compare_gaps))
            gaps = gaps[0:num_of_gap_lists]
            start = row_group_records[0]
            gaps.sort(key=functools.cmp_to_key(compare_gaps_start))
            for gap in gaps:
                sub_partitions.append((start, gap[0]))
                start = gap[-1]
            sub_partitions.append((start, row_group_records[-1]))
            GRT_ranges.extend(sub_partitions)
            del row_group_records
        new_GRT_ranges = []
        GRT_ranges.sort(key=functools.cmp_to_key(compare_gaps_start))
        for _range in GRT_ranges:
            if self.rangeWithin(_range, new_GRT_ranges):
                continue
            elif self.rangeOverLap(_range, new_GRT_ranges):
                new_GRT_ranges.extend(self.gengerate_new_gaps(_range, new_GRT_ranges))
            else:
                new_GRT_ranges.append(_range)
        return new_GRT_ranges

    def assginOneFile(self, file):
        self.GRT_row_group[file] = {}
        table = pp.ParquetFile(file)
        num_of_row_groups = table.num_row_groups

        for row_group_index in range(num_of_row_groups):
            self.GRT_row_group[file][row_group_index] = set()
            row_group_contents = table.read_row_group(row_group_index, columns=[self.column])
            for record in row_group_contents.column(self.column):
                # evaluate on int first todo: add other type
                if str(record) == 'None':
                    continue
                record = getRecord(record)
                record_in = False
                for _range in self.GRT_ranges:
                    if _range[0] <= record <= _range[1]:
                        record_in = True
                        self.GRT_row_group[file][row_group_index].add((_range[0], _range[1]))
                        break
                if not record_in:
                    print("{} miss from rg {}".format(record, row_group_index))

    def extractAllGRT(self):
        print("start to cons grt")
        all_GRT_ranges = []
        # all_task = []
        # with ThreadPoolExecutor(20, thread_name_prefix='GRT_gen_thread_pool') as pools:
        #     for file in self.files:
        #         cur_task = pools.submit(self.extractOneGRTfun, file)
        #         all_task.append(cur_task)
        #     wait(all_task, return_when=ALL_COMPLETED)
        #     for future in as_completed(all_task):
        #         all_GRT_ranges.extend(future.result())

        for file in self.files:
            all_GRT_ranges.extend(self.extractOneGRTfun(file))

        all_GRT_ranges.sort(key=functools.cmp_to_key(compare_gaps_start))
        self.GRT_ranges = []
        self.GRT_row_group = {}
        for _range in all_GRT_ranges:
            if self.rangeWithin(_range, self.GRT_ranges):
                continue
            elif self.rangeOverLap(_range, self.GRT_ranges):
                self.GRT_ranges.extend(self.gengerate_new_gaps(_range, self.GRT_ranges))
            else:
                self.GRT_ranges.append(_range)
        print("gre table origin len is:{}".format(len(self.GRT_ranges)))
        # merge
        while len(self.GRT_ranges) > args.num_of_sub_ranges:
            # find small gap
            smallest_gap = sys.maxsize
            to_be_combine = -1
            for range_num in range(len(self.GRT_ranges) - 1):
                if (self.GRT_ranges[range_num + 1][0] - self.GRT_ranges[range_num][1]) < smallest_gap:
                    smallest_gap = self.GRT_ranges[range_num + 1][0] - self.GRT_ranges[range_num][1]
                    to_be_combine = range_num
            if to_be_combine == -1:
                break
            else:
                new_gap = (self.GRT_ranges[to_be_combine][0], self.GRT_ranges[to_be_combine + 1][1])
                to_be_remove1 = self.GRT_ranges[to_be_combine]
                to_be_remove2 = self.GRT_ranges[to_be_combine + 1]
                self.GRT_ranges.remove(to_be_remove1)
                self.GRT_ranges.remove(to_be_remove2)
                self.GRT_ranges.append(new_gap)
                self.GRT_ranges.sort(key=functools.cmp_to_key(compare_gaps_start))
        print("gre table real len is:{}".format(len(self.GRT_ranges)))
        # reassign gaps
        print("gre start to assign file")
        # all_task = []
        # with ThreadPoolExecutor(20, thread_name_prefix='GRT_gen_thread_pool1') as pools:
        #     for file in self.files:
        #         cur_task = pools.submit(self.assginOneFile, file)
        #         all_task.append(cur_task)
        #     wait(all_task, return_when=ALL_COMPLETED)
        for file in self.files:
            self.assginOneFile(file)

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
        for file in self.files:
            for rg in self.GRT_row_group[file].keys():
                included = False
                for interval in self.GRT_row_group[file][rg]:
                    if interval[0] <= value <= interval[1]:
                        included = True
                        break
                if included:
                    rowgroups.add(file + "-" + str(rg))
        return rowgroups

    def range_search(self, range):
        rowgroups = set()
        for file in self.files:
            for rg in self.GRT_row_group[file].keys():
                included = False
                for interval in self.GRT_row_group[file][rg]:
                    if interval[1] < range[0]:
                        continue
                    if interval[0] > range[1]:
                        continue
                    included = True
                    break
                if included:
                    rowgroups.add(file + "-" + str(rg))
        return rowgroups
