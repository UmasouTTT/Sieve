import sys

from param import *
from index.util import *
import math
import pyarrow.parquet as pp
from bitarray import bitarray
import pickle

class Segment:
    def __init__(self, slope, segment_range):
        # self.data = data
        self.slope = slope
        self.segment_range = segment_range
        # self.row_groups = row_groups
        # self.data_rg = row_group_by_data
        self.sub_segment_size = 0
        self.sub_segments = list()
        self.insert_threshold = 0
        self.unionrgs = False
        self.rgs = None
        # print("Start building middle layer ...")
        #self.build_middle_layer()

    def get_rowgroups(self):#删除了segement类里面荣誉的row_groups,从subsegement中获取
        if self.unionrgs:
            return self.rgs
        rgbitarray = bitarray(len(self.sub_segments[0].row_groups))
        rgbitarray.setall(0)
        for sub_segment in self.sub_segments:
            rgbitarray |= sub_segment.row_groups
        return rgbitarray

    def intersect(self, min, max):
        if min > self.segment_range[1]:
            return False
        if max < self.segment_range[0]:
            return False
        return True

    def is_contained_by(self, min, max):
        if min <= self.segment_range[0] and max >= self.segment_range[1]:
            return True
        return False

    def sub_segment_num(self, partition_num):
        if self.is_large_gap_segment():
            self.sub_segment_size = max(int(1/self.slope), 1)
            partition_num = math.ceil((self.segment_range[1] - self.segment_range[0] + 1) / self.sub_segment_size)
            print("segment is large_gap_segement and partition_num is {}".format(partition_num))
        else:
            self.sub_segment_size = max(int((self.segment_range[1] - self.segment_range[0] + 1) / partition_num), 1)
            partition_num = math.ceil((self.segment_range[1] - self.segment_range[0] + 1) / self.sub_segment_size)
        assert partition_num > 0
        return partition_num

    def is_large_gap_segment(self):
        return 1 / self.slope > args.attribute_percent * (self.segment_range[-1] - self.segment_range[0])

    def get_block_density(self, block_cardinality, key_cardinality):
        # block_cardinality为该segment中key的数量，key_cardinality为segment中key的范围
        self.block_density = block_cardinality / key_cardinality

    def build_middle_layer(self, partition_num, num_of_row_groups):
        # todo   only selectivity -> tradeoff scan cost && filter ability
        self.sub_segment_size = max(int((self.segment_range[1] - self.segment_range[0] + 1) / partition_num), 1)
        sub_segment_num = math.ceil((self.segment_range[1] - self.segment_range[0] + 1) / self.sub_segment_size)
        # build sub segments
        for _ in range(sub_segment_num):
            self.sub_segments.append(SubSegment(num_of_row_groups))
        return sub_segment_num

        # assign row group
        # self.assign_row_group(rglist)

    # def assign_row_group(self, rglist):
    #     # read
    #     for row_group_key in self.row_groups:
    #         row_group = rglist[row_group_key]
    #         file_path = row_group.split("-")[0]
    #         row_group_index = int(row_group.split("-")[1])
    #         table = pp.ParquetFile(file_path)
    #         row_group_contents = table.read_row_group(row_group_index, columns=[self.column])
    #         for record in row_group_contents.column(self.column):
    #             if str(record) == 'None':
    #                 continue
    #             record = getRecord(record)
    #             if self.segment_range[0] <= record <= self.segment_range[1]:
    #                 # todo: speed up 用计算代替遍历，segment多的时候遍历时间长
    #                 sub_segment = self.sub_segments[self.find_subsegment_idx(record)]
    #                 sub_segment.add_row_group(row_group_key)

    def range_search(self, start, end, data, data_rg):
        start_index = max(0, int(self.slope * (start - self.segment_range[0])) - args.segment_error)
        end_index = min(len(data) - 1, math.ceil(self.slope * (end - self.segment_range[0])) + args.segment_error)
        read_length = end_index - start_index

        real_start_index = -1
        real_end_index = -1


        # real read size
        real_read_record_size = 0
        for _ in range(start_index, end_index + 1):
            real_read_record_size += len(data_rg[data[_]])
        for _ in range(start_index, end_index + 1):
            _temp = data[_]
            if data[_] >= start:
                real_start_index = _
                break
        assert real_start_index != -1
        for _ in range(end_index, start_index-1, -1):
            _temp = data[_]
            if data[_] <= end:
                real_end_index = _
                break
        assert real_end_index != -1
        # target row groups
        result_row_groups = set()
        for _ in range(real_start_index, real_end_index + 1):
            result_row_groups = result_row_groups.union(data_rg[data[_]])


        return result_row_groups

    def rough_search(self, start, end):
        rgbitarray = bitarray(len(self.sub_segments[0].row_groups))
        rgbitarray.setall(0)
        start_subsegment_index = self.find_subsegment_idx(start)
        end_subsegment_index = self.find_subsegment_idx(end)
        for _ in range(start_subsegment_index, end_subsegment_index + 1):
            rgbitarray |= self.sub_segments[_].row_groups
        return rgbitarray

    def find_subsegment_idx(self, value):
        return int((value - self.segment_range[0]) / self.sub_segment_size)

class SubSegment:
    def __init__(self, row_group_num):
        # 左闭 右开
        self.row_groups = bitarray(row_group_num)
        self.row_groups.setall(0)

    def num_of_blocks(self):
        return len(self.row_groups)

    def add_row_group(self, row_group_index):
        self.row_groups[row_group_index] = 1

    def union_row_group(self, row_group_indexs):
        for value in row_group_indexs:
            self.row_groups[value] = 1

    def get_row_group_nums(self):
        return self.row_groups.count()

    # def transfer(self):
    #     if self.row_groups.count() < 50:
    #         for i in range(len(self.row_groups)):
    #             if self.row_groups[i] == 1:
    #                 self.row_groups_set.add(i)
    #         self.row_groups.clear()
    #         self.row_groups = None
    #     else:
    #         self.row_groups_set = None

    def get_row_group_set(self):
        # if self.row_groups_set != None:
        #     return self.row_groups_set
        res_set = set()
        for i in range(len(self.row_groups)):
            if self.row_groups[i] == 1:
                res_set.add(i)
        return res_set
