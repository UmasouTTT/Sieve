import copy
import math
import time
from time import sleep

from param import *
import pyarrow.parquet as pp
import os
import sys
import numpy as np
from scipy import stats
from index.learnedIndexDemo.segment import *
from index.util import *
from bitarray.util import *
from index.learnedIndexDemo.Rowgroupdict import Rowgroupdict
import pickle
import threading
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED


class RootLakeIndex:
    def __init__(self, directory, columns):
        self.directory = directory
        self.columns = columns
        self.lakeindexs = {column: {} for column in self.columns}
        self.init()

    def init(self):
        self.directory = self.directory if self.directory.endswith("/") else self.directory + "/"
        files = os.listdir(self.directory)
        files = [self.directory + _ for _ in files]
        for column in self.columns:
            for file in files:
                args.learnedIndexDir = args.learnedIndexDir if args.learnedIndexDir.endswith("/") else args.learnedIndexDir + "/"
                dump_path = args.learnedIndexDir + file[1:].replace("/", "-") + "-" + str(args.partition_num) + "-" + column
                self.lakeindexs[column][file] = LakeIndex(column, file, dump_path)

    def generateIndexes(self, sievename="sieve"):
        for column in self.columns:
            indexsize = 0
            for file in self.lakeindexs[column]:
                indexsize += self.lakeindexs[column][file].generateIndex()
            print("{} size is {}B".format(sievename, indexsize))

    def generateIndexFromFile(self):
        for column in self.columns:
            for file in self.lakeindexs[column]:
                self.lakeindexs[column][file].generateIndexFromFile()

    def range_search(self, _range, column):
        rowgroups = set()
        for file in self.lakeindexs[column]:
            rowgroups = rowgroups.union(self.lakeindexs[column][file].rough_range_search(_range[0], _range[1]))
        return rowgroups

    def point_search(self, _value, column):
        rowgroups = set()
        for file in self.lakeindexs[column]:
            rowgroups = rowgroups.union(self.lakeindexs[column][file].rough_range_search(_value, _value))
        return rowgroups

    def insert(self, _value, rgid, column, file):
        self.lakeindexs[column][file].update(_value, rgid)

class LakeIndex:
    def __init__(self, column, file, dump_path):
        self.column = column
        self.file = file
        self.segments = list()
        self.dump_path = dump_path
        # todo compress
        self.insertdata = {}
        self.insertnumaftreg = 0

    def generateIndex(self):
        data, num_of_row_groups = self.indexParquet()
        segments, rowgroups, keyrange = self.segmentData(data, num_of_row_groups) #rowgroups是list，每个元素是该分段下所有元素存在的row group集合
        left_partition_num = args.partition_num
        ideal_size = (keyrange[1] - keyrange[0] + 1) / left_partition_num
        cumulative_block_cardinality = 0
        total_partition_num = 0
        for _ in range(len(segments)):
            if (self.column != "ss_ticket_number" or args.partition_num == 1000) and 1 / self.segments[_].slope > ideal_size:
                partition_num = (self.segments[_].segment_range[-1] - self.segments[_].segment_range[0] + 1) * self.segments[_].slope
                total_partition_num += self.segments[_].build_middle_layer(partition_num, num_of_row_groups)
                left_partition_num -= len(self.segments[_].sub_segments)
                for key in segments[_]:
                    self.segments[_].sub_segments[self.segments[_].find_subsegment_idx(key)].row_groups |= data[key]
            else:
                cumulative_block_cardinality += rowgroups[_].count(1)
        for _ in range(len(segments)):
            if not ((self.column != "ss_ticket_number" or args.partition_num == 1000) and 1 / self.segments[_].slope > ideal_size):
                partition_num = left_partition_num * (rowgroups[_].count() / cumulative_block_cardinality)
                total_partition_num += self.segments[_].build_middle_layer(partition_num, num_of_row_groups)
                for key in segments[_]:
                    self.segments[_].sub_segments[self.segments[_].find_subsegment_idx(key)].row_groups |= data[key]
        if args.partition_num >= 10000:
            for i in range(len(self.segments)):
                self.segments[i].unionrgs = True
                self.segments[i].rgs = rowgroups[i]
        if args.isinsert:
            for i in range(len(self.segments)):
                current_block_density = sum([_.get_row_group_nums() for _ in self.segments[i].sub_segments]) / (
                        self.segments[i].sub_segments[0].num_of_blocks() * len(self.segments[i].sub_segments))
                self.segments[i].insert_threshold = current_block_density
        if args.isdump:
            with open(self.dump_path, 'wb+') as f:
                pickle.dump(self.segments, f)
        return ((64 * 3 * len(segments) + num_of_row_groups * total_partition_num)/8)

    # 插入实验创建的
    def regenerateIndex(self, segindex):
        data, num_of_row_groups = self.insertParquet(segindex)
        segments, rowgroups, keyrange, tempsegs = self.insertSegmentData(data, num_of_row_groups) #rowgroups是list，每个元素是该分段下所有元素存在的row group集合
        left_partition_num = len(self.segments[segindex].sub_segments) + int(args.partition_num * self.insertnumaftreg / (50000*800))
        ideal_size = (keyrange[1] - keyrange[0] + 1) / left_partition_num
        cumulative_block_cardinality = 0
        for _ in range(len(segments)):
            if 1 / tempsegs[_].slope > ideal_size:
                partition_num = (tempsegs[_].segment_range[-1] - tempsegs[_].segment_range[0] + 1) * tempsegs[_].slope
                tempsegs[_].build_middle_layer(partition_num, num_of_row_groups)
                left_partition_num -= len(tempsegs[_].sub_segments)
                for key in segments[_]:
                    tempsegs[_].sub_segments[tempsegs[_].find_subsegment_idx(key)].row_groups |= data[key]
            else:
                cumulative_block_cardinality += len(rowgroups[_])
        for _ in range(len(segments)):
            if not 1 / tempsegs[_].slope > ideal_size:
                partition_num = left_partition_num * (len(rowgroups[_]) / cumulative_block_cardinality)
                tempsegs[_].build_middle_layer(partition_num, num_of_row_groups)
                for key in segments[_]:
                    tempsegs[_].sub_segments[tempsegs[_].find_subsegment_idx(key)].row_groups |= data[key]
        for i in range(len(tempsegs)):
            current_block_density = sum([_.get_row_group_nums() for _ in tempsegs[i].sub_segments]) / (
                    tempsegs[i].sub_segments[0].num_of_blocks() * len(tempsegs[i].sub_segments))
            tempsegs[i].insert_threshold = current_block_density
        self.segments[segindex] = tempsegs[0]
        for i in range(len(tempsegs)-1, 0, -1):
            self.segments.insert(segindex + 1, tempsegs[i])

    def generateIndexFromFile(self):
        with open(self.dump_path, 'rb+') as f:
            self.segments = pickle.load(f)

    def rough_range_search(self, start, end):
        # segment search
        start_idx = -1
        end_idx = -1
        for segment in self.segments:
            if segment.intersect(start, end):
                if start_idx == -1:
                    start_idx = self.segments.index(segment)
                    end_idx = start_idx
                else:
                    end_idx = self.segments.index(segment)
        # find row groups
        rowgrouparray = bitarray(self.segments[0].sub_segments[0].num_of_blocks())
        rowgrouparray.setall(0)
        row_groups = set()
        if -1 == start_idx:
            return row_groups
        if start_idx != end_idx:
            for _ in range(start_idx + 1, end_idx):
                if self.segments[_].unionrgs:
                    rowgrouparray |= self.segments[_].rgs
                else:
                    for sub_segment in self.segments[_].sub_segments:
                        rowgrouparray |= sub_segment.row_groups

        rowgrouparray |= self.edge_range_search(start_idx, end_idx, start, end
                                                             , self.segments[start_idx].rough_search
                                                             , self.segments[end_idx].rough_search)
        result_row_groups = set()
        for i in range(len(rowgrouparray)):
            if rowgrouparray[i] == 1:
                result_row_groups.add(self.file + "-" + str(i))
        return result_row_groups
        # row_groups_filter_by_min_max = []
        # for row_group in result_row_groups:
        #     if range_overlap(self.minmaxinfo[int(row_group.split("-")[1])]["min-max"], start, end):
        #         row_groups_filter_by_min_max.append(row_group)
        # return row_groups_filter_by_min_max


    def edge_range_search(self, start_idx, end_idx, start, end, start_search_func, end_search_func):
        rowgrouparray = bitarray(self.segments[0].sub_segments[0].num_of_blocks())
        rowgrouparray.setall(0)
        if start_idx == end_idx:
            if self.segments[start_idx].is_contained_by(start, end):
                rowgrouparray |= self.segments[start_idx].get_rowgroups()
            else:
                rowgrouparray |= start_search_func(max(start, self.segments[start_idx].segment_range[0])
                                                          , min(end, self.segments[end_idx].segment_range[1]))
        else:
            # start
            if self.segments[start_idx].is_contained_by(start, self.segments[start_idx].segment_range[-1]):
                rowgrouparray |= self.segments[start_idx].get_rowgroups()
            else:
                rowgrouparray |= start_search_func(max(start, self.segments[start_idx].segment_range[0])
                                                          , self.segments[start_idx].segment_range[-1])
            # end
            if self.segments[end_idx].is_contained_by(self.segments[end_idx].segment_range[0], end):
                rowgrouparray |= self.segments[end_idx].get_rowgroups()
            else:
                rowgrouparray |= end_search_func(self.segments[end_idx].segment_range[0]
                                                        , min(end, self.segments[end_idx].segment_range[1]))

        return rowgrouparray

    #todo other file type
    def indexParquet(self):
        """
        遍历self.file对应的parquet文件
        :return:
        datas：字典，key是self.column属性的值，value是值出现过的rowgroup集合
        _min：self.file中self.column属性的最小值
        _max：self.file中self.column属性的最大值
        num_of_row_groups：self.file中行组的个数
        """
        # get files
        # read data {column:{value:rowGroup}} todo: only numerical type
        datas = {}
        table = pp.ParquetFile(self.file)
        num_of_row_groups = table.num_row_groups
        for row_group_index in range(num_of_row_groups):
            row_group_contents = table.read_row_group(row_group_index, columns=[self.column])
            for record in row_group_contents.column(self.column):
                if str(record) == 'None':
                    continue
                record = getRecord(record)
                if record not in datas:
                    datas[record] = bitarray(num_of_row_groups)
                    datas[record].setall(0)
                datas[record][row_group_index] = 1
        return datas, num_of_row_groups

    def insertParquet(self, segindex):
        """
        遍历第segindex个segement对应的parquet块
        :return:
        datas：字典，key是self.column属性的值，value是值出现过的rowgroup集合
        _min：self.file中self.column属性的最小值
        _max：self.file中self.column属性的最大值
        num_of_row_groups：self.file中行组的个数
        """
        rgs = set()
        for subseg in self.segments[segindex].sub_segments:
            rgs = rgs.union(subseg.get_row_group_set())
        # get files
        # read data {column:{value:rowGroup}} todo: only numerical type
        datas = {}
        table = pp.ParquetFile(self.file)
        num_of_row_groups = table.num_row_groups
        real_num_of_row_groups = self.segments[segindex].sub_segments[0].num_of_blocks()
        for row_group_index in rgs:
            if row_group_index >= num_of_row_groups:
                continue
            row_group_contents = table.read_row_group(row_group_index, columns=[self.column])
            for record in row_group_contents.column(self.column):
                if str(record) == 'None':
                    continue
                record = getRecord(record)
                if record < self.segments[segindex].segment_range[0] or record > self.segments[segindex].segment_range[1]:
                    continue
                if record not in datas:
                    datas[record] = bitarray(real_num_of_row_groups)
                    datas[record].setall(0)
                datas[record][row_group_index] = 1
        for row_group_index in rgs:
            tempdata = self.insertdata.get(row_group_index, set())
            for record in tempdata:
                if record < self.segments[segindex].segment_range[0] or record > self.segments[segindex].segment_range[1]:
                    continue
                if record not in datas:
                    datas[record] = bitarray(real_num_of_row_groups)
                    datas[record].setall(0)
                datas[record][row_group_index] = 1
        return datas, real_num_of_row_groups

    #data：一个set，key是value，value是定位到file + rowGroup的信息
    def segmentData(self, data, num_of_row_groups):
        """
        :param data: 字典，key是属性存在的值，value是值出现的file+rowgroup集合
        :return:
        """
        # make segments
        segments = []
        rowgroups = []
        sl_high = sys.maxsize
        sl_low = 0
        segment = []
        rowgroup = bitarray(num_of_row_groups)
        rowgroup.setall(0)
        last_rgs = bitarray(num_of_row_groups)
        last_rgs.setall(0)
        y = 0
        prekey = None
        sort_data_keys = sorted(data.keys())
        total_gap = 0
        learned_gap = 0
        gap_list = []
        for key in sort_data_keys:
            belonged_rg = data[key]
            if 0 == len(segment):
                segment.append(key)
                rowgroup |= belonged_rg
                last_rgs = belonged_rg
                y = 0
            else:
                _y = y
                if prekey != None and key-prekey > 1:
                    total_gap += (key-prekey-1)
                    gap_list.append(key-prekey-1)
                    # _y += 2
                    if count_or(last_rgs, belonged_rg) > args.largegapth:
                        _y += 2
                    else:
                        _y += 1
                else:
                    if not subset(belonged_rg, last_rgs):
                        _y += 1
                _sl = _y / (key - segment[0])
                if _sl > sl_high or _sl < sl_low or (key - segment[-1]) > int(args.sieve_gap_percent * (sort_data_keys[-1] - sort_data_keys[0])) or (args.segment_error == 1 and _y != y):#最后一个条件新加的应对gap
                    # new segment
                    learned_gap += (key - segment[-1] - 1)
                    segments.append(segment)
                    rowgroups.append(rowgroup)
                    if len(segment) == 1:
                        tempsl = 1
                    else:
                        if y != 0:
                            tempsl = y / (segment[-1] - segment[0])
                        else:
                            tempsl = 1 / (segment[-1] - segment[0] + 1)
                    self.segments.append(Segment(tempsl, (segment[0], segment[-1])))
                    sl_high = sys.maxsize
                    sl_low = 0
                    segment = [key]
                    rowgroup = copy.copy(belonged_rg)
                    last_rgs = belonged_rg
                    y = 0
                else:
                    # update
                    _sl_high = (_y + args.segment_error) / (key - segment[0])
                    _sl_low = (_y - args.segment_error) / (key - segment[0])
                    sl_high = min(sl_high, _sl_high)
                    sl_low = max(sl_low, _sl_low)
                    segment.append(key)
                    rowgroup |= belonged_rg
                    last_rgs = belonged_rg
                    y = _y
            prekey = key
        # not global gap
        if len(segment) == 1:
            tempsl = 1
        else:
            if y != 0:
                tempsl = y / (segment[-1] - segment[0])
            else:
                tempsl = 1 / (segment[-1] - segment[0] + 1)
        rowgroups.append(rowgroup)
        segments.append(segment)
        self.segments.append(Segment(tempsl, (segment[0], segment[-1])))
        return segments, rowgroups, [sort_data_keys[0], sort_data_keys[-1]]


    def insertSegmentData(self, data, num_of_row_groups):
        """
        :param data: 字典，key是属性存在的值，value是值出现的file+rowgroup集合
        :return:
        """
        # make segments
        tempsegs = []
        segments = []
        rowgroups = []
        sl_high = sys.maxsize
        sl_low = 0
        segment = []
        rowgroup = bitarray(num_of_row_groups)
        rowgroup.setall(0)
        last_rgs = bitarray(num_of_row_groups)
        last_rgs.setall(0)
        y = 0
        prekey = None
        sort_data_keys = sorted(data.keys())
        total_gap = 0
        learned_gap = 0
        gap_list = []
        for key in sort_data_keys:
            belonged_rg = data[key]
            if 0 == len(segment):
                segment.append(key)
                rowgroup |= belonged_rg
                last_rgs = belonged_rg
                y = 0
            else:
                _y = y
                if prekey != None and key-prekey > 1:
                    total_gap += (key-prekey-1)
                    gap_list.append(key-prekey-1)
                    # _y += 2
                    if count_or(last_rgs, belonged_rg) > args.largegapth:
                        _y += 2
                    else:
                        _y += 1
                else:
                    if not subset(belonged_rg, last_rgs):
                        _y += 1
                _sl = _y / (key - segment[0])
                if _sl > sl_high or _sl < sl_low or (key - segment[-1]) > int(args.sieve_gap_percent * (sort_data_keys[-1] - sort_data_keys[0])) or (args.segment_error == 1 and _y != y):#最后一个条件新加的应对gap
                    # new segment
                    learned_gap += (key - segment[-1] - 1)
                    segments.append(segment)
                    rowgroups.append(rowgroup)
                    if len(segment) == 1:
                        tempsl = 1
                    else:
                        if y != 0:
                            tempsl = y / (segment[-1] - segment[0])
                        else:
                            tempsl = 1 / (segment[-1] - segment[0] + 1)
                    tempsegs.append(Segment(tempsl, (segment[0], segment[-1])))
                    sl_high = sys.maxsize
                    sl_low = 0
                    segment = [key]
                    rowgroup = copy.copy(belonged_rg)
                    last_rgs = belonged_rg
                    y = 0
                else:
                    # update
                    _sl_high = (_y + args.segment_error) / (key - segment[0])
                    _sl_low = (_y - args.segment_error) / (key - segment[0])
                    sl_high = min(sl_high, _sl_high)
                    sl_low = max(sl_low, _sl_low)
                    segment.append(key)
                    rowgroup |= belonged_rg
                    last_rgs = belonged_rg
                    y = _y
            prekey = key
        # not global gap
        if len(segment) == 1:
            tempsl = 1
        else:
            if y != 0:
                tempsl = y / (segment[-1] - segment[0])
            else:
                tempsl = 1 / (segment[-1] - segment[0] + 1)
        rowgroups.append(rowgroup)
        segments.append(segment)
        tempsegs.append(Segment(tempsl, (segment[0], segment[-1])))
        return segments, rowgroups, [sort_data_keys[0], sort_data_keys[-1]], tempsegs

    def indexOnTheLake(self, data, segments, column):
        # make column directory
        if not os.path.exists(self.index_directory + column + "/"):
            os.mkdir(self.index_directory + column + "/")
        # write root todo: line->offset
        f = open(self.index_directory + column + "/" + "root", "w+", encoding="utf-8")
        # todo: make b+ tree
        for segment in segments:
            f.write("{},{}\n".format(segment[0], segment[-1]))
        f.close()
        # write file
        for segment in segments:
            f = open(self.index_directory + column + "/" + "{}-{}".format(segment[0], segment[-1]), "w+", encoding="utf-8")
            # record learned index y = ax + b
            a = (len(segment) - 1) / (segment[-1] - segment[0])
            b = 1 # first line for learned index parameter
            f.write("{},{}\n".format(a, b))
            # record data
            for record in segment:
                paths = ""
                for _ in data[record]:
                    paths += _ + "|"
                    # if _ != data[record][-1]:
                    #     paths += _ + "|"
                    # else:
                    #     paths += _
                f.write("{},{}\n".format(record, paths))
            f.close()

    def update(self, _value, rgid):
        # new block
        self.insertnumaftreg += 1
        if rgid not in self.insertdata:
            self.insertdata[rgid] = set()
        self.insertdata[rgid].add(_value)
        curfile_rgnum = self.segments[0].sub_segments[0].num_of_blocks()
        if rgid >= curfile_rgnum:
            for i in range(len(self.segments)):
                for j in range(len(self.segments[i].sub_segments)):
                    self.segments[i].sub_segments[j].row_groups.append(0)

        insertflag = False
        for segindex in range(len(self.segments)):
            seg = self.segments[segindex]
            if _value >= seg.segment_range[0] and _value <= seg.segment_range[1]:
                insertflag = True
                subsegment = seg.sub_segments[seg.find_subsegment_idx(_value)]
                subsegment.add_row_group(rgid)
                # block density
                current_block_density = sum([_.get_row_group_nums() for _ in seg.sub_segments]) / (
                        curfile_rgnum * len(seg.sub_segments))
                if seg.insert_threshold < 0.05:
                    if current_block_density > seg.insert_threshold * 3:
                        self.regenerateIndex(segindex)
                        self.insertnumaftreg = 0
                else:
                    if current_block_density > seg.insert_threshold + 0.15:
                        self.regenerateIndex(segindex)
                        self.insertnumaftreg = 0
                break

class Reader:
    def __init__(self, index_directory):
        self.index_directory = index_directory if self.index_directory.endswith("/") else index_directory + "/"
        self.root_index = {}
        self.start()

    def start(self):
        columns = os.listdir(self.index_directory)
        column_files = [self.index_directory + _ for _ in columns]
        for column_file in column_files:
            column = column_file.split("/")[-1]
            self.root_index[column] = []
            f = open(column_file, "r+", encoding="utf-8")
            for line in f:
                # todo B+
                self.root_index[column].append(int(line.strip().split(",")[0]), int(line.strip().split(",")[1]))
            f.close()

    def indexFiles(self, column, value):
        # find file name
        segment = None
        for _ in self.root_index[column]:
            if value > _[0] and value < _[1]:
                segment = _
                break
        if segment is None:
            print("No such value")
            return
        file_name = self.index_directory + column + "/" + "{}-{}".format(segment[0], segment[-1])

        # predict line:
        # read learned index
        parameters = os.popen('sed -n {}p {}'.format(1, file_name)).read()
        parameters = parameters.strip().split(",")
        a = parameters[0], b = parameters[1]
        # predict position
        predicted_line = a * value + b
        # find real position
        predicted_line_content = os.popen('sed -n {}p {}'.format(predicted_line, file_name)).read()
        _value = predicted_line_content.strip().split(",")[0]
        if _value == value:
            return predicted_line_content.strip().split(",")[1]
        elif _value > value:
            while True:
                predicted_line -= 1
                predicted_line_content = os.popen('sed -n {}p {}'.format(predicted_line, file_name)).read()
                _value = predicted_line_content.strip().split(",")[0]
                if _value == value:
                    return predicted_line_content.strip().split(",")[1]
                elif _value < value:
                    return
        else:
            while True:
                predicted_line += 1
                predicted_line_content = os.popen('sed -n {}p {}'.format(predicted_line, file_name)).read()
                _value = predicted_line_content.strip().split(",")[0]
                if _value == value:
                    return predicted_line_content.strip().split(",")[1]
                elif _value > value:
                    return

if __name__ == "__main__":
    lakeIndex = LakeIndex(["orderkey"], "../../dataManager/tpch/data_50000/", "indexData", "serializedIndex")
    #lakeIndex.generateIndex()
    lakeIndex.generateIndexFromFile()
    result = lakeIndex.rough_range_search(327798978, 328416005)
    print(len(result))
    print(result)