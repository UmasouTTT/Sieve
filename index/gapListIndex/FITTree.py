# 用key数组+对应行组数组的形式组织的倒排索引，没有fiting中间的树形结构
import bisect
import copy
import pickle
import time

import pyarrow.parquet as pp
import os
from index.index import *
from index.util import getRecord
from param import args
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, as_completed


class FITTREE(Index):
    def __init__(self, directory, columns):
        super(FITTREE, self).__init__(directory, columns)
        self.directory = directory
        self.columns = columns
        self.indexs = {} #里面每个文件指到两个数组，第一个数组存排序后的值，第二个数组存值对应的rgset
        self.fitingsize = 0
        self.insertnone = 0

    def generateOneFile(self, file, column):
        tempdict = {}
        table = pp.ParquetFile(file)
        num_of_row_groups = table.num_row_groups
        for row_group_index in range(num_of_row_groups):
            row_group_contents = table.read_row_group(row_group_index, columns=[column])
            for record in row_group_contents.column(column):
                if str(record) == 'None':
                    continue
                record = getRecord(record)
                if record not in tempdict:
                    tempdict[record] = set()
                tempdict[record].add(row_group_index)
        self.fitingsize += (num_of_row_groups * len(tempdict) / 8)
        tempsrotedrecords = sorted(tempdict)
        dumpindex = [[] for i in range(2)]
        for record in tempsrotedrecords:
            dumpindex[0].append(record)
            dumpindex[1].append(copy.deepcopy(tempdict[record]))
        dump_path = args.fittreeDir + file[1:].replace("/", "-") + column
        with open(dump_path, 'wb+') as f:
            pickle.dump(dumpindex, f)
        del tempdict

    def generateIndexes(self):
        files = os.listdir(self.directory)
        files = [self.directory + _ for _ in files]
        for column in self.columns:
            for file in files:
                self.generateOneFile(file, column)
            print("fiting size is {}B".format(self.directory, column, self.fitingsize))

    def generateIndexFromFile(self, column):
        files = os.listdir(self.directory)
        files = [self.directory + _ for _ in files]
        for file in files:
            dump_path = args.fittreeDir + file[1:].replace("/", "-") + column
            with open(dump_path, 'rb+') as f:
                self.indexs[file] = pickle.load(f)
            f.close()

    def point_search(self, value, column):
        result_rgs = set()
        for file_key in self.indexs:
            findindex = bisect.bisect_left(self.indexs[file_key][0], value)
            if findindex >= len(self.indexs[file_key][0]):
                continue
            if self.indexs[file_key][0][findindex] != value:
                continue
            for tempv in self.indexs[file_key][1][findindex]:
                result_rgs.add(file_key + "-" + str(tempv))
        return result_rgs

    def range_search(self, data_range, column):
        result_rgs = set()
        for file_key in self.indexs:
            findindex = bisect.bisect_left(self.indexs[file_key][0], data_range[0])
            if findindex >= len(self.indexs[file_key][0]):
                continue
            for tempindex in range(findindex, len(self.indexs[file_key][0])):
                if self.indexs[file_key][0][tempindex] > data_range[1]:
                    break
                for tempv in self.indexs[file_key][1][tempindex]:
                    result_rgs.add(file_key + "-" + str(tempv))
        return result_rgs

    def update(self, filepath, data, rg_id):
        curtime = time.time()
        findindex = bisect.bisect_left(self.indexs[filepath][0], data)
        if self.indexs[filepath][0][findindex] == data:
            self.indexs[filepath][1][findindex].add(rg_id)
        else:
            self.insertnone += 1

