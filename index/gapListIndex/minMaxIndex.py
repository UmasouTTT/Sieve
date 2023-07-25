import pickle
import sys

import pyarrow.parquet as pp
import os
from index.index import *
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, as_completed

from index.util import getRecord, point_cover, range_overlap
from param import parser, args


class MinMaxIndex(Index):
    def __init__(self, directory, columns):
        super(MinMaxIndex, self).__init__(directory, columns)
        self.directory = directory
        self.minmaxIndexs = {column: {} for column in columns}
        self.columns = columns
        self.indexsize = 0

    def genPerFile(self, file, column):
        dump_path = args.minmaxIndexDir + file[1:].replace("/", "-") + column
        curminmax = MinMax(file, column, self)
        self.minmaxIndexs[column][file] = curminmax
        self.indexsize += curminmax.indexsize
        if args.isdump:
            with open(dump_path, 'wb+') as f:
                pickle.dump(curminmax, f)
                f.close()

    def generateIndexes(self):
        files = os.listdir(self.directory)
        files = [self.directory + _ for _ in files]
        for column in self.columns:
            for file in files:
                self.genPerFile(file, column)
        print("zonemap size is {}B".format(self.indexsize))


    def generateIndexFromFile(self):
        files = os.listdir(self.directory)
        files = [self.directory + _ for _ in files]
        for column in self.columns:
            for file in files:
                dump_path = args.minmaxIndexDir + file[1:].replace("/", "-") + column
                with open(dump_path, 'rb+') as f:
                    try:
                        curminmax = pickle.load(f)
                        self.minmaxIndexs[column][file] = curminmax
                        f.close()
                    except EOFError:
                        print(dump_path)
                        self.genPerFile(file, column)
                        f.close()

    def point_search(self, value, column):
        searched_blocks = list()
        for file in self.minmaxIndexs[column]:
            searched_blocks.extend(self.minmaxIndexs[column][file].point_search(value))
        return searched_blocks

    def range_search(self, data_range, column):
        searched_blocks = list()
        for file in self.minmaxIndexs[column]:
            searched_blocks.extend(self.minmaxIndexs[column][file].range_search(data_range))
        return searched_blocks

    def insert(self, data, rg_id, column, file):
        self.minmaxIndexs[column][file].update(data, rg_id)

class MinMax:
    def __init__(self, file, column, sieve):
        self.file = file
        self.column = column
        self.sieve = sieve
        self.columns_inf = {}
        self.indexsize = 0
        self.init_gap_list()

    def init_gap_list(self):
        self.parquet_init_gap_list()

    def parquet_init_gap_list(self):
        column = self.column
        table = pp.ParquetFile(self.file)
        num_of_row_groups = table.num_row_groups
        self.indexsize = num_of_row_groups * 128 / 8
        for row_group_index in range(num_of_row_groups):
            row_group_contents = table.read_row_group(row_group_index, columns=[column])
            _min = sys.maxsize
            _max = -sys.maxsize
            for record in row_group_contents.column(column):
                if str(record) == 'None':
                    continue
                record = getRecord(record)
                _min = min(record, _min)
                _max = max(record, _max)
            # sort
            self.columns_inf[row_group_index] = {}
            self.columns_inf[row_group_index]["range"] = [_min, _max]

    def point_search(self, value):
        rowgroups = set()
        for rg in self.columns_inf.keys():
            if point_cover(self.columns_inf[rg]["range"], value):
                rowgroups.add(self.file + "-" + str(rg))
        return rowgroups



    def range_search(self, range):
        rowgroups = set()
        for rg in self.columns_inf.keys():
            if range_overlap(self.columns_inf[rg]["range"], range[0], range[1]):
                rowgroups.add(self.file + "-" + str(rg))
        return rowgroups

    def update(self, data, rg_id):
        if rg_id in self.columns_inf.keys():
            self.columns_inf[rg_id]["range"][0] = min(self.columns_inf[rg_id]["range"][0], data)
            self.columns_inf[rg_id]["range"][1] = max(self.columns_inf[rg_id]["range"][1], data)
        else:
            self.columns_inf[rg_id] = {}
            self.columns_inf[rg_id]["range"] = [data, data]



