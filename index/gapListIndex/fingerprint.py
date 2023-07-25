#fingerprint，使用整体的最小值最大值。
import copy
import math
import pickle
import sys

from index.index import *
from param import *
from index.util import *
from bitarray import bitarray
import time

class RootAllFileFingerPrints(Index):
    def __init__(self, directory, columns):
        self.directory = directory
        self.fingerprints = {}
        self.columns = columns

    def generateIndexes(self):
        files = os.listdir(self.directory)
        files = [self.directory + _ for _ in files]
        for column in self.columns:
            self.fingerprints[column] = AllFileFingerPrints(files, column)
            print("fingerprint size is {}B".format(self.fingerprints[column].indexsize))
            if args.isdump:
                dump_path = args.fingerprintIndexDir + self.directory[1:].replace("/", "-") + column
                with open(dump_path, 'wb+') as f:
                    pickle.dump(self.fingerprints[column], f)

    def generateIndexFromFile(self):
        for column in self.columns:
            dump_path = args.fingerprintIndexDir + self.directory[1:].replace("/", "-") + column
            with open(dump_path, 'rb+') as f:
                self.fingerprints[column] = pickle.load(f)

    def point_search(self, value, column):
        searched_blocks = self.fingerprints[column].point_search(value)
        return searched_blocks

    def range_search(self, data_range, column):
        searched_blocks = self.fingerprints[column].range_search(data_range)
        return searched_blocks

    def insert(self, _value, rgid, column, file):
        self.fingerprints[column].update(_value, rgid, file)

class AllFileFingerPrints:
    def __init__(self, files, column):
        self.files = files
        self.column = column
        # {column:{rowgroup:[gap1, gap2...]}}
        self.column_inf = {}
        self.indexsize = 0
        self.init_fingerprints()

    def init_fingerprints(self):
        # todo get range by query process
        if args.file_type == "Parquet":
            self.parquet_init_fingerprints()
        else:
            print("Wrong file type : {}".format(parser.file_type))

    def getOneFileMinMax(self, file):
        _min = sys.maxsize
        _max = -sys.maxsize
        table = pp.ParquetFile(file)
        # min max
        num_of_row_groups = table.num_row_groups
        for row_group_index in range(num_of_row_groups):
            row_group_contents = table.read_row_group(row_group_index, columns=[self.column])
            for record in row_group_contents.column(self.column):
                # evaluate on int first todo: add other type
                if str(record) == 'None':
                    continue
                record = getRecord(record)
                _min = min(_min, record)
                _max = max(_max, record)
        return _min, _max

    def getindex(self, value):
        startindex, endindex = 0, len(self.intervals) - 1
        while startindex <= endindex:
            middleindex = int((startindex + endindex) / 2)
            if self.intervals[middleindex][0] > value:
                endindex = middleindex - 1
            elif self.intervals[middleindex][1] <= value:
                startindex = middleindex + 1
            else:
                return middleindex
        return -1

    def assign_one_file(self, file):
        self.column_inf[file] = {}
        table = pp.ParquetFile(file)
        num_of_row_groups = table.num_row_groups
        self.indexsize += (num_of_row_groups * args.num_of_intervals / 8)
        for row_group_index in range(num_of_row_groups):
            self.column_inf[file][row_group_index] = bitarray(args.num_of_intervals)
            self.column_inf[file][row_group_index].setall(0)
            row_group_contents = table.read_row_group(row_group_index, columns=[self.column])
            for record in row_group_contents.column(self.column):
                if str(record) == 'None':
                    continue
                record = getRecord(record)
                _index = self.getindex(record)
                self.column_inf[file][row_group_index][_index] = 1

    def parquet_init_fingerprints(self):
        _min = sys.maxsize
        _max = -sys.maxsize
        for file in self.files:
            _cur_min, _cur_max = self.getOneFileMinMax(file)
            _min = min(_cur_min, _min)
            _max = max(_cur_max, _max)

        self.maxvalue = _max
        self.minvalue = _min
        self.interval_size = math.ceil((_max + 1 - _min) / args.num_of_intervals)
        self.intervals = []
        _start = _min
        for _ in range(args.num_of_intervals):
            self.intervals.append((_start, _start + self.interval_size))
            _start += self.interval_size
        self.indexsize += ((len(self.intervals)) * 128 / 8)
        for file in self.files:
            self.assign_one_file(file)

    def update(self, key, rgid, file):
        # new file
        # todo 考虑是否加上超过cardinality
        if rgid not in self.column_inf[file]:
            self.column_inf[file][rgid] = bitarray(args.num_of_intervals)
            self.column_inf[file][rgid].setall(0)
            for _index in range(len(self.intervals)):
                if key >= self.intervals[_index][0] and key < self.intervals[_index][1]:
                    self.column_inf[file][rgid][_index] = 1
                    break
        else:
            for _index in range(len(self.intervals)):
                if key >= self.intervals[_index][0] and key < self.intervals[_index][1]:
                    self.column_inf[file][rgid][_index] = 1
                    break

    def point_search(self, value):
        searchbitarray = bitarray(len(self.column_inf[self.files[0]][0]))
        searchbitarray.setall(0)
        for _index in range(len(self.intervals)):
            if value >= self.intervals[_index][0] and value < self.intervals[_index][1]:
                searchbitarray[_index] = 1
        rowgroups = set()
        for file in self.files:
            for rg in self.column_inf[file].keys():
                andres = self.column_inf[file][rg] & searchbitarray
                if andres.count(1) > 0:
                    rowgroups.add(file + "-" + str(rg))
        return rowgroups

    def range_search(self, vrange):
        rowgroups = set()
        searchbitarray = bitarray(len(self.column_inf[self.files[0]][0]))
        searchbitarray.setall(0)
        for i in range(len(self.intervals)):
            if i in range(len(self.intervals)):
                if self.intervals[i][1] < vrange[0]:
                    continue
                if self.intervals[i][0] > vrange[1]:
                    continue
                searchbitarray[i] = 1
        for file in self.files:
            for rg in self.column_inf[file].keys():
                andres = self.column_inf[file][rg] & searchbitarray
                if andres.count(1) > 0:
                    rowgroups.add(file + "-" + str(rg))
        return rowgroups