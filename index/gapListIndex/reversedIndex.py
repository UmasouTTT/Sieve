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


class ReversedIndex(Index):
    def __init__(self, directory, columns):
        super(ReversedIndex, self).__init__(directory, columns)
        self.directory = directory
        self.columns = columns
        self.indexs = {}
        self.fitingsize = 0

    def generateOneFile(self, file, column):
        print("start reversed gen of {} {}".format(file, column))
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
        print("for file {}, column {}, fiting tree size is {}(B)".format(file, column, num_of_row_groups * len(tempdict) / 8))
        # self.indexs[file] = copy.deepcopy(tempdict)
        dump_path = args.reversedIndexDir + file[1:].replace("/", "-") + column
        with open(dump_path, 'wb+') as f:
            pickle.dump(tempdict, f)
        del tempdict

    def generateIndexes(self):
        files = os.listdir(self.directory)
        files = [self.directory + _ for _ in files]
        for column in self.columns:
            for file in files:
                self.generateOneFile(file, column)
            print("for dir {}, column {}, fiting size is {}".format(self.directory, column, self.fitingsize))

    def generateIndexFromFile(self, column):
        files = os.listdir(self.directory)
        files = [self.directory + _ for _ in files]
        for file in files:
            dump_path = args.reversedIndexDir + file[1:].replace("/", "-") + column
            with open(dump_path, 'rb+') as f:
                self.indexs[file] = pickle.load(f)
            f.close()

    def point_search(self, value, column):
        result_rgs = set()
        for file_key in self.indexs:
            tempset = self.indexs[file_key].get(value, set())
            for tempv in tempset:
                result_rgs.add(file_key + "-" + str(tempv))
        return result_rgs

    def range_search(self, data_range, column):
        result_rgs = set()
        for file_key in self.indexs:
            tempdict = self.indexs[file_key]
            curtime = time.time()
            tempsrotedrecords = sorted(tempdict)
            print("sort time is {}".format(time.time() - curtime))
            startindex = bisect.bisect_left(tempsrotedrecords, data_range[0])
            for i in range(startindex, len(tempsrotedrecords)):
                if tempsrotedrecords[i] > data_range[1]:
                    break
                tempset = tempdict.get(tempsrotedrecords[i], set())
                for tempv in tempset:
                    result_rgs.add(file_key + "-" + str(tempv))
        return result_rgs

if __name__=="__main__":
    wltype = "tpch"
    table = "partsupp"
    column = "partkey"
    directory = "/mydata/bigdata/data/{}_parquet_100_rewrite/{}/".format(wltype, table)
    reversedIndex = ReversedIndex(directory, columns=[column])
    reversedIndex.generateIndexFromFile()
    print(reversedIndex.range_search([19500521, 19500525], column))
    print(reversedIndex.point_search(19500521, column))

