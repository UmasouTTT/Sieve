import pickle

import pyarrow.parquet as pp
import os
from index.index import *
from index.util import getRecord
from param import args


class ReversedAllIndex(Index):
    def __init__(self, directory, columns):
        super(ReversedAllIndex, self).__init__(directory, columns)
        self.directory = directory
        self.columns = columns
        self.reversedIndexs = {column: {} for column in columns}

    def generateIndexes(self):
        files = os.listdir(self.directory)
        files = [self.directory + _ for _ in files]
        for column in self.columns:
            dump_path = args.reversedIndexDir + self.directory[1:].replace("/", "-") + column
            for file in files:
                table = pp.ParquetFile(file)
                num_of_row_groups = table.num_row_groups
                for row_group_index in range(num_of_row_groups):
                    row_group_contents = table.read_row_group(row_group_index, columns=[column])
                    for record in row_group_contents.column(column):
                        if str(record) == 'None':
                            continue
                        record = getRecord(record)
                        if record not in self.reversedIndexs[column]:
                            self.reversedIndexs[column][record] = set()
                        self.reversedIndexs[column][record].add(file + "-" + str(row_group_index))
            with open(dump_path, 'wb+') as f:
                pickle.dump(self.reversedIndexs[column], f)

    def generateIndexFromFile(self):
        for column in self.columns:
            dump_path = args.reversedIndexDir + self.directory[1:].replace("/", "-") + column
            with open(dump_path, 'rb+') as f:
                self.reversedIndexs[column] = pickle.load(f)

    def point_search(self, value, column):
        result_rgs = self.reversedIndexs[column].get(value, set())
        return result_rgs

    def range_search(self, data_range, column):
        result_rgs = set()
        for value in range(data_range[0], data_range[1] + 1):
            result_rgs = result_rgs.union(self.reversedIndexs[column].get(value, set()))
        return result_rgs

