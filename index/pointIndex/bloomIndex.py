import os
from pybloom_live import ScalableBloomFilter
import pyarrow.parquet as pp
from index.index import *
from index.util import *
from param import *

class RootBloomIndex(Index):
    def __init__(self, directory, columns):
        super(RootBloomIndex, self).__init__(directory, columns)
        self.directory = directory
        self.bloomIndexs = set()
        self.columns = columns
        self.init()

    def init(self):
        files = os.listdir(self.directory)
        files = [self.directory + _ for _ in files]
        for file in files:
            self.bloomIndexs.add(BloomIndex(file, self.columns, self))

    def point_search(self, value, column):
        searched_blocks = list()
        for bloomIndex in self.bloomIndexs:
            searched_blocks.extend(bloomIndex.point_search(column, value))
        return searched_blocks

class BloomIndex():
    def __init__(self, file, columns, sieve):
        self.file = file
        self.columns = columns
        self.sieve = sieve
        # {column:{rowgroup:[gap1, gap2...]}}
        self.columns_inf = {column: {} for column in self.columns}
        self.init_bloom_index()

    def init_bloom_index(self):
        # todo get range by query process
        if args.file_type == "Parquet":
            self.parquet_init_bloom_index()
        else:
            print("Wrong file type : {}".format(parser.file_type))

    def parquet_init_bloom_index(self):
        for column in self.columns:
            table = pp.ParquetFile(self.file)
            num_of_row_groups = table.num_row_groups
            for row_group_index in range(num_of_row_groups):
                self.columns_inf[column][row_group_index] = ScalableBloomFilter(initial_capacity=args.bloom_init_capacity, error_rate=args.bloom_error_rate)
                row_group_contents = table.read_row_group(row_group_index, columns=[column])
                for record in row_group_contents.column(column):
                    # evaluate on int first todo: add other type
                    if str(record) == 'None':
                        continue
                    record = getRecord(record)
                    self.columns_inf[column][row_group_index].add(record)

    def point_search(self, column, value):
        rowgroups = []
        for rg in self.columns_inf[column].keys():
            if value in self.columns_inf[column][rg]:
                rowgroups.append(self.file + "-" + str(rg))
        return rowgroups