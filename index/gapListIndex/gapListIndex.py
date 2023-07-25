import pickle
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED

import pyarrow.parquet as pp
import os
from index.index import *
from param import *
from index.util import *

class GapListRoot(Index):
    def __init__(self, directory, columns):
        super(GapListRoot, self).__init__(directory, columns)
        self.directory = directory
        self.gapListIndexs = {column:{} for column in columns}
        self.columns = columns

    def generateIndex(self):
        files = os.listdir(self.directory)
        files = [self.directory + _ for _ in files]
        all_task = []
        with ThreadPoolExecutor(20, thread_name_prefix='gaplist_generate_Thread_Pool') as pools:
            for column in self.columns:
                for file in files:
                    cur_task = pools.submit(self.parallel_genfunc, column, file)
                    all_task.append(cur_task)
            wait(all_task, return_when=ALL_COMPLETED)


    def parallel_genfunc(self, column, file):
        self.gapListIndexs[column][file] = GapList(file, column)
        dump_path = args.gaplistIndexDir + file[1:].replace("/", "-") + column
        with open(dump_path, 'wb+') as f:
            pickle.dump(self.gapListIndexs[column][file], f)

    def generateIndexFromFile(self):
        files = os.listdir(self.directory)
        files = [self.directory + _ for _ in files]
        for column in self.columns:
            for file in files:
                dump_path = args.gaplistIndexDir + file[1:].replace("/", "-") + column
                with open(dump_path, 'rb+') as f:
                    self.gapListIndexs[column][file] = pickle.load(f)

    def point_search(self, value, column):
        files = os.listdir(self.directory)
        files = [self.directory + _ for _ in files]
        searched_blocks = []
        for file in files:
            searched_blocks.extend(self.gapListIndexs[column][file].point_search(value))
        return searched_blocks

    def range_search(self, data_range, column):
        files = os.listdir(self.directory)
        files = [self.directory + _ for _ in files]
        searched_blocks = []
        for file in files:
            searched_blocks.extend(self.gapListIndexs[column][file].range_search(data_range))
        return searched_blocks

class GapList:
    def __init__(self, file, column):
        self.file = file
        self.column = column
        # {column:{rowgroup:[gap1, gap2...]}}
        self.column_inf = {}
        self.init_gap_list()

    def init_gap_list(self):
        # todo get range by query process
        if args.file_type == "Parquet":
            self.parquet_init_gap_list()
        else:
            print("Wrong file type : {}".format(parser.file_type))

    def parquet_init_gap_list(self):
        table = pp.ParquetFile(self.file)
        num_of_row_groups = table.num_row_groups
        for row_group_index in range(num_of_row_groups):
            row_group_contents = table.read_row_group(row_group_index, columns=[self.column])
            row_group_records = set()
            for record in row_group_contents.column(self.column):
                # evaluate on int first todo: add other type
                if str(record) == 'None':
                    continue
                record = getRecord(record)
                row_group_records.add(record)
            # sort
            self.column_inf[row_group_index] = {}
            row_group_records = list(row_group_records)
            row_group_records.sort()
            self.column_inf[row_group_index]["range"] = [row_group_records[0], row_group_records[-1]]
            # add gap list
            self.column_inf[row_group_index]["gaps"] = []
            num_of_gap_lists = args.num_of_gap_lists
            # 普通
            # for _ in range(len(row_group_records) - 1):
            #     # todo: for simple, present is only int
            #     if row_group_records[_ + 1] - row_group_records[_] > args.int_interval:
            #         self.column_inf[row_group_index]["gaps"].append((row_group_records[_], row_group_records[_ + 1]))
            #         num_of_gap_lists -= 1
            #         if num_of_gap_lists <= 0:
            #             break
            #改进
            gaps = []
            for _ in range(len(row_group_records) - 1):
                # todo: for simple, present is only int
                if row_group_records[_ + 1] - row_group_records[_] > args.int_interval:
                    gaps.append((row_group_records[_], row_group_records[_ + 1]))
            gaps.sort(key=functools.cmp_to_key(compare_gaps))
            for _ in range(num_of_gap_lists):
                if _ == len(gaps) or len(gaps) == 0:
                    break
                self.column_inf[row_group_index]["gaps"].append(gaps[_])

    def point_search(self, value):
        rowgroups = []
        for rg in self.column_inf.keys():
            if point_cover(self.column_inf[rg]["range"], value):
                included = True
                for gap in self.column_inf[rg]["gaps"]:
                    if point_cover_without_include(gap, value):
                        included = False
                        break
                if included:
                    rowgroups.append(self.file + str(rg))
        return rowgroups



    def range_search(self, range):
        rowgroups = []
        for rg in self.column_inf.keys():
            if range_overlap(self.column_inf[rg]["range"], range[0], range[1]):
                included = True
                for gap in self.column_inf[rg]["gaps"]:
                    if range_in(gap, (range[0], range[1])):
                        included = False
                        break
                if included:
                    rowgroups.append(self.file + str(rg))
        return rowgroups


