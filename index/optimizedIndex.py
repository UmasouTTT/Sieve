from index.index import *
import os
import pyarrow.parquet as pp
from index.util import *
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, as_completed

class OptimizedIndex(Index):
    def __init__(self, directory, columns):
        super(OptimizedIndex, self).__init__(directory, columns)

    def point_search(self, value, column):
        rowgroups = []
        files = os.listdir(self.directory)
        files = [self.directory + _ for _ in files]
        for file in files:
            table = pp.ParquetFile(file)
            num_of_row_groups = table.num_row_groups
            for row_group_index in range(num_of_row_groups):
                row_group_contents = table.read_row_group(row_group_index, columns=[column])
                for record in row_group_contents.column(column):
                    # evaluate on int first todo: add other type
                    if str(record) == 'None':
                        continue
                    record = getRecord(record)
                    if record == value:
                        rowgroups.append(file + "-" + str(row_group_index))
                        break
        return rowgroups

    def perfile_rangesearch(self, file, data_range, column):
        rowgroups = set()
        table = pp.ParquetFile(file)
        num_of_row_groups = table.num_row_groups
        for row_group_index in range(num_of_row_groups):
            row_group_contents = table.read_row_group(row_group_index, columns=[column])
            for record in row_group_contents.column(column):
                # evaluate on int first todo: add other type
                if str(record) == 'None':
                    continue
                record = getRecord(record)
                if value_in(data_range, record):
                    rowgroups.add(file + "-" + str(row_group_index))
                    break
        return rowgroups
    def range_search(self, data_range, column):
        rowgroups = set()
        files = os.listdir(self.directory)
        files = [self.directory + _ for _ in files]
        all_task = []
        with ThreadPoolExecutor(20, thread_name_prefix='opt_search_pools') as pools:
            for file in files:
                cur_task = pools.submit(self.perfile_rangesearch, file, data_range, column)
                all_task.append(cur_task)
            wait(all_task, return_when=ALL_COMPLETED)
            for future in as_completed(all_task):
                rowgroups.union(future.result())
        return rowgroups