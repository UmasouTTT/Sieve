import os
import sys
import pyarrow.parquet as pp
from index.util import *

def read_data(directory, attribute):
    record_num = {}
    data = set()
    num_of_records = 0

    files = os.listdir(directory)
    files = [directory + _ for _ in files]
    for file in files:
        table = pp.ParquetFile(file)
        num_of_row_groups = table.num_row_groups
        for row_group_index in range(num_of_row_groups):
            row_group_contents = table.read_row_group(row_group_index, columns=[attribute])
            for record in row_group_contents.column(attribute):
                # evaluate on int first todo: add other type
                if str(record) == 'None':
                    continue
                record = getRecord(record)
                data.add(record)
                if record not in record_num:
                    record_num[record] = 0
                record_num[record] += 1
                num_of_records += 1

    data = list(data)
    data.sort()
    return record_num, data, num_of_records


def dataset_value_range(data):
    return data[0], data[-1]

def dataset_value_num(data_num, value):
    return data_num[value]
