import random
import sys
import time
import pyarrow.parquet as pp

def getBlkDis():
    file_path = "/mydata/bigdata/data/wikipedia/wiki9.parquet"
    table = pp.ParquetFile(file_path)
    num_of_row_groups = table.num_row_groups
    record_map = {}
    for row_group_index in range(num_of_row_groups):
        row_group_contents = table.read_row_group(row_group_index, columns=["pagecount"])
        for record in row_group_contents.column("pagecount"):
            if str(record) == 'None':
                continue
            recordValue = int(str(record))
            if recordValue not in record_map:
                record_map[recordValue] = set()
            record_map[recordValue].add(row_group_index)
    records = sorted(record_map)
    for record in records:
        print("record {}, blk num is {}".format(record, len(record_map[record])))

if __name__=="__main__":
    getBlkDis()