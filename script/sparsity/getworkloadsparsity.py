import os
import pyarrow.parquet as pp
import sys

sys.path.append("../..")
from index.index import *
from index.util import getRecord
from param import args

def getDirSparsity(parquetdir, column):
    files = os.listdir(parquetdir)
    minvalue, maxvalue = sys.maxsize, -sys.maxsize + 1
    valueset = set()
    for file in files:
        table = pp.ParquetFile(parquetdir + file)
        num_of_row_groups = table.num_row_groups
        for row_group_index in range(num_of_row_groups):
            row_group_contents = table.read_row_group(row_group_index, columns=[column])
            for record in row_group_contents.column(column):
                if str(record) == 'None':
                    continue
                record = getRecord(record)
                valueset.add(record)
                minvalue = min(minvalue, record)
                maxvalue = max(maxvalue, record)
    print("for parquet dir {}, column {}, density is {}".format(
        parquetdir, column, len(valueset) / (maxvalue - minvalue + 1)
    ))

def getOpenstreetDensity(parquetdir, column):
    files = os.listdir(parquetdir)
    isinlist = [False] * 4294802501
    for file in files:
        table = pp.ParquetFile(parquetdir + file)
        num_of_row_groups = table.num_row_groups
        for row_group_index in range(num_of_row_groups):
            row_group_contents = table.read_row_group(row_group_index, columns=[column])
            for record in row_group_contents.column(column):
                if str(record) == 'None':
                    continue
                record = getRecord(record)
                isinlist[record + 2147483648] = True
    validcount = 0
    for v in isinlist:
        if v:
            validcount += 1
    print("for parquet dir {}, column {}, density is {}".format(
        parquetdir, column, validcount / 4294802501
    ))

if __name__=="__main__":
    parquetdir = "/mydata/bigdata/data/openstreet/parquet/"
    column = "lon"
    getOpenstreetDensity(parquetdir, column)