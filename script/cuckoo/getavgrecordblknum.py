# 获取平均每个record出现的blk个数
import pyarrow.parquet as pp

def getavgrecordblknum(parquetpath, column):
    table = pp.ParquetFile(parquetpath)
    num_of_row_groups = table.num_row_groups
    recordmap = {}
    for row_group_index in range(num_of_row_groups):
        row_group_contents = table.read_row_group(row_group_index,columns=[column])
        for record in row_group_contents.column(column):
            rv = int(str(record))
            if rv not in recordmap:
                recordmap[rv] = set()
            recordmap[rv].add(row_group_index)
    totalblk = 0
    for rv in recordmap:
        totalblk += len(recordmap[rv])
    print("for parquetpath {}, column {}, avg record blk num is {}".format(
        parquetpath, column, totalblk/len(recordmap)
    ))

if __name__=="__main__":
    parquetfiles = ["/mydata/bigdata/data/wikipediasub/wiki14.parquet",
                    "/mydata/bigdata/data/openstreetsub/0.parquet",
                    "/mydata/bigdata/data/tpcds_parquet_100/store_salessub/store_sales0"]
    columns = ["pagecount", "lon", "ss_ticket_number"]
    for i in range(len(columns)):
        getavgrecordblknum(parquetfiles[i], columns[i])
