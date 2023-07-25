import pyarrow.parquet as pp
import pyarrow as pa


# 提取出只包含一个列的parquet文件
def trans(originparquet, column):
    table = pp.ParquetFile(originparquet)
    num_of_row_groups = table.num_row_groups
    records = []
    for row_group_index in range(num_of_row_groups):
        row_group_contents = table.read_row_group(row_group_index, columns=[column])
        for record in row_group_contents.column(column):
            recordv = int(str(record))
            records.append(recordv)
    schema = pa.schema([
        ('key', pa.int64())
    ])
    batch = pa.RecordBatch.from_arrays([pa.array(records, pa.int64())],
                                       schema=schema)
    table = pa.Table.from_batches([batch])
    pp.write_table(table, '{}.parquet'.format(column), row_group_size=50000)

if __name__=="__main__":
    originparquets = ["/mydata/bigdata/data/tpcds_parquet_100/store_sales/store_sales1",
                      "/mydata/bigdata/data/wikipedia/wiki14.parquet",
                      "/mydata/bigdata/data/openstreet/1.parquet"]
    columns = ["ss_ticket_number", "pagecount", "lon"]
    for i in range(1):
        trans(originparquets[i], columns[i])