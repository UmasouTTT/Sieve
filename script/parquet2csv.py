import pandas as pd
import pyarrow.parquet as pq


def parquet2csv(parquetpath, csvpath):
    # 读取Parquet文件
    parquet_file = pq.ParquetFile(parquetpath)
    df = parquet_file.read().to_pandas()
    # 将数据保存为CSV文件
    df.to_csv(csvpath, index=False)