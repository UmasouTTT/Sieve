import os


def getGapPercent(parquetdir, columnname):
    # 开一个长度为
    files = os.listdir(parquetdir)
    for file in files:
        parquet_file = parquetdir + file
    return None