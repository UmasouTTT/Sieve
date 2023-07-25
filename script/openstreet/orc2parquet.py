# 从orc到parquet的转换
# 数据集说明地址：https://github.com/awslabs/open-data-docs/tree/main/docs/osm-pds
# 数据集引用地址：https://registry.opendata.aws/osm/
# segment_error 100000
# 数据集下载方式
# aws s3 ls --no-sign-request s3://osm-pds/
# aws s3 cp --no-sign-request s3://osm-pds/planet-history/history-latest.orc .
# 最终只留下0-188（按字符串顺序的这么多个，wc -l是103个）
import os
import pyarrow as pa
from pyarrow import orc
import pyarrow.parquet as pq

def read_orc_write_txt(orc_file_path, txt_file_dir):
    orc_file = orc.ORCFile(orc_file_path)
    print(orc_file.schema)
    stripe_num = orc_file.nstripes

    txt_index = 0
    columns = ["id", "lon"]
    temp_values = [[] for i in range(len(columns))]
    for i in range(stripe_num):
        stripe_values = orc_file.read_stripe(i, columns)
        columnvalues = []
        for j in range(len(columns)):
            columnvalues.append(stripe_values.column(j))
        for j in range(len(columnvalues[0])):
            temp_values[0].append(columnvalues[0][j])
            temp_values[1].append(columnvalues[1][j])
            if len(temp_values[0]) >= 50000 * 800:
                f = open(txt_file_dir + "{}.txt".format(txt_index), "w+", encoding="UTF-8")
                for tindex in range(len(temp_values[0])):
                    f.write(str(temp_values[0][tindex]) + "," + str(temp_values[1][tindex]) + "\n")
                f.close()
                temp_values = [[] for oo in range(len(columns))]
                txt_index += 1
                if txt_index >= 188:
                    return
        # for j in range(len(columns)):
        #     value_arrays = stripe_values.column(j)
        #     for value in value_arrays:
        #         temp_values[j].append(value)
        # if len(temp_values[0]) >= 50000 * 800:
        #     f = open(txt_file_dir + "{}.txt".format(txt_index), "w+", encoding="UTF-8")
        #     for tindex in range(len(temp_values[0])):
        #         f.write(str(temp_values[0][tindex]) + "," + str(temp_values[1][tindex]) + "\n")
        #     f.close()
        #     temp_values = [[] for oo in range(len(columns))]
        #     txt_index += 1

def txts_2_parquets(txt_file_dir, parquet_file_dir):
    schema = pa.schema([
        ('id', pa.int64()),
        ('lon', pa.int64())
    ])
    txt_files = os.listdir(txt_file_dir)
    for txt_file in txt_files:
        ids = []
        lons = []
        f = open(txt_file_dir + txt_file, "r+", encoding="UTF-8")
        for line in f:
            id = int(line.strip().split(",")[0])
            ids.append(id)
            lon = line.strip().split(",")[1]
            if lon == 'None':
                lons.append(0)
            else:
                lons.append(int(float(lon)*10000000))
        f.close()
        batch = pa.RecordBatch.from_arrays([pa.array(ids, pa.int64()),
                                            pa.array(lons, pa.int64())],
                                           schema=schema)
        table = pa.Table.from_batches([batch])
        pq.write_table(table, parquet_file_dir + txt_file.split(".")[0] + ".parquet", row_group_size=50000)

def extractOneFile(orc_file_path):
    orc_file = orc.ORCFile(orc_file_path)
    stripe_num = orc_file.nstripes

    txt_index = 0
    columns = ["id", "type", "tags", "lat", "lon", "nds", "members", "changeset", "timestamp", "uid", "user", "version", "visible"]
    temp_values = [[] for i in range(len(columns))]
    for stripindex in range(stripe_num):
        stripe_values = orc_file.read_stripe(stripindex, columns)
        columnvalues = []
        for columnindex in range(len(columns)):
            columnvalues.append(stripe_values.column(columnindex))
        for recordindex in range(len(columnvalues[0])):
            for oi in range(len(temp_values)):
                temp_values[oi].append(columnvalues[oi][recordindex])
            if len(temp_values[0]) >= 50000 * 800:
                break
        if len(temp_values[0]) >= 50000 * 800:
            break
    f = open("firstcompelete.txt", "w+", encoding="UTF-8")
    for recordindex in range(len(temp_values[0])):
        for oj in range(len(temp_values) - 1):
            f.write(str(temp_values[oj][recordindex]) + ",")
        f.write(str(temp_values[-1][recordindex]) + "\n")
    f.close()
    return


def txt_2_parquets(txt_file="firstcompelete.txt"):
    schema = pa.schema([
        ('id', pa.string()),
        ('type', pa.string()),
        ('tags', pa.string()),
        ('lat', pa.string()),
        ('lon', pa.string()),
        ('nds', pa.string()),
        ('members', pa.string()),
        ('changeset', pa.string()),
        ('timestamp', pa.string()),
        ('uid', pa.string()),
        ('user', pa.string()),
        ('version', pa.string()),
        ('visible', pa.string())
    ])
    records = [[] for i in range(13)]
    f = open(txt_file, "r+", encoding="UTF-8")
    for line in f:
        currecords = line.strip().split(",")
        for i in range(len(records)):
            records[i].append(currecords[i])
    f.close()
    batch = pa.RecordBatch.from_arrays([
                                        pa.array(records[0], pa.string()),
                                        pa.array(records[1], pa.string()),
                                        pa.array(records[2], pa.string()),
                                        pa.array(records[3], pa.string()),
                                        pa.array(records[4], pa.string()),
                                        pa.array(records[5], pa.string()),
                                        pa.array(records[6], pa.string()),
                                        pa.array(records[7], pa.string()),
                                        pa.array(records[8], pa.string()),
                                        pa.array(records[9], pa.string()),
                                        pa.array(records[10], pa.string()),
                                        pa.array(records[11], pa.string()),
                                        pa.array(records[12], pa.string())
                                        ],
                                       schema=schema)
    table = pa.Table.from_batches([batch])
    pq.write_table(table, txt_file.split(".")[0] + ".parquet", row_group_size=50000)

if __name__=="__main__":
    orcpath, txtdir, parquetdir = "/mydata/bigdata/data/openstreet/history-latest.orc", "/mydata/bigdata/data/openstreet/txt/", "/mydata/bigdata/data/openstreet/parquet/"
    # if not os.path.exists(txtdir):
    #     os.makedirs(txtdir)
    # if not os.path.exists(parquetdir):
    #     os.makedirs(parquetdir)
    # read_orc_write_txt(orcpath, txtdir)
    # txts_2_parquets(txtdir, parquetdir)
    txt_2_parquets()