# pip3 install awscli
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
                if txt_index > 188:
                    break

def txts_2_parquets(txt_file_dir, parquet_file_dir):
    schema = pa.schema([
        ('id', pa.int64()),
        ('lon', pa.int64())
    ])
    txt_files = os.listdir(txt_file_dir).sort()
    for i in range(min(100, len(txt_files))):
        ids = []
        lons = []
        f = open(txt_file_dir + txt_files[i], "r+", encoding="UTF-8")
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
        pq.write_table(table, parquet_file_dir + txt_files[i].split(".")[0] + ".parquet", row_group_size=50000)

if __name__=="__main__":
    os.system("pip3 install awscli")
    os.system("aws s3 cp --no-sign-request s3://osm-pds/planet-history/history-latest.orc ./")
    orcpath, txtdir, parquetdir = "./history-latest.orc", "./mapstxt/", "./Maps/"
    if not os.path.exists(txtdir):
        os.makedirs(txtdir)
    if not os.path.exists(parquetdir):
        os.makedirs(parquetdir)
    read_orc_write_txt(orcpath, txtdir)
    txts_2_parquets(txtdir, parquetdir)
    os.system("rm -rf mapstxt")