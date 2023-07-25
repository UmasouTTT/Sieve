# 将wiki page count数据集转成parquet格式
import os
import pyarrow as pa
import pyarrow.parquet as pq

os.system("chmod +x getwiki.sh")
os.system("./getwiki.sh")

# 定义 Schema
schema = pa.schema([
    ('pagename', pa.string()),
    ('pageinfo', pa.string()),
    ('pagecategory', pa.uint64()),
    ('pagecount', pa.uint64())
])

files = os.listdir("wikitemp/")
count = 0
l1 = []
l2 = []
l3 = []
l4 = []
for file in files:
    if "pagecounts" in file:
        count += 1
        f = open("wikitemp/" + file, "r+", encoding="UTF-8")
        for line in f:
            a1 = line.strip().split(" ")[0]
            a2 = line.strip().split(" ")[1]
            a3 = int(line.strip().split(" ")[2])
            a4 = int(line.strip().split(" ")[3])
            l1.append(a1)
            l2.append(a2)
            l3.append(a3)
            l4.append(a4)
        f.close()
        if count % 5 == 0:
            batch = pa.RecordBatch.from_arrays([pa.array(l1, pa.string()),
                                                pa.array(l2, pa.string()),
                                                pa.array(l3, type=pa.uint64()),
                                                pa.array(l4, type=pa.uint64())],
                                               schema=schema)
            table = pa.Table.from_batches([batch])
            pq.write_table(table, 'wiki{}.parquet'.format(int(count/10)), row_group_size=50000)
            l1.clear()
            l2.clear()
            l3.clear()
            l4.clear()
    else:
        continue
if len(l1) > 0:
    batch = pa.RecordBatch.from_arrays([pa.array(l1, pa.string()),
                                        pa.array(l2, pa.string()),
                                        pa.array(l3, type=pa.uint64()),
                                        pa.array(l4, type=pa.uint64())],
                                        schema=schema)
    table = pa.Table.from_batches([batch])
    pq.write_table(table, 'wiki{}.parquet'.format(int(count / 10)), row_group_size=50000)

if not os.path.exists("Wikipedia"):
    os.makedirs("Wikipedia")
os.system("mv wiki*.parquet Wikipedia")
os.system("rm -rf wikitemp")