# gen_workload_2txt为每种查询生成了1000条查询，太多了，缩小成不重复的200条

def tothin(originfile, dstfile):
    wf = open(dstfile, "w+", encoding="UTF-8")
    f = open(originfile, "r+", encoding="UTF-8")
    inserted = set()
    ispointstart = False
    for line in f:
        if "selectivity" in line:
            wf.write(line)
            inserted.clear()
        elif "range" in line and line not in inserted:
            if len(inserted) < 200:
                wf.write(line)
                inserted.add(line)
        elif "point" in line:
            if not ispointstart:
                ispointstart = True
                inserted.clear()
            if len(inserted) < 200 and line not in inserted:
                wf.write(line)
                inserted.add(line)

    f.close()
    wf.close()

if __name__=="__main__":
    # tothin("openstreet_workload.txt", "openstreet_workloadsub.txt")
    # tothin("lineitem_orderkey_workload.txt", "lineitem_orderkey_workloadsub.txt")
    tothin("ss_ticket_number_1tb_workload.txt", "ss_ticket_number_1tb_workloadsub.txt")