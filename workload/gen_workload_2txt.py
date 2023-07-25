import os
import random
import sys
sys.path.append("../")
from index.gapListIndex.minMaxIndex import *
from index.learnedIndexDemo.IndexOnTheLake import LakeIndex, RootLakeIndex
from index.gapListIndex.reversedIndex import ReversedIndex
from workload.util import *

def gen_workload(directory, column):
    """
    随机范围查+点查的负载生成，生成结果不会带opt索引的值
    :param directory:
    :param column:
    """
    files = os.listdir(directory)
    files = [directory + _ for _ in files]
    data_list = list()
    for file in files:
        table = pp.ParquetFile(file)
        num_of_row_groups = table.num_row_groups
        for row_group_index in range(num_of_row_groups):
            row_group_contents = table.read_row_group(row_group_index, columns=[column])
            for record in row_group_contents.column(column):
                # evaluate on int first todo: add other type
                if str(record) == 'None':
                    continue
                record = getRecord(record)
                data_list.append(record)
    data_list.sort()
    sizel = len(data_list)
    print("sizel is {}".format(sizel))
    selectivities = [0.00001, 0.0001, 0.001]
    cur_select_time = 1000
    for selectivity in selectivities:
        print("directory {}, column {}, selectivity {} start".format(directory, column, selectivity))
        for i in range(cur_select_time):
            step = int(sizel * selectivity)
            _start = random.randint(0, sizel - step)
            _end = min(_start + step, sizel - 1)
            print("range:{},{}".format(data_list[_start], data_list[_end]))
    # point gen
    for i in range(cur_select_time):
        _index = random.randint(0, sizel-1)
        print("point:{}".format(data_list[_index]))

def gen_range_workload(directory, column):
    """
    较为均匀的范围查负载生成，因为照顾了域值范围内每个均匀的小范围，生成结果带opt索引的值
    :param directory:
    :param column:
    :return:
    """
    # 生成这样的负载，列出所有唯一的值，选择度就是唯一值向后推的个数，然后选择500个查询。步长为切割为500份。
    files = os.listdir(directory)
    files = [directory + _ for _ in files]
    recordmap = {}
    for file in files:
        table = pp.ParquetFile(file)
        num_of_row_groups = table.num_row_groups
        for row_group_index in range(num_of_row_groups):
            row_group_contents = table.read_row_group(row_group_index, columns=[column])
            for record in row_group_contents.column(column):
                # evaluate on int first todo: add other type
                if str(record) == 'None':
                    continue
                record = getRecord(record)
                if record not in recordmap:
                    recordmap[record] = set()
                recordmap[record].add(file + "_" + str(row_group_index))

    data_list = sorted(recordmap)
    sizel = len(data_list)
    def getoptres(sindex, eindex):
        curset = set()
        for i in range(sindex, eindex+1):
            curset = curset.union(recordmap[data_list[i]])
        return len(curset)
    print("sizel is {}".format(sizel))
    selectivities = [0.00001, 0.0001, 0.001]
    selectlen = [int(v * sizel) for v in selectivities]
    workload = [[] for i in range(3)]
    optres = [[] for i in range(3)]
    loopstep = int(sizel / 500)
    print("loop step is {}".format(loopstep))
    print("cur select step is {}".format(selectlen))
    curstart = random.randint(0, loopstep-1)
    while curstart < sizel:
        curends = [min(curstart + v, sizel - 1) for v in selectlen]
        for i in range(3):
            workload[i].append([data_list[curstart], data_list[curends[i]]])
            optres[i].append(getoptres(curstart, curends[i]))
        curstart += loopstep
    for j in range(3):
        print("select {} workload".format(j+1))
        for i in range(len(workload[0])):
            print("search range is {}-{}, opt res is {}".format(workload[j][i][0], workload[j][i][1], optres[j][i]))
        print("opt avg is {}".format(sum(optres[j]) / len(optres[j])))

def gen_workload_opt(directory, column):
    """
    较为均匀的点查负载生成，因为照顾了域值范围内每个均匀的小范围，生成结果带opt索引的值
    :param directory:
    :param column:
    """
    files = os.listdir(directory)
    files = [directory + _ for _ in files]
    data_list = list()
    kvmap = {}
    for file in files:
        table = pp.ParquetFile(file)
        num_of_row_groups = table.num_row_groups
        for row_group_index in range(num_of_row_groups):
            row_group_contents = table.read_row_group(row_group_index, columns=[column])
            for record in row_group_contents.column(column):
                # evaluate on int first todo: add other type
                if str(record) == 'None':
                    continue
                record = getRecord(record)
                if record not in kvmap:
                    kvmap[record] = set()
                data_list.append(record)
                kvmap[record].add(file + "_" + str(row_group_index))
    data_list.sort()
    #生成2000个点查，步长为data_list长度/2000
    datal = len(data_list)
    step = int(datal / 2000)
    curindex = 0
    while curindex < datal:
        _key = data_list[curindex]
        _len = len(kvmap[_key])
        print("key is:{};optlen is:{}".format(_key, _len))
        curindex += step



if __name__=="__main__":
    directory = ""
    is_single_file = args.singleFile
    if args.wltype == "tpch" or args.wltype == "tpcds":
        if is_single_file:
            directory = "/mydata/bigdata/data/{}_{}/{}sub/".format(args.wltype, args.sf, args.table)
        else:
            directory = "/mydata/bigdata/data/{}_{}/{}/".format(args.wltype, args.sf, args.table)
    elif args.wltype == "wiki":
        if is_single_file:
            directory = "/mydata/bigdata/data/wikipediasub/"
        else:
            directory = "/mydata/bigdata/data/wikipedia/"
    elif args.wltype == "openstreet":
        directory = "/mydata/bigdata/data/openstreet/parquet/"
        args.column = "lon"
    elif args.wltype == "worse_data":
        directory = "/mydata/bigdata/data/worse_data/"
        args.column = "liuid"
    # gen_workload(directory, args.column)
    gen_range_workload("/mydata/bigdata/data/wikipedia/", "pagecount")