import bisect
import math
import os
import random
import sys
from workload.util import *


class WorkLoad:
    def __init__(self, directory):
        self.directory = directory
        self.range_selective = (0.005, 0.05)
        self.point_selective = 0.001
        self.save_path = "workload.txt"
        self.data = None

    def init(self, attribute):
        # read data
        self.record_num, self.data, self.num_of_records = read_data(self.directory, attribute)

    def random_generate_range(self, selectivity):
        if self.data is None:
            print("Workload is not init!")
            exit(0)

        # read
        start_index = random.randint(0, len(self.data) - 1)
        end_index = start_index
        start = self.data[start_index]
        end = start
        content_num = self.record_num[start]
        while self.num_of_records * selectivity > content_num:
            if end_index == len(self.data):
                break

            end_index += 1
            if end_index == len(self.data):
                break
            end = self.data[end_index]
            content_num += self.record_num[end]
        #print("present selectivity is {}, range is {}".format(content_num / self.num_of_records, (start, end)))
        return (start, end)

    def random_generate_point(self, isin = True, rangelist = []):
        if self.data is None:
            print("Workload is not init!")
            exit(0)
        if isin:
            _index = random.randint(0, len(self.data) - 1)
            return self.data[_index]
        else:
            while True:
                _index = random.randint(0, len(self.data) - 2)
                if self.data[_index+1] > self.data[_index] + 1:
                    tempv = (self.data[_index+1] + self.data[_index]) // 2
                    for _range in rangelist:
                        if tempv >= _range[0] and tempv <= _range[1]:
                            return tempv


    def genseries_range(self, selectivity, gnum):
        """
        :param selectivity: 需要生成的选择度
        :param gnum: 需要生成的查询负载个数
        :return:
        """
        # 用以一次性生成一组数据,gnum表示要生成的个数，主要是为了保证均衡
        data_step = int(len(self.data) / (gnum+2))
        genranges = []
        curindex = random.randint(0, data_step-1)
        for oi in range(gnum):
            start_index = curindex
            end_index = start_index
            start = self.data[start_index]
            end = start
            content_num = self.record_num[start]
            while self.num_of_records * selectivity > content_num:
                if end_index == len(self.data):
                    break
                end_index += 1
                if end_index == len(self.data):
                    break
                end = self.data[end_index]
                content_num += self.record_num[end]
            genranges.append((start, end))
            curindex += data_step
            if curindex >= len(self.data):
                break
        return genranges

    def genseries_point(self, gnum, appearpercent=0.1):
        """
        :param gnum: 需要生成的点查负载个数
        :param appearpercent: 需要随机生成的点查中，出现过的值的占比
        :return:
        """
        # 生成一组点查询，包括两部分：出现过的和没出现过的， 定义90个未出现的搭配10个出现的
        appearnum = math.ceil(gnum * appearpercent)
        res = []
        step = int(len(self.data) / appearnum)
        startindex = random.randint(0, step - 1)
        for i in range(appearnum):
            res.append(self.data[startindex])
            startindex += step
        for i in range(1, len(self.data)):
            if self.data[i] - self.data[i-1] > 1:
                temp = (self.data[i] + self.data[i-1]) // 2
                assert temp != self.data[i]
                assert temp != self.data[i-1]
                res.append(temp)
                if len(res) >= gnum:
                    return res
        return res


# test
# w = WorkLoad("../data/")
#
# print(w.random_generate_range("cs_sold_date_sk", 0.1))