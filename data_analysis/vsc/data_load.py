import re
import linecache
import pandas as pd
import os
import numpy as np
import sys


class DataLoad(object):
    def __init__(self, path):
        # 文件夹绝对路径
        self.path = path
        # 获取文件名
        self.file_list = self.get_file(self.path)

    # 获取文件名
    def get_file(self, path):
        return [filename for filename in os.listdir(path)]

    # 以PT-ER提取整块记录
    def extract_record_base_pter(self, line_list):
        pt_list = list()
        er_list = list()
        for i in range(len(line_list)):
            if re.match("^PT", line_list[i]):
                pt_list.append(i)
            if re.match("^ER", line_list[i]):
                er_list.append(i)
        df = pd.DataFrame(data=[pt_list, er_list]).T
        result = list()
        for row in df.iterrows():
            result.append("".join(line_list[row[1][0]: row[1][1] + int(1)]))
        print(result)
        return result

    # 以***TDA***为标记提取块
    def extract_record_base_tda(self, line_list):
        tda_list = list()
        for i in range(len(line_list)):
            if line_list[i][:9] == '***TDA***':
                tda_list.append(i)
        result = list()
        for i in range(len(tda_list)-1):
            result.append(''.join(line_list[tda_list[i]:tda_list[i+1]]))
        return result

    # 获取所有记录
    def get_all_records_base_pter(self):
        all_records = list()
        for file in self.file_list:
            if file.split(".")[1] == "txt":
                self.file_name = file.split(".")[0]
                # 获取每一行数据
                line_list = linecache.getlines(self.path+'\\' + file)
                #line_list = linecache.getlines("./" + file)
                # 从所有行中找PT-ER记录块，并返回为每个文件中的专利记录
                result_list = self.extract_record_base_pter(line_list)
                all_records.extend(result_list)
        return all_records

    def get_all_records_base_tda(self):
        all_records = list()
        for file in self.file_list:
            if file.split(".")[1] == "txt":
                self.file_name = file.split(".")[0]
                # 获取每一行数据
                line_list = linecache.getlines(self.path+'\\' + file)
                #line_list = linecache.getlines("./" + file)
                # 从所有行中找***TDA***记录块，并返回为每个文件中的专利记录
                result_list = self.extract_record_base_tda(line_list)
                all_records.extend(result_list)
        # print(all_records)
        return all_records

    # pn用；分隔的字符串
    def get_IP(self, data):
        lines = data.split('\n')
        for line in lines:
            if line[0:2] == 'IP':
                return line[2:].strip()

    def get_IP_list(self, data):
        ip = self.get_IP(data)
        ip_list = ip.split(';')
        ip_list = [i.strip() for i in ip_list]
        return ip_list

    def get_TI(self, data):
        lines = data.split('\n')
        for line in lines:
            if line[0:2] == 'TI':
                return line[2:].strip()
    #用正则表达式来提取出某一行
    def get_specified_lines(self, data, reg, flags=0):
        line_list = data.split('\n')
        pattern = re.compile(reg, flags=flags)
        result_list = list()
        for line in line_list:
            result = re.findall(pattern, line)
            if result is not None:
                result_list.extend(result)
        return result_list[0]

# if __name__ == "__main__":
#     dl = DataLoad(r'F:\project\vsc_workplace\data')
    
