import re
import linecache
import pandas as pd
import os
import json
'''
先获取文件加下所有文本数据的文件名，再把所有数据按PT-ER为一条记录提取存入列表，遍历列表，提取PN、PI、UT、CP、被引编号
'''


class DataProcess(object):
    def __init__(self, path):
        # 文件夹绝对路径
        self.path = path
        # 获取文件名
        self.file_list = self.get_file(self.path)

    # 获取文件名
    def get_file(self, path):
        return [filename for filename in os.listdir(path)]

    # 提取整块记录
    def roughProcessData(self, line_list):
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
            result.append("".join(line_list[row[1][0]:row[1][1] + int(1)]))
        return result

    def getPN(self, data):
        lines = data.split('\n')
        for line in lines:
            if line[0:2] == 'PN':
                return line[2:].strip()

    def getPN_list(self, data):
        pn = self.getPN(data)
        pn_list = pn.split(';')
        pn_list = [i.strip() for i in pn_list]
        return pn_list

    def getUT(self, data):
        lines = data.split('\n')
        for line in lines:
            if line[0:2] == 'UT':
                return line[2:].strip()

    # 同义词字典
    def getDict(self, data):
        return {self.getUT(data):self.getPN_list(data)}

    # 获取所有记录
    def getAllRecords(self):
        all_records = list()
        for file in self.file_list:
            if file.split('.')[1] == "txt":
                self.file_name = file.split('.')[0]
                # 获取每一行数据
                line_list = linecache.getlines('./' + file)
                # 从所有行中找PT-ER记录块，并返回为每个文件中的专利记录
                result_list = self.roughProcessData(line_list)
                all_records.extend(result_list)
                # self.save_db(result_list)
        return all_records

    def run(self):
        dict_list=list()
        records = self.getAllRecords()
        for record in records:
            dict_list.append(self.getDict(record)) 
        
        with open("./dict.json", "w",encoding="gbk+") as f:
            json.dump(dict_list, f,ensure_ascii=False)
        f.close()

def main():
    result = DataProcess(r'D:\报告\data\data')
    result.run()


if __name__ == '__main__':
    main()
