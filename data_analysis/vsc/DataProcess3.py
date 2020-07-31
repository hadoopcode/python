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

    def getPI(self, data):
        lines = data.split('\n')
        for line in lines:
            if line[0:2] == 'PI':
                return line[-4:]  # 过滤前缀

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

    # PN去重
    def getPN_Set(self, data):
        pn = self.getPN(data)
        pn_list = pn.split(';')
        pn_list = [i.strip() for i in pn_list]
        pn_list = list(set(pn_list))
        # print(pn_list)
        return pn_list

    def getUT(self, data):
        lines = data.split('\n')
        for line in lines:
            if line[0:2] == 'UT':
                return line[2:].strip()

    def getCP(self, data):
        lines = data.split('\n')
        cp_index = int(0)
        next_index = int()
        # 获取CP编号的下标号
        for line in lines:
            if line[0:2] == 'CP':
                cp_index = lines.index(line)
        if cp_index != int(0):
            # 获取CP下一个编号的下标号
            for line in lines[cp_index + 1:]:
                # 用于判断下一个开头编号的，即不为空字符串，可能是CR，也有可能是UT，匹配到了就说明到了CP这一块数据的最后一条
                if line[0:2] != "  ":
                    next_index = lines.index(line)
                    break
            cp = lines[cp_index:next_index]

            # 去除来自于pn的,开头有三个空格的
            cp = [c for c in cp if c[:6] == '      ']
            # 去前后空字符
            cp = [i.strip() for i in cp]
            # 提取cp中长串中的编号
            cp = [list(filter(None, i.split(' ')))[0] for i in cp]
            # 去除被引用编号中有pn的编号
            pn_list = self.getPN_list(data)
            cp = [c for c in cp if c not in pn_list]
            # 去重
            cp = list(set(cp))
            # 替换
            for c in cp:
                item = self.find(c)
                if item is not None:
                    cp[cp.index(c)] = item
            # 去重
            cp = list(set(cp))
            return cp
        else:
            return None

    def find(self, c):
        items = self.getDict()
        for item in items:
            for key in item:
                pn_list = item[key]
                if c in pn_list:
                    return key
        return None

    # 读取同义词字典文件中的数据
    def getDict(self, path='./dict.json'):
        with open(path, 'r') as f:
            items = json.load(f)
        f.close()
        return items

    # 同义词字典
    def getUTDir(self, data):
        return {'UT': self.getUT(data), 'PN': self.getPN_list(data)}

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

    def run(self, excel_path='./data.xlsx', sheet_name='data'):
        # 获取所有条专利数据
        records = self.getAllRecords()
        result_list = list()
        for record in records:
            cp = self.getCP(record)
            if cp is not None:
                # 对每条记录提取
                ut = self.getUT(record)
                pn = self.getPN(record)
                pi = self.getPI(record)
                item = {
                    'UT': ut,
                    'PN': pn,
                    'time': pi,
                    'cp': ';'.join(cp)
                }
                result_list.append(item)
        df = pd.DataFrame(result_list)
        # 存入Excel
        df.to_excel(excel_path, sheet_name=sheet_name)


def main():
    result = DataProcess(r'D:\报告\data\data')
    result.run()


if __name__ == '__main__':
    main()
