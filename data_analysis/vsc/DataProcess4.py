import re
import linecache
import pandas as pd
import os
import numpy as np
from collections import Counter


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
            result.append("".join(line_list[row[1][0]: row[1][1] + int(1)]))
        return result

    # 获取所有记录
    def getAllRecords(self):
        all_records = list()
        for file in self.file_list:
            if file.split(".")[1] == "txt":
                self.file_name = file.split(".")[0]
                # 获取每一行数据
                line_list = linecache.getlines("./" + file)
                # 从所有行中找PT-ER记录块，并返回为每个文件中的专利记录
                result_list = self.roughProcessData(line_list)
                all_records.extend(result_list)
        return all_records

    # 获取所有国家
    def getAllCountry(self):
        country_list = list()
        records = self.getAllRecords()
        for record in records:
            country_list.extend(self.getCountry(record))
        return set(country_list)

    # 获取单条记录中的国家
    def getCountry(self, data):
        country_list = list()
        pn_list = self.getPN_list(data)
        country_list.extend([pn[:2] for pn in pn_list])
        return country_list

    # pn用；分隔的字符串
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

    def getCP(self, data):
        lines = data.split("\n")
        cp_index = int(0)
        next_index = int()
        # 获取CP编号的下标号
        for line in lines:
            if line[0:2] == "CP":
                cp_index = lines.index(line)
        if cp_index != int(0):
            # 获取CP下一个编号的下标号
            for line in lines[cp_index + 1:]:
                if line[0:2] != "  ":
                    next_index = lines.index(line)
                    break
            cp = lines[cp_index:next_index]
            cp[0] = cp[0][2:]
            pn_index = [cp.index(c) for c in cp if c[:6] != '      ']
            item = dict()
            for pn in range(len(pn_index)):
                if pn + 1 < len(pn_index):
                    cp1 = cp[pn_index[pn] + 1:pn_index[pn + 1]]
                    cp1 = [i.strip() for i in cp1]
                    cp1 = [list(filter(None, i.split(' ')))[0] for i in cp1]
                    item[cp[pn_index[pn]].strip()] = cp1
                else:
                    cp1 = cp[pn_index[pn] + 1:]
                    cp1 = [i.strip() for i in cp1]
                    cp1 = [list(filter(None, i.split(' ')))[0] for i in cp1]
                    item[cp[pn_index[pn]].strip()] = cp1
            return item
            '''
            返回的数据类型{来自pn的编号1：[被引用编号1,被引用编号2,....]，……}
            '''
        else:
            return None

    def getUT(self, data):
        lines = data.split('\n')
        for line in lines:
            if line[0:2] == 'UT':
                return line[2:].strip()

    # 单条记录的统计结果
    def getCountResult(self, data, country_set=set()):
        # 以每个国家为键,值为两个元素的列表；列表第一个元素是pn中该国家的个数，第二个元素是cp中有引用的个数
        items = {country: list([int(0), int(0)]) for country in country_set}
        cp = self.getCP(data)
        pn_list = self.getPN_list(data)
        if cp is not None:
            for pn in pn_list:
                if pn[:2] in country_set:
                    items[pn[:2]][0] += int(1)
            for key in cp:
                if key[:2] in country_set:
                    items[key[:2]][1] += int(1)
            return {self.getUT(data): items}
            # 返回的数据类型{UT：{国家1：[PN中国家1个数，国家1的CP中的个数]，国家2：[]........}}
        else:
            return None

    # 把pn,cn统计后都为0的国家过滤掉,即单条记录中没出现过的国家过滤掉
    def getCountFilterZero(self, data, country_set=set()):
        count_item = self.getCount(data, country_set)
        if count_item is not None:
            for ut_key in count_item:
                country_items = count_item[ut_key]
                df = pd.DataFrame(country_items)
                df = df.loc[:, df.apply(sum) != 0]
            return {ut_key: df.to_dict('list')}
        else:
            None

    # 最后处理好的数据
    def getItems(self, country_set=list()):
        records = self.getAllRecords()
        items = {country: list() for country in country_set}
        # 构造全为0的二维数组
        inital_arr = np.zeros((len(country_set), 2), dtype=np.int64)
        for record in records:
            cp = self.getCP(record)
            if cp is not None:
                count = self.getCountResult(record, country_set)
                # 对所有数据统计
                for key in count:
                    df = pd.DataFrame(data=count[key], columns=country_set).T
                    inital_arr += np.array(df)  #矩阵累加
                # pn与被引用的编号关系
                for cp_key in cp:
                    if cp_key[:2] in country_set:
                        cp_str = ';'.join(cp[cp_key])
                        item = {
                            "PN": cp_key, "QuotedNumber": cp_str if cp_str is not '' else None}
                    items[cp_key[:2]].append(item)
                df_count = pd.DataFrame(inital_arr, index=country_set,columns=['from_PN','from_C'])
        return df_count, items

    def run(self, excel_path="./data.xlsx"):
        country_set = self.getAllCountry()
        # items = self.getItems(country_set={'CN'})
        df_count, items = self.getItems(country_set=country_set)
        write = pd.ExcelWriter(excel_path)
        # pd.set_option('display.max_rows', None)
        # pd.set_option('display.max_colwidth', 500)

        for key in items:
            # 过滤统计出都为0的国家
            if items[key]:
                df = pd.DataFrame(items[key])
                df.to_excel(write, sheet_name=key)
        df_count.to_excel(write, sheet_name="统计结果")


def main():
    result = DataProcess(r"D:\报告\data\data")
    result.run()


if __name__ == "__main__":
    main()
