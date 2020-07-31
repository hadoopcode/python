# from xml.etree import ElementTree as ET
import pymongo
import re
import linecache
import pandas as pd
import os
import MysqlUtil

class DataProcess(object):
    def __init__(self, path):
        self.path = path
        self.client = pymongo.MongoClient()
        self.db = self.client['patent_str']
        self.file_name = None
        self.file_list = self.get_file(self.path)
        self.tags = set()
        self.mysqlclient = MysqlUtil.Mysql('patents2', '123456sql')

    #获取文件名
    def get_file(self, path):
        for file in os.walk(path):
            for f in file[2:]:
                return f[1:]

    #粗处理数据
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

    #存入数据库
    def save_db(self, result_list):
        for result in result_list:
            item = {'patent': result}
            self.db[self.file_name].insert_one(item)
    #把过滤的数据重新存入
    def save_db1(self, result_list, name):
        for result in result_list:
            item = {'patent': result}
            self.db[name].insert_one(item)

    #获取MongoDB中数据的数量
    def count(self):
        for file in self.file_list:
            if file.split('.')[1] == "txt":
                self.file_name = file.split('.')[0]
                count = self.db[self.file_name].count()
                print(count)

    #获取MongoDB对象集合
    def collection_list(self):
        collections = list()
        for file in self.file_list:
            if file.split('.')[1] == "txt":
                self.file_name = file.split('.')[0]
                collections.append(self.db[self.file_name])
        return collections

    def getData(self, collection, name):
        mongo_object_list = collection.find()
        datas = list()
        for o in mongo_object_list:
            datas.append(o['patent'])
        #返回patent集合
        return datas

    #数据提取
    def preciseProcessData(self, datas):
        tag_list = list()
        ex_data = list()
        #拿到每一条patent
        for data in datas:
            lines = data.split('\n')
            for line in lines:
                if line[0:2] in self.tags:
                    tag_list.append(line[0:2])
            sql = "insert into patent("
            sql1 = "values("
            for i in range(len(tag_list) - 1):
                result = re.findall(tag_list[i] + '(.*?)' + tag_list[i + 1],
                                    data, re.S)
                if i is not len(tag_list) - 2:
                    sql = sql + tag_list[i] + ','
                    sql1 = sql1 + '"' + result[0] + '"' + ","
                else:
                    sql = sql + tag_list[i] + ")"
                    sql1 = sql1 + '"' + result[0] + '"' + ")"
            sql2 = sql + ' ' + sql1
            flag = self.mysqlclient.executeNonQuery(sql2)

            if flag != int(1):
                ex_data.append(data)
            tag_list.clear()
        return ex_data

    #获取标签
    def getTag(self):
        for file in self.file_list:
            if file.split('.')[1] == "txt":
                self.file_name = file.split('.')[0]
                line_list = linecache.getlines('./' + file)
                for line in line_list:
                    if line[0:2].isalpha() and line[0:2].isupper():
                        self.tags.add(line[0:2])
        return self.tags

    def runRough(self):
        for file in self.file_list:
            if file.split('.')[1] == "txt":
                self.file_name = file.split('.')[0]
                line_list = linecache.getlines('./' + file)
                result_list = self.roughProcessData(line_list)
                self.save_db(result_list)

    #精确处理数据
    def runPrecise(self):
        collections = self.collection_list()
        for collection in collections:
            datas = self.getData(collection, 'patent')
            exdata = self.preciseProcessData(datas)
            self.save_db1(exdata, 'ex_data')
        self.client.close()

    

def main():
    result = DataProcess(r'D:\data\data')
    #result.runRough()
    result.runPrecise()


if __name__ == '__main__':
    main()
