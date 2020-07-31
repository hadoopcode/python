import pymongo
import re
import linecache
import pandas as pd
import os


class DataProcess(object):
    def __init__(self, path):
        self.path = path
        self.client = pymongo.MongoClient()
        self.db = self.client['data1']
        self.file_name = None

    # 获取文件名
    def get_file(self, path):
        for file in os.walk(path):
            for f in file[2:]:
                return f[1:]

    # 提取数据
    def processData(self, line_list):

        title_list = list()
        for line in line_list:
            if re.match('^TI ', line) is not None:
                title_list.append(re.findall('TI (.*)', line)[0])
        abstrcat_list = list()
        for line in line_list:
            if re.match('^AB', line) is not None:
                # if re.match('^AB    NOVELTY - ', line) is not None:
                abstrcat_list.append(re.findall('^AB(.*)', line)[0])
                # continue
                # abstrcat_list.append(re.findall('^AB(.*)', line)[0])
        item = {'title': title_list, 'abstrcat': abstrcat_list}
        # return pd.DataFrame(data=item)

    # 存入数据库
    def save(self, df):
        for indexs in df.index:
            item = {
                'title': df.loc[indexs]['title'],
                'abstrcat': df.loc[indexs]['abstrcat']
            }
            self.db[self.file_name].insert_one(item)

    def run(self):
        # 获取文件名列表
        file_list = self.get_file(self.path)
        for file in file_list:
            # self.file_name = file.split('.')[0]
            line_list = linecache.getlines('./' + file)
            df = self.processData(line_list)
            # self.save(df)


def main():
    result = DataProcess(r'D:\data\data')
    result.run()


if __name__ == '__main__':
    main()
