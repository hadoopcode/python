import os
from bs4 import BeautifulSoup
import pandas as pd
import re


class ExtractTc(object):

    def __init__(self, path):
        # 文件夹绝对路径
        self.path = path
        # 获取文件名
        self.file_list = self.get_file(self.path)
        self.records_count = 0

    # 获取文件名
    def get_file(self, path):
        return [filename for filename in os.listdir(path) if re.match(r'.*html$', filename)]

    def parse_html(self, html_doc):
        soup = BeautifulSoup(html_doc, "html.parser")
        records = soup.select("div[id^='RECORD_']")
        tc_ga = list()
        for record in records:
            tc = record.select_one("div.DIIpan value").text
            ga = record.select_one("span.data_bold value").text
            tc_ga.append({'tc': tc, 'ga': ''.join(ga.split('-'))})
        print("抽取{}条".format(len(tc_ga)))
        return tc_ga

    # 获取所有记录
    def get_all_tc_ga(self):
        tc_ga_list = list()
        for file in self.file_list:
            full_path = os.path.join(self.path, file)
            with open(full_path, encoding="utf8") as f:
                tc_ga = self.parse_html(f.read())
                tc_ga_list.extend(tc_ga)
                print("文件{}读取完毕".format(file))
        f.close()
        return pd.DataFrame(data=tc_ga_list)


def run():
    print("开始提取tc")
    load = ExtractTc("data/input_html")
    df = load.get_all_tc_ga()
    print("总计:{}条".format(df.shape[0]))
    df.to_csv("data/out_tc/output.csv")
    print("提取结束")


if __name__ == '__main__':
    run()
