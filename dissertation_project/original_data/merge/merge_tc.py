import os

import pandas as pd


class MergeTc(object):

    def __init__(self, path):
        # 文件夹绝对路径
        self.path = path
        # 获取文件名
        self.file_list = self.get_file(self.path)
        self.records_count = 0

    # 获取文件名
    def get_file(self, path):
        return [filename for filename in os.listdir(path)]

    def get_tc(self, path):
        return pd.read_csv(path)

    def merge(self, tc_path):
        tc = self.get_tc(tc_path)
        tc.drop(tc.columns[0], axis=1, inplace=True)
        tc.rename(columns={'tc': 'TC', 'ga': 'GA'}, inplace=True)
        for file in self.file_list:
            full_path = os.path.join(self.path, file)
            excel = pd.read_excel(full_path)
            excel.drop(excel.columns[0], axis=1, inplace=True)
            excel.fillna("", inplace=True)
            merge = pd.merge(excel, tc, how='left')
            merge["TC"].fillna('0', inplace=True)
            output_excel = 'data/result_excel/{}.csv'.format(file.split(r'.')[0])
            write = pd.ExcelWriter(os.path.join(r'data/result_excel', file))
            merge.to_excel(write, sheet_name=file.split('.')[0])
            write.save()
            print("{}输出完成".format(file))


def run():
    print("开始合并tc")
    merge_tc = MergeTc("data/middle_excel")
    merge_tc.merge("data/out_tc/output.csv")
    print("结束合并tc")


if __name__ == '__main__':
    run()
