import asyncio
import os
import time
import sys
import pandas as pd


def get_file(path):
    return [filename for filename in os.listdir(path)]


async def save_to_txt(file):
    if file.split('.')[1] == 'xlsx':
        print(file)
        excel = pd.read_excel(os.path.join(INPUT_DIR_PATH, file))
        excel.drop(excel.columns[0], axis=1, inplace=True)
        excel.fillna("", inplace=True)
        excel.drop('ER', axis=1, inplace=True)
        excel['ER'] = ""
        keys = list(excel.keys())

        for k in keys:
            excel[k] = excel[k].apply(lambda x: str(x))
            excel[k] = excel[k].apply(lambda x: k + "  " + x if x is not "" else x)
        excel['ER'] = excel['ER'].apply(lambda x: x + "ER\n\n\n")
        with open(os.path.join(OUT_DIR_PATH, file.replace('xlsx', 'txt')), 'w+', encoding='utf-8') as f:
            for index, row_value in excel.iterrows():
                pt_er_list = list(filter(None, list(row_value)))
                pr_er = "\n".join(pt_er_list)
                f.write(pr_er)
        f.close()


OUT_DIR_PATH = 'data/result_txt'
INPUT_DIR_PATH = 'data/result_excel'


def run():
    global INPUT_DIR_PATH
    global OUT_DIR_PATH
    try:
        print("开始转为txt")
        if len(sys.argv) == int(3):
            INPUT_DIR_PATH = sys.argv[1]
            OUT_DIR_PATH = sys.argv[2]
        start = time.time()
        file_list = get_file(INPUT_DIR_PATH)
        tasks = [save_to_txt(file) for file in file_list]
        # 收集携程任务
        loop = asyncio.get_event_loop()
        # 运行多个协程
        loop.run_until_complete(asyncio.gather(*tasks))
        end = time.time()
        print(end - start)
        print("转化结束")
    except Exception as e:
        print(e)


if __name__ == '__main__':
    run()
