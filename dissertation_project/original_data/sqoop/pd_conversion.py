import linecache
import pandas as pd
import os
import re
import asyncio
import time
import sys


def extract_field(lines, datafield):
    flag = False
    index = int()
    index1 = int()
    for i in range(len(lines)):
        if lines[i][0:2] == datafield:
            index = i
            flag = True
            continue
        if flag and (lines[i] is not '') and (not lines[i][0].isspace()):
            index1 = i
            flag = False
    lines[index] = lines[index][2:]
    return "".join(lines[index:index1]).strip()


def get_file(path):
    return [filename for filename in os.listdir(path)]


def extract(data):
    lines = data.split('\n')
    fields = [line[0:2] for line in lines if line[0:2].isalpha()]
    flag = False
    index = int()
    index1 = int()
    result = list()
    item = {}
    for field in fields:
        item[field] = extract_field(lines, field)
    return item


def pt_to_er(line_list):
    pt_list = list()
    er_list = list()
    for i in range(len(line_list)):
        if re.match("^PT", line_list[i]):
            pt_list.append(i)
        if re.match("^ER", line_list[i]):
            er_list.append(i)
    df = pd.DataFrame(data=[pt_list, er_list]).T
    for row in df.iterrows():
        yield "".join(line_list[row[1][0]:row[1][1] + int(1)])


async def save_to_excel(file):
    if file.split('.')[1] == 'txt':
        write = pd.ExcelWriter(os.path.join(OUT_DIR_PATH, file).replace('txt', 'xlsx'))
        full_path = os.path.join(INPUT_DIR_PATH, file)
        line_list = linecache.getlines(full_path)
        pt_er_list = list()
        for pt_er in pt_to_er(line_list):
            contents = extract(pt_er)
            pt_er_list.append(contents)
        df = pd.DataFrame(pt_er_list)
        er_col = df.pop('ER')
        df.insert(len(df.keys()), 'ER', er_col)
        df.to_excel(write, sheet_name=file.split('.')[0])
        write.save()


INPUT_DIR_PATH = 'data/intput_txt'
OUT_DIR_PATH = 'data/middle_excel'


def run():
    global INPUT_DIR_PATH
    global OUT_DIR_PATH
    try:
        print("开始转换为excel")
        if len(sys.argv) == int(3):
            INPUT_DIR_PATH = sys.argv[1]
            OUT_DIR_PATH = sys.argv[2]
        start = time.time()
        file_list = get_file(INPUT_DIR_PATH)
        tasks = [save_to_excel(file) for file in file_list]
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
