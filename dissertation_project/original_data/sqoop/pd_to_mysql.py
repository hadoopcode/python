import os
import pandas as pd
from sqlalchemy import create_engine


def get_file(path):
    return [filename for filename in os.listdir(path)]


engine = create_engine('mysql+mysqlconnector://root:123456sql@localhost:3306/dwpi')


def save_to_mysql(file):
    if file.split('.')[1] == 'xlsx':
        df = pd.read_excel(os.path.join(INPUT_DIR_PATH, file))
        df.drop(df.columns[0], axis=1, inplace=True)
        df.fillna("", inplace=True)
        df.drop('ER', axis=1, inplace=True)
        df['ER'] = ""

        df[['PN', 'TI', 'AB','PD','UT', 'TC']].to_sql(name='original', con=engine, if_exists='append', index=False)
        print(file + " write to Mysql table successfully!")


INPUT_DIR_PATH = r'F:\project\pycharm_workplace\dissertation_project\original_data\data\result_excel'


def run():
    global INPUT_DIR_PATH

    file_list = get_file(INPUT_DIR_PATH)
    for file in file_list:
        save_to_mysql(file)


if __name__ == '__main__':
    run()
