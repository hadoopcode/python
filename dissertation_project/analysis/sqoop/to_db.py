from sqlalchemy import create_engine
import pandas as pd


def save_to_mysql(data, table_name, db_info=None):
    if db_info is None:
        db_info = {'user': 'root', 'password': '123456sql', 'host': 'localhost', 'db': 'dwpi'}
    engine = create_engine('mysql+mysqlconnector://{user}:{password}@{host}/{db}?charset=utf8'.format(**db_info),
                           encoding='utf-8')
    df = pd.DataFrame(data=data)
    df.to_sql(name=table_name, con=engine, if_exists='append', index=True)
    print('save successfully')
