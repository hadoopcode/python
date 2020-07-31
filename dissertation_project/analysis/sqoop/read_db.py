import pandas as pd
from sqlalchemy import create_engine


# engine = create_engine('mysql+pymysql://root:123456sql@localhost:3306/dwpi')


def read_mysql(table, db_info=None):
    if db_info is None:
        db_info = {'user': 'root', 'password': '123456sql', 'host': 'localhost', 'db': 'dwpi'}
    engine = create_engine('mysql+mysqlconnector://{user}:{password}@{host}/{db}?charset=utf8'.format(**db_info),
                           encoding='utf-8')
    return pd.read_sql_table(table_name=table, con=engine,)
