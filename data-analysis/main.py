import pandas as pd
import numpy as np

# %%

data = pd.read_excel(r'F:\project\pycharm_workplace\data-analysis\data\Book1_family_self.xlsx')
ut_cp = data[['UT', 'cp']]
ut = data['UT']
cp = data['cp'].dropna().reset_index(drop=True)
cp = pd.DataFrame(data['cp'].dropna().reset_index(drop=True))
cp = pd.Series(cp['cp'].str.split(';', expand=True).stack().reset_index(drop=True).rename('cp').unique())

# %%
init_matrix = pd.DataFrame(data=np.zeros((cp.index.size, ut.index.size)), index=cp, columns=ut)
# %%
for column in init_matrix.columns:
    cp = ut_cp[ut_cp['UT'] == column]['cp'].values[0]
    if cp is not np.nan:
        cp_list = cp.split(r';')
        for c in cp_list:
            init_matrix[column][c] = init_matrix[column][c] + 1
    else:
        continue


# %%

def indirect(columns):
    column_list = list()
    for column in columns:
        if column in init_matrix.index.to_list():
            new_row = init_matrix.loc[column, :]
            new_columns = new_row[new_row != 0]
            _new_columns = new_columns
            for ncolumn in _new_columns:
                if ncolumn in columns:
                    new_columns.drop(index=ncolumn)
            column_list.extend(new_columns.index.to_list())
    result = len(set(column_list))
    if result == int(0):
        return 0
    else:
        return result + indirect(new_columns)


# %%
indirect_count = [indirect(row[row != 0].index) for index, row in init_matrix.iterrows()]
direct_count = init_matrix.sum(axis=1).to_list()
df = pd.DataFrame(
    data={'UT': init_matrix.index.to_list(), 'indirect_count': indirect_count, 'direct_count': direct_count})
df['target'] = df.apply(lambda x: 0.5 * x['indirect_count'] + 0.5 * x['direct_count'], axis=1)
# %%
df.to_csv(r'F:\project\pycharm_workplace\data-analysis\data\result.csv')

# %%
from pyspark import conf
from pyspark import context
from operator import add

config = conf.SparkConf().setAppName("wordcount")
spark_context = context.SparkContext(config)
context_text_file = spark_context.textFile(r"F:\Download\in")
by = context_text_file.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add).sortBy(lambda x: x[0],
                                                                                                     ascending=False)
by.foreach(print)
