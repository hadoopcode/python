from sqoop import read_db
import pandas as pd
import numpy as np
from flashtext import KeywordProcessor


def count(x):
    word_df = pd.read_csv('data/word.csv')
    words = word_df['keyword'].values.tolist()
    df = pd.DataFrame(data=np.zeros((1, len(words))), columns=words)
    for index, value in x[['keywords']].iterrows():
        keyword_processor = KeywordProcessor()
        keyword_processor.add_keywords_from_list(words)
        found = list(set(keyword_processor.extract_keywords(value['keywords'])))
        df[found] = df[found] + 1
    return df


def run1():
    df = pd.read_csv('data/keyword1.csv')
    df.drop(columns=['Unnamed: 0'], inplace=True)
    groupby = df[['PN', 'PD_YEAR', 'keywords']].groupby(by='PD_YEAR').apply(count)
    df1 = groupby[['watch', 'watches']].apply(sum, axis=1).to_frame(name='watch/watches')
    df2 = groupby[['clothing', 'cloth', 'clothes']].apply(sum, axis=1).to_frame(name='clothing/cloth/clothes')
    df3 = groupby[['textile', 'textiles']].apply(sum, axis=1).to_frame(name='textile/textiles')
    df4 = pd.concat([groupby, df1, df2, df3], axis=1)
    df4.drop(columns=['watch', 'watches', 'clothing', 'cloth', 'clothes', 'textile', 'textiles'], inplace=True)
    df4['All categories'] = df4.apply(sum, axis=1)
    # df4.insert(0, 'year', [i[0] for i in df4.index.values.tolist()])
    # df4['year'] = [i[0] for i in df4.index.values.tolist()]

    df4.T.to_csv('data/c_result.csv')
    print('finally')


def run2():
    df = read_db.read_mysql("original_etl_con")
    df.drop(columns=['Unnamed: 0'], inplace=True)
    groupby = df[['PN', 'PD_YEAR', 'TC']].groupby(by='PD_YEAR').apply(count)
    df1 = groupby[['watch', 'watches']].apply(sum, axis=1).to_frame(name='watch/watches')
    df2 = groupby[['clothing', 'cloth', 'clothes']].apply(sum, axis=1).to_frame(name='clothing/cloth/clothes')
    df3 = groupby[['textile', 'textiles']].apply(sum, axis=1).to_frame(name='textile/textiles')
    df4 = pd.concat([groupby, df1, df2, df3], axis=1)
    df4.drop(columns=['watch', 'watches', 'clothing', 'cloth', 'clothes', 'textile', 'textiles'], inplace=True)
    df4['All categories'] = df4.apply(sum, axis=1)
    df4.to_csv('data/y_result.csv')
    print('finally')


def count2(x):
    pn_count = x['PN'].count()
    tc_sum = x['TC'].sum()
    avg = round(tc_sum / pn_count, 2)
    tc_max = x['TC'].max()
    tc_min = x['TC'].min()
    std = round(x['TC'].std(ddof=0), 2)
    avg_plus_3std = avg + 3 * std
    df = pd.DataFrame(
        data={'Number of patents': [pn_count], 'Total forward citations': [tc_sum], 'Average forward citations': avg,
              'Minimum': tc_min, 'Maximum': tc_max, 'Standard deviation': std,
              'Average plus three standard deviations': avg_plus_3std})
    return df


def run3():
    df = read_db.read_mysql("original_etl_keyword2")
    groupby = df[['PN', 'PD_YEAR', 'TC']].groupby(by='PD_YEAR').apply(count2)

    groupby.to_csv('data/y_result.csv')
    print('finally')


def run4():
    df = read_db.read_mysql("original_etl_keyword2")
    df[['PD_YEAR', 'TI', 'AB']].to_csv('data/ti_ab.csv')
    print('finally')


if __name__ == '__main__':
    run4()
