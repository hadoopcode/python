from nltk_etl.extract_keywords import get_keywords, get_keywords_tfidf, get_keywords_summa
from sqoop import read_db as rdb
from nltk_etl import extarct_phrase as eh
from sqoop import to_db as td
import pandas as pd
import asyncio
import time

# async def task(df):
#     phrase = df.apply(lambda x: ['|'.join(eh.extract_phrase(x['TI'])), '|'.join(eh.extract_phrase(x['AB']))],
#                       axis=1,
#                       result_type='broadcast')
#     phrase.rename(columns={'TI': 'TI_phrase', 'AB': 'AB_phrase'}, inplace=True)
#     await asyncio.sleep(1)
#     return phrase
#
#
# def get_all_phrase():
#     start = time.time()
#     rdf = rdb.read_mysql('original_etl_tmp')
#     df = rdf[['TI', 'AB']]
#     # # df.drop(columns=['index'], inplace=True)
#     tasks = [task(df.iloc[i:i + 500]) for i in range(0, df.shape[0], 500)]
#     loop = asyncio.get_event_loop()
#     result = loop.run_until_complete(asyncio.gather(*tasks))
#     df_result = pd.concat(result)
#
#     concat = pd.concat([rdf, df_result], axis=1)
#
#     td.save_to_mysql(concat, 'original_etl_phrase')
#     end = time.time()
#     # print(end - start)


# 用RankText提取一次关键词
def run1():
    df = rdb.read_mysql('data_year5')
    keywords_summa = df[['AB_TI']].applymap(get_keywords_summa)
    concat_df_summa = pd.concat([df[['PN', 'PD_YEAR']], keywords_summa, df[['TC']]], axis=1)
    # concat_df_summa.to_csv(r'F:\project\pycharm_workplace\dissertation_project\etl_data\1.csv')
    td.save_to_mysql(concat_df_summa, 'data_year_phrase')


# 用tfidf提取一次关键词
def run2():
    df = rdb.read_mysql('data_year_phrase')
    df.drop(columns=['index'], inplace=True)
    ti_ab_phrase = get_keywords_tfidf(df['AB_TI'].values.tolist())
    # ti_ab_phrase.drop(columns=[''], inplace=True)
    concat = pd.concat([df[['PN', 'PD_YEAR']], ti_ab_phrase, df[['TC']]], axis=1)
    concat.to_csv(r'F:\project\pycharm_workplace\dissertation_project\etl_data\data\keyword_matrix.csv')


def replace_to_target():
    keyword_matrix = pd.read_csv(r'F:\project\pycharm_workplace\dissertation_project\etl_data\data\keyword_matrix.csv')
    target = keyword_matrix.iloc[:, keyword_matrix.shape[1] - 1]
    target_str = list()
    for i, v in target.items():
        if v >= 21:
            target_str.append('L1')
        elif 21 > v >= 9:
            target_str.append('L2')
        elif 9 > v >= 3:
            target_str.append('L3')
        elif 3 > v >= 0:
            target_str.append('L4')
        else:
            pass

    df_target = pd.DataFrame(data={'target': target_str})
    concat = pd.concat([keyword_matrix, df_target], axis=1)
    concat.to_csv(r'F:\project\pycharm_workplace\dissertation_project\etl_data\data\keyword_matrix_target.csv')
    print('over')


def get_all_keywords():
    run1()
    run2()
    replace_to_target()


if __name__ == '__main__':
    # get_all_phrase()
    get_all_keywords()
