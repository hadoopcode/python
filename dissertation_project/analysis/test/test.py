import pandas as pd


def func(x):
    duplicates = x.drop_duplicates()
    count = duplicates.count() - 1
    return count


if __name__ == '__main__':
    df = pd.read_csv(r'F:\project\pycharm_workplace\dissertation_project\etl_data\data\keyword_matrix.csv')
    df.drop(columns=['Unnamed: 0'], inplace=True)
    df.loc['count'] = df.apply(func)
    values = df.iloc[:, 2:df.shape[1] - 1].sort_values(by='count', ascending=False, axis=1)
    values.to_csv(r'F:\project\pycharm_workplace\dissertation_project\analysis\data\tmp.csv')
    print('over')
