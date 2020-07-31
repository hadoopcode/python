import pandas as pd
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import train_test_split

if __name__ == '__main__':
    keyword_matrix = pd.read_csv(
        r'/etl_data/data/keyword_matrix_target.csv')
    data = keyword_matrix.iloc[:, :keyword_matrix.shape[1] - 2]
    target = keyword_matrix[['PN', 'target.1']]

    X_train, X_test, y_train, y_test = train_test_split(data.values.tolist(), target.values.tolist(), test_size=0.2)

    X_train_df = pd.DataFrame(data=X_train)
    X_test_df = pd.DataFrame(data=X_test)
    y_train_df = pd.DataFrame(data=y_train)
    y_test_df = pd.DataFrame(data=y_test)

    # X_train_df.to_csv(r'F:\project\pycharm_workplace\dissertation_project\analysis\data\result\X_train.csv')
    # X_test_df.to_csv(r'F:\project\pycharm_workplace\dissertation_project\analysis\data\result\X_test.csv')
    # y_train_df.to_csv(r'F:\project\pycharm_workplace\dissertation_project\analysis\data\result\y_train.csv')
    # y_test_df.to_csv(r'F:\project\pycharm_workplace\dissertation_project\analysis\data\result\y_test.csv')

    knn = KNeighborsClassifier(n_neighbors=20, weights='distance')
    knn.fit(X_train_df.iloc[:, 2:].values.tolist(), y_train_df[1].tolist())
    y_predict = knn.predict(X_test_df.iloc[:, 2:].values.tolist())
    y_test_df['predict'] = y_predict
    y_test_df.to_csv(r'F:\project\pycharm_workplace\dissertation_project\analysis\data\result\predict.csv')
    print('over')
