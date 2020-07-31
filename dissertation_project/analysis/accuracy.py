from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import train_test_split
import pandas as pd
from sklearn.metrics import accuracy_score
from sklearn.metrics import classification_report
import json

def plot_values(n, X_train, X_test, y_train, y_test):
    knn = KNeighborsClassifier(n_neighbors=n, weights='distance')
    knn.fit(X_train, y_train)
    y_predict = knn.predict(X_test)
    # knn.score(X_test, y_test)

    report = classification_report(y_test, y_predict, digits=4, output_dict=True)

    return report


if __name__ == '__main__':
    keyword_matrix = pd.read_csv(
        r'F:\project\pycharm_workplace\dissertation_project\etl_data\data\keyword_matrix_target.csv')
    data = keyword_matrix.iloc[:, 2:keyword_matrix.shape[1] - 2]
    target = keyword_matrix.iloc[:, keyword_matrix.shape[1] - 1]
    X_train, X_test, y_train, y_test = train_test_split(data.values.tolist(), target.values.tolist(), test_size=0.2)
    report_list = list()
    for i in range(25, 51):
        report_list.append(plot_values(i, X_train, X_test, y_train, y_test))
        print(i)
    try:
        with open(r'data\report25_50.json', 'w+', encoding='utf-8') as f:
            json.dump({'report': report_list}, f)
    except IOError as ie:
        print(ie)

    # with open(r'data\report.json', 'r+', encoding='utf-8') as f:
    #     load_dict = json.load(f)
    # accuracy_list = list()
    # for r in load_dict['report']:
    #     accuracy_list.append(r['accuracy'])
    # df = pd.DataFrame(data={'accuracy': accuracy_list})
    # df.to_csv(r'data\accuracy.csv')
    # print(df)
