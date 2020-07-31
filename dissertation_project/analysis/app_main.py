from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import train_test_split
from sklearn.externals import joblib
import pandas as pd


def get_y_predict():
    keyword_matrix = pd.read_csv(
        r'F:\project\pycharm_workplace\dissertation_project\etl_data\data\keyword_matrix_target.csv')
    data = keyword_matrix.iloc[:, 2:keyword_matrix.shape[1] - 2]
    target = keyword_matrix.iloc[:, keyword_matrix.shape[1] - 1]
    knn = KNeighborsClassifier(weights='distance')
    X_train, X_test, y_train, y_test = train_test_split(data.values.tolist(), target, test_size=0.2)
    joblib.dump(knn, r"F:\project\pycharm_workplace\dissertation_project\analysis\model\test.pkl")
    y_predict = knn.predict(X_test)
    print("asdsad")


if __name__ == '__main__':
    get_y_predict()
