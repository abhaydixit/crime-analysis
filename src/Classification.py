__author__ = "Abhay Rajendra Dixit"
__author__ = "Adya Shrivastava"
__author__ = "Pranjal Pandey"

import pandas as pd
import numpy as np
import json
import sys
import time
import connection
from numpy import loadtxt
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import confusion_matrix
from sklearn.model_selection import cross_val_score, KFold
from sklearn.metrics import classification_report
import seaborn as sns
import matplotlib.pyplot as plt


from pymongo import MongoClient

pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', None)

def getDF(mongo_collection):
    city_data = list(mongo_collection.find({"city": "LACity"},
                                           {"offense": 1, "sex": 1, "weapon": 1,
                                            "age": 1, "shift": 1, "month": 1, "_id": 0}))
    df = pd.DataFrame(city_data)
    df.to_csv("La_clean.csv", index=False)
    print(df)
    return df
def analyse(la_df):
    print(la_df.columns)
    X_columns = ['shift', 'month', 'weapon', 'sex', 'age']
    target = 'offense'
    la_df_x = la_df[X_columns]
    la_df_y = la_df[target]

    X = la_df_x.values
    X = X.astype(str)
    Y = la_df_y.values
    encoded_x = None
    row = X.shape[1]

    label_encoder = LabelEncoder()
    label_encoded_y = label_encoder.fit_transform(Y)

    mapping = dict(zip(label_encoder.classes_, range(1, len(label_encoder.classes_) + 1)))
    print(mapping)

    for i in range(0, row):
        label_encoder = LabelEncoder()
        x_feature = label_encoder.fit_transform(X[:, i]).reshape(X.shape[0], 1)
        o_encoder = OneHotEncoder(sparse=False, categories='auto')
        feature = o_encoder.fit_transform(x_feature)
        if encoded_x is None:
            encoded_x = feature
        else:
            encoded_x = np.concatenate((encoded_x, feature), axis=1)

    X_train, X_test, y_train, y_test = train_test_split(encoded_x, label_encoded_y, test_size=0.25,
                                                        random_state=7)

    model = XGBClassifier()
    model.fit(X_train, y_train)
    print(model)

    y_pred = model.predict(X_test)
    predictions = [round(value) for value in y_pred]

    accuracy = accuracy_score(y_test, predictions)
    print("Accuracy: %.2f%%" % (accuracy * 100.0))

    labels = np.unique(y_test)

    cfn = cmtx = pd.DataFrame(
        confusion_matrix(y_test, y_pred, labels=labels),
        index=labels,
        columns=labels
    )
    #
    plt.figure(figsize=(10, 7))
    sns.heatmap(cfn, annot=True, cmap="YlGnBu")
    plt.show()

    scores = cross_val_score(model, X_train, y_train, cv=5)
    print("Mean cross-validation score: %.2f" % scores.mean())


    kfold = KFold(n_splits=10, shuffle=True)
    kf_cv_scores = cross_val_score(model, X_train, y_train, cv=kfold)
    print("K-fold CV average score: %.2f" % kf_cv_scores.mean())

    print(classification_report(y_test, y_pred))




def main():
    config_file_path = '../config'
    mongo_connection_params = connection.get_mongo_parameters(config_file_path + '/connection.json')
    print("Using Mongo Connection Params as: ", mongo_connection_params)
    mongo_database = connection.get_mongo_connection(mongo_connection_params)
    mongo_collection = mongo_database['city_crimes']
    la_df = getDF(mongo_collection)
    # la_df = pd.read_csv("La_clean.csv")
    # la_df = la_df[:200000]
    analyse(la_df)
    # perform_KNN(mongo_collection)



if __name__ == "__main__":
    main()


