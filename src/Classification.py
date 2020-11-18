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
    # dataset = la_df.values
    # split data into X and y

    X = la_df_x.values
    X = X.astype(str)
    Y = la_df_y.values
    encoded_x = None
    for i in range(0, X.shape[1]):
        label_encoder = LabelEncoder()
        feature = label_encoder.fit_transform(X[:, i])
        feature = feature.reshape(X.shape[0], 1)
        onehot_encoder = OneHotEncoder(sparse=False, categories='auto')
        feature = onehot_encoder.fit_transform(feature)
        if encoded_x is None:
            encoded_x = feature
        else:
            encoded_x = np.concatenate((encoded_x, feature), axis=1)
    print("X shape: : ", encoded_x.shape)

    # encode string class values as integers
    label_encoder = LabelEncoder()
    label_encoder = label_encoder.fit(Y)
    label_encoded_y = label_encoder.transform(Y)

    # split data into train and test sets
    seed = 7
    test_size = 0.33
    X_train, X_test, y_train, y_test = train_test_split(encoded_x, label_encoded_y, test_size=test_size,
                                                        random_state=seed)
    # fit model no training data
    model = XGBClassifier()
    model.fit(X_train, y_train)
    print(model)
    # make predictions for test data
    y_pred = model.predict(X_test)
    predictions = [round(value) for value in y_pred]
    # evaluate predictions
    accuracy = accuracy_score(y_test, predictions)
    print("Accuracy: %.2f%%" % (accuracy * 100.0))

def main():
    config_file_path = '../config'
    mongo_connection_params = connection.get_mongo_parameters(config_file_path + '/connection.json')
    print("Using Mongo Connection Params as: ", mongo_connection_params)
    mongo_database = connection.get_mongo_connection(mongo_connection_params)
    mongo_collection = mongo_database['city_crimes']
    la_df = getDF(mongo_collection)
    la_df = pd.read_csv("La_clean.csv")
    analyse(la_df)



if __name__ == "__main__":
    main()


