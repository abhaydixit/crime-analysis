import pandas as pd
import time

from src.connection import get_mongo_connection
from src.connection import get_mongo_parameters

from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import fpgrowth

pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', None)


def read_data(mongo_collection):
    cursor = mongo_collection.aggregate([{
        '$project': {
            'city': 1,
            'offense': 1,
            'shift': 1,
            'month': 1,
            '_id': 0
        }
    }])

    records = []
    for item in cursor:
        re = [item['city'], item['offense'], item['shift'], str(item['month'])]
        records.append(re)
    del cursor
    transaction_encoder = TransactionEncoder()
    transaction_encoder_transformed = transaction_encoder.fit(records).transform(records)
    records_df = pd.DataFrame(transaction_encoder_transformed, columns=transaction_encoder.columns_)

    fp = fpgrowth(records_df, min_support=0.001, use_colnames=True)
    fp = fp.sort_values(['support'], ascending=[False])

    print("%s \t\t %50s" % ('Support', 'Frequent Itemsets'))
    for index, row in fp.iterrows():
        if len(row['itemsets']) > 1:
            # print(index, row)
            print("%0.6f \t\t %50s" % (row['support'], list(row['itemsets'])))


def comparing_la_city(mongo_collection):
    cursor = mongo_collection.aggregate([
        {
            '$match':
                {
                    'city': 'LACity'
                }
        },
        {
            '$project': {
                'offense': 1,
                'age': 1,
                'sex': 1,
                'month': 1,
                'shift': 1,
                '_id': 0
            }
        }
    ])

    records = []
    for item in cursor:
        re = [str(item['age']), item['sex'], item['offense'], str(item['month']), item['shift']]
        records.append(re)

    # for re in records:
    #     print(re)

    transaction_encoder = TransactionEncoder()
    transaction_encoder_transformed = transaction_encoder.fit(records).transform(records)
    records_df = pd.DataFrame(transaction_encoder_transformed, columns=transaction_encoder.columns_)

    fp = fpgrowth(records_df, min_support=0.045, use_colnames=True)
    fp = fp.sort_values(['support'], ascending=[False])

    print("%s \t\t %50s" % ('Support', 'Frequent Itemsets'))
    for index, row in fp.iterrows():
        if len(row['itemsets']) > 1:
            # print(index, row)
            print("%0.6f \t\t %50s" % (row['support'], list(row['itemsets'])))


def main():
    """
    Main function
    :return: None
    """
    start = time.time()
    config_file_path = '../config'

    mongo_connection_params = get_mongo_parameters(config_file_path + '/connection.json')
    print("Using Mongo Connection Params as: ", mongo_connection_params)
    mongo_database = get_mongo_connection(mongo_connection_params)
    mongo_collection = mongo_database['city_crimes']

    read_data(mongo_collection)

    # comparing_la_city(mongo_collection)


if __name__ == '__main__':
    main()
