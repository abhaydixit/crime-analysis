import time
import matplotlib.pyplot as plt
from scipy import stats
from collections import Counter

from src.connection import get_mongo_connection
from src.connection import get_mongo_parameters


def age_and_number_of_crime(mongo_collection):
    cursor = mongo_collection.aggregate([{
        '$match': {'city': 'LACity'}
        },
        {
            '$project': {
                'age': 1,
                '_id': 0
            }
        },
        {
            '$group': {
                '_id': "$age",
                'count': {"$sum": 1}
            }
        }
    ])

    age = []
    count = []
    for item in cursor:
        if int(item['_id']) != 31:
            age.append(item['_id'])
            count.append(item['count'])

    correlation_age_count, p_value = stats.pearsonr(age, count)
    print(correlation_age_count)

    plt.figure()
    plt.scatter(age, count, color='red', s=3)
    plt.xlabel("Age")
    plt.ylabel("Number of Crimes")
    plt.title("Age v/s Number of Crimes")
    # plt.text('Pearson\'s Coefficient = ' + str(-0.48351713746224395), loc='upper left' )


def time_and_number_of_crimes(mongo_collection):
    cursor = mongo_collection.aggregate([
        {
            '$project': {
                'time': 1,
                '_id': 0
            }
        }
    ])

    temo = []

    for item in cursor:
        t, _, _ = map(int, item['time'].split(':'))
        temo.append(t)

    c = Counter(temo)
    times = list(c.keys())
    counts = list(c.values())

    correlation_age_count, p_value = stats.pearsonr(times, counts)
    print(correlation_age_count)

    plt.figure()
    plt.scatter(times, counts, color='darkgreen', s=3)
    plt.xlabel("Hour of Day")
    plt.ylabel("Number of Crimes")
    plt.title("Hour of Day v/s Number of Crimes")
    # plt.text('Pearson\'s Coefficient = ' + str(0.7050896695888618), loc='upper left')


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

    age_and_number_of_crime(mongo_collection)

    time_and_number_of_crimes(mongo_collection)
    plt.show()

if __name__ == '__main__':
    main()
