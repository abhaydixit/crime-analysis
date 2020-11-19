import sys
from collections import Counter

import matplotlib.pyplot as plt
from scipy import stats

from src import connection as conn
import pandas as pd


MONTH_NUMBER = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep",
                       10: "Oct", 11: "Nov", 12: "Dec"}
YEAR = {2011: '11', 2012: '12', 2013: '13', 2014: '14', 2015: '15', 2016: '16', 2017: '17', 2018: '18', 2019: '19', 2020: '20'}


def pie_chart(mongo_coln):
    crime_series = list(mongo_coln.aggregate([
        {'$match': {'year': {
            '$gt': 2012,
            '$lte': 2019
        }}},
        {'$project':
            {
                'city': 1,
                'offense': 1,
                "_id": 0,

            }
        },
        {
            '$group': {
                '_id': {
                    'offense': '$offense',
                    'city': '$city'
                },
                "count": {"$sum": 1}
            }
        }
    ]))

    austin_dict = {'city': 'Austin', "offense": [], "crime count": []}
    baltimore_dict = {'city': 'Baltimore', "offense": [], "crime count": []}
    chicago_dict = {'city': 'Chicago', "offense": [], "crime count": []}
    lacity_dict = {'city': 'LACity', "offense": [], "crime count": []}
    roch_dict = {'city': 'Rochester', "offense": [], "crime count": []}

    for item in crime_series:
        if item['_id']['city'] == "Austin":
            austin_dict['offense'].append(item['_id']['offense'])
            austin_dict['crime count'].append(item['count'])
        elif item['_id']['city'] == "Baltimore":
            baltimore_dict['offense'].append(item['_id']['offense'])
            baltimore_dict['crime count'].append(item['count'])
        elif item['_id']['city'] == "Chicago":
            chicago_dict['offense'].append(item['_id']['offense'])
            chicago_dict['crime count'].append(item['count'])
        elif item['_id']['city'] == "LACity":
            lacity_dict['offense'].append(item['_id']['offense'])
            lacity_dict['crime count'].append(item['count'])
        elif item['_id']['city'] == "Rochester":
            roch_dict['offense'].append(item['_id']['offense'])
            roch_dict['crime count'].append(item['count'])

    cities = [austin_dict, baltimore_dict, chicago_dict, lacity_dict, roch_dict]
    for city in cities:
        labels = city['offense']
        sizes = city['crime count']
        plt.figure()
        plt.title(city['city'])
        plt.pie(sizes, labels=labels, startangle=90)
        plt.legend(loc='best', labels=['%s, %1.1f %%' % (l, (s/sum(sizes))*100) for l, s in zip(labels, sizes)])
        plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    # plt.show()



def time_and_number_of_crimes(mongo_coln):
    cursor = mongo_coln.aggregate([
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
    print("Pearson's correlation coefficient for hour of day and number of crimes", correlation_age_count)

    plt.figure()
    plt.scatter(times, counts, color='darkgreen', s=3)
    plt.xlabel("Hour of Day")
    plt.ylabel("Number of Crimes")
    plt.title("Hour of Day v/s Number of Crimes")


def age_and_number_of_crime(mongo_coln):
    cursor = mongo_coln.aggregate([{
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
    print("Pearson's correlation coefficient for age and number of crimes", correlation_age_count)

    plt.figure()
    plt.scatter(age, count, color='red', s=3)
    plt.xlabel("Age")
    plt.ylabel("Number of Crimes")
    plt.title("Age v/s Number of Crimes")



def month_series(mongo_coln):
    crime_series = list(mongo_coln.aggregate([
        {'$match': {'year': {
            '$gt': 2012,
            '$lte': 2019
        }}},
        {'$project':
            {
                'city': 1,
                'month': 1,
                "_id": 0,

            }
        },
        {
            '$group': {
                '_id': {
                    'month': '$month',
                    'city': '$city'
                },
                "count": {"$sum": 1}
            }
        },
        {'$sort': {"_id.month": 1}},
    ]))

    austin_dict = {'city': 'Austin', "month": [], "crime count": []}
    baltimore_dict = {'city': 'Baltimore', "month": [], "crime count": []}
    chicago_dict = {'city': 'Chicago', "month": [], "crime count": []}
    lacity_dict = {'city': 'LACity', "month": [], "crime count": []}
    roch_dict = {'city': 'Rochester', "month": [], "crime count": []}

    for item in crime_series:
        if item['_id']['city'] == "Austin":
            austin_dict['month'].append(MONTH_NUMBER[item['_id']['month']])
            austin_dict['crime count'].append(item['count'])
        elif item['_id']['city'] == "Baltimore":
            baltimore_dict['month'].append(MONTH_NUMBER[item['_id']['month']])
            baltimore_dict['crime count'].append(item['count'])
        elif item['_id']['city'] == "Chicago":
            chicago_dict['month'].append(MONTH_NUMBER[item['_id']['month']])
            chicago_dict['crime count'].append(item['count'])
        elif item['_id']['city'] == "LACity":
            lacity_dict['month'].append(MONTH_NUMBER[item['_id']['month']])
            lacity_dict['crime count'].append(item['count'])
        elif item['_id']['city'] == "Rochester":
            roch_dict['month'].append(MONTH_NUMBER[item['_id']['month']])
            roch_dict['crime count'].append(item['count'])


    plt.figure()
    plt.plot(austin_dict['month'], austin_dict['crime count'], label=austin_dict['city'])
    plt.plot(baltimore_dict['month'], baltimore_dict['crime count'], label=baltimore_dict['city'])
    plt.plot(chicago_dict['month'], chicago_dict['crime count'], label=chicago_dict['city'])
    plt.plot(lacity_dict['month'], lacity_dict['crime count'], label=lacity_dict['city'])
    plt.plot(roch_dict['month'], roch_dict['crime count'], label=roch_dict['city'])

    plt.xlabel('Month')
    plt.ylabel('Number of crimes')
    plt.title('Number of crimes committed in different cities in different months')
    plt.legend()
    plt.grid(True)
    # plt.show()


def month_year_time_series(mongo_coln):

    """With Speed"""
    crime_series = list(mongo_coln.aggregate([
        {'$project':
            {
                'city': 1,
                'month': 1,
                "year": 1,
                "_id": 0,

            }
        },
        {
            '$group': {
                '_id': {
                    'month': '$month',
                    'year': '$year',
                    'city': '$city'
                },
                "count": {"$sum": 1}
            }
        },
        {'$sort': {'_id.year': 1, "_id.month": 1}},
    ]))

    crime_series = list(filter(lambda x: 2019 >= x['_id']['year'] > 2012, crime_series))

    austin_dict = {'city': 'Austin', "date": [], "crime count": []}
    baltimore_dict = {'city': 'Baltimore', "date": [], "crime count": []}
    chicago_dict = {'city': 'Chicago', "date": [], "crime count": []}
    lacity_dict = {'city': 'LACity', "date": [], "crime count": []}
    roch_dict = {'city': 'Rochester', "date": [], "crime count": []}

    for item in crime_series:
        if item['_id']['city'] == "Austin":
            austin_dict['date'].append(MONTH_NUMBER[item['_id']['month']] + " " + YEAR[item['_id']['year']])
            austin_dict['crime count'].append(item['count'])
        elif item['_id']['city'] == "Baltimore":
            baltimore_dict['date'].append(MONTH_NUMBER[item['_id']['month']] + " " + YEAR[item['_id']['year']])
            baltimore_dict['crime count'].append(item['count'])
        elif item['_id']['city'] == "Chicago":
            chicago_dict['date'].append(MONTH_NUMBER[item['_id']['month']] + " " + YEAR[item['_id']['year']])
            chicago_dict['crime count'].append(item['count'])
        elif item['_id']['city'] == "LACity":
            lacity_dict['date'].append(MONTH_NUMBER[item['_id']['month']] + " " + YEAR[item['_id']['year']])
            lacity_dict['crime count'].append(item['count'])
        elif item['_id']['city'] == "Rochester":
            roch_dict['date'].append(MONTH_NUMBER[item['_id']['month']] + " " + YEAR[item['_id']['year']])
            roch_dict['crime count'].append(item['count'])


    plt.figure()
    plt.plot(austin_dict['date'], austin_dict['crime count'], label=austin_dict['city'])
    plt.plot(baltimore_dict['date'], baltimore_dict['crime count'], label=baltimore_dict['city'])
    plt.plot(chicago_dict['date'], chicago_dict['crime count'], label=chicago_dict['city'])
    plt.plot(lacity_dict['date'], lacity_dict['crime count'], label=lacity_dict['city'])
    plt.plot(roch_dict['date'], roch_dict['crime count'], label=roch_dict['city'])

    plt.xlabel('Month-Year')
    plt.ylabel('Number of crimes')
    plt.xticks(rotation=90)
    plt.title('Time series of number of crimes committed in different cities')
    plt.legend()
    plt.grid(True)



    plt.figure()

    austin_df = pd.DataFrame(austin_dict)
    austin_df['sc_austin'] = austin_df.iloc[:, 2].rolling(window=4).mean()

    baltimore_df = pd.DataFrame(baltimore_dict)
    baltimore_df['sc_baltimore'] = baltimore_df.iloc[:, 2].rolling(window=4).mean()

    chicago_df = pd.DataFrame(chicago_dict)
    chicago_df['sc_chicago'] = chicago_df.iloc[:, 2].rolling(window=9).mean()

    lacity_df = pd.DataFrame(lacity_dict)
    lacity_df['sc_lacity'] = lacity_df.iloc[:, 2].rolling(window=4).mean()

    roch_df = pd.DataFrame(roch_dict)
    roch_df['sc_roch'] = roch_df.iloc[:, 2].rolling(window=4).mean()

    plt.plot(austin_df['date'], austin_df['sc_austin'], label="Austin")
    plt.plot(baltimore_df['date'], baltimore_df['sc_baltimore'], label="Baltimore")
    plt.plot(chicago_df['date'], chicago_df['sc_chicago'], label="Chicago")
    plt.plot(lacity_df['date'], lacity_df['sc_lacity'], label="LACity")
    plt.plot(roch_df['date'], roch_df['sc_roch'], label="Rochester")

    plt.xlabel('Month-Year')
    plt.ylabel('Number of crimes')
    plt.xticks(rotation=90)
    plt.title('Time series of number of crimes committed in different cities')
    plt.legend()
    plt.grid(True)
    # plt.show()


def main():
    if __name__ == '__main__':
        """
            Main function:
            Print the output,call the functions, prints
            the overall time taken.
        """
        config_file_path = '../config'

        mongo_connection_params = conn.get_mongo_parameters(config_file_path + '/connection.json')
        print("Using Mongo Connection Params as: ", mongo_connection_params)
        mongo_database = conn.get_mongo_connection(mongo_connection_params)
        mongo_coln = mongo_database['city_crimes']


        # Time series
        month_year_time_series(mongo_coln)
        month_series(mongo_coln)
        pie_chart(mongo_coln)
        age_and_number_of_crime(mongo_coln)
        time_and_number_of_crimes(mongo_coln)
        plt.show()


if __name__ == "__main__":
    main()
