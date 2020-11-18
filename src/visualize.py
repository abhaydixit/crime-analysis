import sys
import time
import matplotlib.pyplot as plt
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


    print(crime_series)
    austin_dict = {'city': 'Austin', "offense": [], "crime count": []}
    baltimore_dict = {'city': 'Baltimore', "offense": [], "crime count": []}
    chicago_dict = {'city': 'Chicago', "offense": [], "crime count": []}
    lacity_dict = {'city': 'LA City', "offense": [], "crime count": []}
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
        elif item['_id']['city'] == "LA City":
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
        # explode = (0, 0.1, 0, 0)  # only "explode" the 2nd slice (i.e. 'Hogs')
        plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
        plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

    plt.show()


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

    print(crime_series)
    # crime_series = list(filter(lambda x: 2019 >= x['_id']['year'] > 2012, crime_series))

    austin_dict = {'city': 'Austin', "month": [], "crime count": []}
    baltimore_dict = {'city': 'Baltimore', "month": [], "crime count": []}
    chicago_dict = {'city': 'Chicago', "month": [], "crime count": []}
    lacity_dict = {'city': 'LA City', "month": [], "crime count": []}
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
        elif item['_id']['city'] == "LA City":
            lacity_dict['month'].append(MONTH_NUMBER[item['_id']['month']])
            lacity_dict['crime count'].append(item['count'])
        elif item['_id']['city'] == "Rochester":
            roch_dict['month'].append(MONTH_NUMBER[item['_id']['month']])
            roch_dict['crime count'].append(item['count'])


    f1 = plt.figure(1)
    plt.plot(austin_dict['month'], austin_dict['crime count'], label=austin_dict['city'])
    plt.plot(baltimore_dict['month'], baltimore_dict['crime count'], label=baltimore_dict['city'])
    plt.plot(chicago_dict['month'], chicago_dict['crime count'], label=chicago_dict['city'])
    plt.plot(lacity_dict['month'], lacity_dict['crime count'], label=lacity_dict['city'])
    plt.plot(roch_dict['month'], roch_dict['crime count'], label=roch_dict['city'])

    plt.xlabel('Month')
    plt.ylabel('Number of crimes')
    plt.title('Time series of number of crimes committed in different cities')
    plt.legend()
    plt.grid(True)
    plt.show()


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
    lacity_dict = {'city': 'LA City', "date": [], "crime count": []}
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
        elif item['_id']['city'] == "LA City":
            lacity_dict['date'].append(MONTH_NUMBER[item['_id']['month']] + " " + YEAR[item['_id']['year']])
            lacity_dict['crime count'].append(item['count'])
        elif item['_id']['city'] == "Rochester":
            roch_dict['date'].append(MONTH_NUMBER[item['_id']['month']] + " " + YEAR[item['_id']['year']])
            roch_dict['crime count'].append(item['count'])


    f1 = plt.figure(1)
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



    f2 = plt.figure(2)

    austin_df = pd.DataFrame(austin_dict)
    austin_df['sc_austin'] = austin_df.iloc[:, 2].rolling(window=4).mean()

    baltimore_df = pd.DataFrame(baltimore_dict)
    baltimore_df['sc_baltimore'] = austin_df.iloc[:, 2].rolling(window=4).mean()

    chicago_df = pd.DataFrame(chicago_dict)
    chicago_df['sc_chicago'] = chicago_df.iloc[:, 2].rolling(window=9).mean()

    lacity_df = pd.DataFrame(lacity_dict)
    lacity_df['sc_lacity'] = lacity_df.iloc[:, 2].rolling(window=4).mean()

    roch_df = pd.DataFrame(roch_dict)
    roch_df['sc_roch'] = roch_df.iloc[:, 2].rolling(window=4).mean()

    plt.plot(austin_df['date'], austin_df['sc_austin'], label="Austin")
    plt.plot(baltimore_df['date'], baltimore_df['sc_baltimore'], label="Baltimore")
    plt.plot(chicago_df['date'], chicago_df['sc_chicago'], label="Chicago")
    plt.plot(lacity_df['date'], lacity_df['sc_lacity'], label="LA City")
    plt.plot(roch_df['date'], roch_df['sc_roch'], label="Rochester")

    plt.xlabel('Month-Year')
    plt.ylabel('Number of crimes')
    plt.xticks(rotation=90)
    plt.title('Time series of number of crimes committed in different cities')
    plt.legend()
    plt.grid(True)
    plt.show()


def main():
    if __name__ == '__main__':
        """
            Main function:
            Print the output,call the functions, prints
            the overall time taken.
        """
        config_file_path = '../config'
        # dir_path = get_dataset_path(config_file_path + '/path.json')

        mongo_connection_params = conn.get_mongo_parameters(config_file_path + '/connection.json')
        print("Using Mongo Connection Params as: ", mongo_connection_params)
        mongo_database = conn.get_mongo_connection(mongo_connection_params)
        mongo_coln = mongo_database['Crime Analysis by City']

        # Expand the cursor and construct the DataFrame
        # df = pd.DataFrame(list(cursor))
        # print(df.head(10))


        # Time series
        # month_year_time_series(mongo_coln)
        # month_series(mongo_coln)
        pie_chart(mongo_coln)


if __name__ == "__main__":
    main()
