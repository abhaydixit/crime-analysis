__author__ = "Abhay Rajendra Dixit"
__author__ = "Adya Shrivastava"
__author__ = "Pranjal Pandey"

import pandas as pd
import numpy as np
import json
import sys
import time

from pymongo import MongoClient

pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', None)


def get_df(dir_path):
    """
    Get dataframe for each of the cities
    :param dir_path: base path containing csv files
    :return: dataframe for all the cities: Austin, Baltimore, Chicago, LA, Rochester
    """
    austin_df = get_austin_df(dir_path)
    baltimore_df = get_baltimore_df(dir_path)
    chicago_df = get_chicago_df(dir_path)
    la_df = get_la_df(dir_path)
    rochester_df = get_rochester_df(dir_path)
    return austin_df, baltimore_df, chicago_df, la_df, rochester_df


def format_time(df, column_name):
    """
    Function used to format the time
    :param df: dataframe
    :param column_name: time column
    :return: None
    """
    df[column_name] = df[column_name].astype(str)
    df[column_name] = df[column_name].replace(to_replace=r'(AM|PM)', value=r' \1', regex=True)
    df[column_name] = pd.to_datetime(df[column_name])
    df[column_name] = df[column_name].apply(lambda x: x.strftime('%H:%M:%S'))
    df[column_name] = pd.to_datetime(df[column_name], infer_datetime_format=True)


def get_shift(df, column_name):
    """
    Get shift from the hour
    :param df: dataframe
    :param column_name: time column name
    :return: None
    """
    df['hour'] = df[column_name].dt.hour
    df['hour'] = df['hour'].astype('int32')
    df['shift'] = df['hour'].apply(parse_values)
    df['time'] = df['time'].dt.time.astype(str)


def parse_values(x):
    """
    Parse input hour value to shift
    :param x: input hour value
    :return: shift as Morning, Noon, Evening or Night
    """
    if 5 <= x < 12:
        return 'Morning'
    elif 12 <= x < 16:
        return 'Noon'
    elif 16 <= x < 20:
        return 'Evening'
    else:
        return 'Night'


def change_datatypes(df, column_name):
    """
    Function used to change the datatype of a specific column to float32
    :param df: dataframe
    :param column_name: column name
    :return: None
    """
    df[column_name].replace('', np.NAN, inplace=True)
    df[column_name] = df[column_name].astype(np.float32)
    df[column_name].replace(np.NAN, None, inplace=True)


def clean_address(df, column_name):
    """
    Function used to clean the addresses, for ex, ST will be replaced by STREET, N will be replaced by NORTH, etc
    :param df: dataframe
    :param column_name: column name
    :return: None
    """
    df[column_name].replace(
        to_replace=[" N ", "^N ", " E ", "^E", " S ", "^S", " W ", "^W", " roa$| ROA$", " rd$| RD$", " ave$| AVE$", " AV$",
                    " stree$| STREE$", " ln|LN", " boulev$| BOULEV$", " blvd$| BLVD$", " blvd | BLVD ", " BL$",  " dr$| DR$",
                    " parkwa$| PARKWA$", " st$| ST$"],
        value=[" NORTH ", "NORTH ", " EAST ", "EAST ", " SOUTH ", "SOUTH ", " WEST ", "WEST ", " ROAD", " ROAD",
               " AVENUE", " AVENUE", " STREET", " LINE", " BOULEVARD", " BOULEVARD", " BOULEVARD", " BOULEVARD ",
               " DRIVE", " PARKWAY", " STREET"], regex=True, inplace=True)


def delete_columns(df, col_list):
    """
    Function used to drop the list of columns from the dataframe
    :param df: dataframe
    :param col_list: list of columns to be dropped
    :return: None
    """
    df.drop(col_list, axis=1, inplace=True)


def get_austin_df(dir_path):
    """
    Function used to read the Austin dataset and clean it
    :param dir_path: base directory containing the Austin dataset in .csv format
    :return: austin dataframe
    """
    austin = pd.read_csv(dir_path + 'Austin.csv', na_values=None, keep_default_na=False, skip_blank_lines=True,
                         usecols=["Highest Offense Description", "Occurred Date Time", "Address", "Latitude",
                                  "Longitude"])
    # Rename columns
    austin.rename(
        {'Latitude': 'latitude', 'Longitude': 'longitude', 'Address': 'address', 'Highest Offense Description':
            'offense'}, axis=1, inplace=True)

    austin.dropna(subset=['Occurred Date Time'], inplace=True)

    # Split Date and Time
    austin[["date", "time", "am/pm"]] = austin['Occurred Date Time'].str.split(" ", expand=True)
    austin['time'] = austin["time"].str.cat(austin["am/pm"], sep="")

    austin['date'].replace('', np.nan, inplace=True)
    austin.dropna(subset=["date"], inplace=True)

    # Split Date into 3 separate columns
    austin[["day", "month", "year"]] = austin["date"].str.split("/", expand=True).astype(np.int32)
    austin.drop(austin[austin['year'] < 2011].index, inplace=True)

    # Process Time to get shift. Call get_shift function
    format_time(austin, 'time')
    get_shift(austin, 'time')

    # Change data type of Lat and Long
    change_datatypes(austin, 'latitude')
    change_datatypes(austin, 'longitude')

    # Delete Columns
    delete_columns(austin, ['Occurred Date Time', 'date', 'am/pm', 'hour'])

    # Clean address
    clean_address(austin, "address")

    austin = austin[['offense', 'address', 'day', 'month', 'year', 'time', 'shift', 'longitude', 'latitude']]

    # austin.info()
    print(austin.head())
    print("Processed Austin data!")
    return austin


def get_chicago_df(dir_path):
    """
    Function used to read the Chicago dataset and clean it
    :param dir_path: base directory containing the Chicago dataset in .csv format
    :return: chicago dataframe
    """
    chicago = pd.read_csv(dir_path + 'Chicago.csv', na_values=None, keep_default_na=False, skip_blank_lines=True,
                          usecols=["Date", "Block", "Primary Type", "Latitude", "Longitude", "Year"])

    chicago.dropna(subset=["Year"], inplace=True)
    chicago.drop(chicago[chicago['Year'] < 2011].index, inplace=True)

    # Rename columns
    chicago.rename({'Primary Type': 'offense', 'Block': 'address', 'Latitude': 'latitude', 'Longitude': 'longitude'},
                   axis=1, inplace=True)

    # Split Date and Time
    chicago[["date", "time", "am/pm"]] = chicago['Date'].str.split(" ", expand=True)
    chicago['time'] = chicago["time"].str.cat(chicago["am/pm"], sep="")

    # Split Date into 3 separate columns
    chicago[["day", "month", "year"]] = chicago["date"].str.split("/", expand=True).astype(np.int32)
    chicago.dropna(subset=["year"], inplace=True)

    # Change data type of Lat and Long
    change_datatypes(chicago, 'latitude')
    change_datatypes(chicago, 'longitude')

    # Process Time to get shift. Call get_shift function
    format_time(chicago, 'time')
    get_shift(chicago, 'time')

    # Delete Columns
    delete_columns(chicago, ['Year', 'Date', 'date', 'am/pm', 'hour'])

    # Clean address
    clean_address(chicago, "address")

    chicago = chicago[['offense', 'address', 'day', 'month', 'year', 'time', 'shift', 'longitude', 'latitude']]

    # chicago.info()
    print(chicago.head())
    print("Processed Chicago data!")
    return chicago


def get_baltimore_df(dir_path):
    """
    Function used to read the Baltimore dataset and clean it
    :param dir_path: base directory containing the Baltimore dataset in .csv format
    :return: Baltimore dataframe
    """
    baltimore = pd.read_csv(dir_path + 'Baltimore.csv', na_values=None, keep_default_na=False, skip_blank_lines=True,
                            usecols=["CrimeDate", "CrimeTime", "Location", "Description", "Weapon", "Latitude",
                                     "Longitude"])

    # Rename columns
    baltimore.rename({'CrimeTime': 'time', 'Description': 'offense', 'Location': 'address', 'Latitude': 'latitude',
                      'Longitude': 'longitude', 'Weapon': 'weapon'}, axis=1, inplace=True)

    # Drop rows with NA values in CrimeDate
    baltimore.dropna(subset=['CrimeDate'], inplace=True)

    # Process Time
    baltimore['time'] = baltimore['time'].str.replace(':', '').str.strip()
    baltimore['time'] = baltimore['time'].astype(str)
    baltimore['time'] = baltimore['time'].apply(lambda x: x.zfill(4) if len(x) < 4 else x)
    baltimore['time'] = pd.to_datetime(baltimore['time'], format='%H%M%S')
    baltimore['time'] = pd.to_datetime(baltimore['time'], infer_datetime_format=True)

    # Split Date into 3 separate columns
    baltimore[["month", "day", "year"]] = baltimore["CrimeDate"].str.split("/", expand=True).astype(np.int32)
    baltimore.drop(baltimore[baltimore['year'] < 2011].index, inplace=True)

    # Process Time to get shift. Call get_shift function
    get_shift(baltimore, 'time')

    # Change data type of Lat and Long
    change_datatypes(baltimore, 'latitude')
    change_datatypes(baltimore, 'longitude')

    # Clean address
    clean_address(baltimore, "address")

    # Delete Columns
    delete_columns(baltimore, ["CrimeDate", 'hour'])

    baltimore = baltimore[['offense', 'address', 'day', 'month', 'year', 'time', 'shift', 'longitude',
                           'latitude', 'weapon']]

    # baltimore.info()
    print(baltimore.head())
    print("Processed Baltimore data!")
    return baltimore


def get_la_df(dir_path):
    """
    Function used to read the LA City dataset and clean it
    :param dir_path: base directory containing the LA City dataset in .csv format
    :return: lacity dataframe
    """
    lacity = pd.read_csv(dir_path + 'LACity.csv', na_values=None, keep_default_na=False, skip_blank_lines=True,
                         usecols=["DATE OCC", "TIME OCC", "Crm Cd Desc", "Vict Age", "Vict Sex",
                                  "Weapon Desc", "LOCATION", "LAT", "LON"])
    # Rename columns
    lacity.rename({'LON': 'longitude', 'LAT': 'latitude', 'Crm Cd Desc': 'offense', 'LOCATION': 'address',
                   'Weapon Desc': 'weapon', 'TIME OCC': 'time', 'Vict Sex': 'sex', 'Vict Age': 'age'}, axis=1,
                  inplace=True)

    # Drop rows with NA values in Date Occurred
    lacity.dropna(subset=['DATE OCC'], inplace=True)

    # Split Date into 3 separate columns
    lacity[["month", "day", "year"]] = lacity['DATE OCC'].str[:11].str.split("/", expand=True).astype(np.int32)
    lacity.drop(lacity[lacity['year'] < 2011].index, inplace=True)

    # Process Time to get shift. Call get_shift function
    lacity['time'] = lacity['time'].astype(str)
    lacity['time'] = lacity['time'].apply(lambda x: x.zfill(4) if len(x) < 4 else x)
    lacity['time'] = pd.to_datetime(lacity['time'], format='%H%M%S')
    lacity['time'] = pd.to_datetime(lacity['time'], infer_datetime_format=True)
    get_shift(lacity, 'time')

    # Change data type of Lat and Long
    change_datatypes(lacity, 'latitude')
    change_datatypes(lacity, 'longitude')

    # Clean address
    clean_address(lacity, "address")

    # Delete Columns
    delete_columns(lacity, ["DATE OCC", 'hour'])

    lacity = lacity[['offense', 'address', 'day', 'month', 'year', 'time', 'shift', 'longitude', 'latitude', 'weapon',
                     'sex', 'age']]

    # lacity.info()
    print(lacity.head())
    print("Processed LA City data!")
    return lacity


def get_rochester_df(dir_path):
    """
    Function used to read the Rochester dataset and clean it
    :param dir_path: base directory containing the Rochester dataset in .csv format
    :return: rochester dataframe
    """
    rochester = pd.read_csv(dir_path + 'Rochester.csv',
                            usecols=["Geocode_Street", "OccurredFrom_Time", "OccurredFrom_Timestamp", "Statute_Text",
                                     "Weapon_Description", "X", "Y"])
    # Rename columns
    rochester.rename({'X': 'longitude', 'Y': 'latitude', 'Statute_Text': 'offense', 'Weapon_Description': 'weapon',
                      'Geocode_Street': 'address', "OccurredFrom_Time": 'time'}, axis=1, inplace=True)

    # Split Date into 3 separate columns
    rochester[["year", "month", "day"]] = rochester['OccurredFrom_Timestamp'].str[:11].str.split("/",
                                                                                expand=True).astype(np.int32)
    rochester.drop(rochester[rochester['year'] < 2011].index, inplace=True)

    # Process Time to get shift. Call get_shift function
    rochester['time'] = rochester['time'].astype(str)
    rochester['time'] = rochester['time'].apply(lambda x: x.zfill(4) if len(x) < 4 else x)
    rochester['time'] = pd.to_datetime(rochester['time'], format='%H%M%S')
    rochester['time'] = pd.to_datetime(rochester['time'], infer_datetime_format=True)
    get_shift(rochester, 'time')

    # Change data type of Lat and Long
    change_datatypes(rochester, 'latitude')
    change_datatypes(rochester, 'longitude')

    # Clean Address
    clean_address(rochester, "address")

    # Delete Columns
    delete_columns(rochester, ["OccurredFrom_Timestamp", 'hour'])

    rochester = rochester[['offense', 'address', 'day', 'month', 'year', 'time', 'shift', 'longitude', 'latitude', 'weapon']]

    # rochester.info()
    print(rochester.head())
    print("Processed Rochester data!")
    return rochester


def get_dataset_path(file_path):
    """
    Function to get the base file path containing the datasets in .csv format from the configuration file
    :param file_path: File containing the base file path. Should be a '.json' file
    :return: base file path
    """
    print("Reading datasets from", file_path)
    with open(file_path, 'r') as f:
        base_path = json.loads(f.read())
        return str(base_path['base_path'])


def get_mongo_parameters(mongo_file):
    """
    Function to get the mongo parameters from the configuration file
    :param mongo_file: File containing the Mongo connection details. Should be a '.json' file
    :return: Mongo connection parameters
    """
    print('Getting mongo params from', mongo_file)
    with open(mongo_file, 'r') as f:
        connection_details = json.loads(f.read())
        return dict(
            host=connection_details['host'],
            port=connection_details['port'],
            database=connection_details['database']
        )


def get_mongo_connection(mongo_params):
    """
    Function to get Mongo connection
    :param mongo_params: dictionary containing mongo parameters
    :return: Mongo Connection
    """
    try:
        connection = MongoClient("mongodb://" + mongo_params['host'] + ":" + str(mongo_params['port']))
        print("Connected to MongoDB successfully")
    except:
        print("Could not connect to MongoDB. Please check the connection parameters")
        sys.exit(-1)
    return connection[mongo_params['database']]


def insert_df_to_mongo(mongo_collection, dataframe, city_name):
    """
    Function to export dataframe to MongoDB
    :param mongo_collection: collection name
    :param dataframe: dataframe to be exported
    :param city_name: City name
    :return: None
    """
    dataframe.insert(loc=0, column='city', value=city_name)

    for i in range(0, len(dataframe.index), 1000):
        new_df = dataframe[i: i + 1000]
        dataframe_dict = new_df.to_dict('records')
        mongo_collection.insert_many(dataframe_dict)
    print('Inserted Data to MongoDB for', city_name)
    del dataframe


def insert_data_to_mongo(mongo_collection, aus, balt, chicago, la, roch):
    """
    Helper function to insert data to Mongo db
    :param mongo_collection: Mongo collection name
    :param aus: Austin dataframe
    :param balt: Baltimore datafrane
    :param chicago: Chicago datagframe
    :param la: LACity dataframe
    :param roch: Rochester dataframe
    :return: None
    """
    insert_df_to_mongo(mongo_collection, aus, 'Austin')
    insert_df_to_mongo(mongo_collection, balt, 'Baltimore')
    insert_df_to_mongo(mongo_collection, chicago, 'Chicago')
    insert_df_to_mongo(mongo_collection, la, 'LA City')
    insert_df_to_mongo(mongo_collection, roch, 'Rochester')


def main():
    """
    Main function
    :return: None
    """
    start = time.time()
    config_file_path = '../config'
    dir_path = get_dataset_path(config_file_path + '/path.json')

    # Get Dataframes
    aus, balt, chicago, la, roch = get_df(dir_path)

    mongo_connection_params = get_mongo_parameters(config_file_path + '/connection.json')
    print("Using Mongo Connection Params as: ", mongo_connection_params)
    mongo_database = get_mongo_connection(mongo_connection_params)
    mongo_collection = mongo_database['Crime Analysis by City']

    insert_data_to_mongo(mongo_collection, aus, balt, chicago, la, roch)
    end = time.time()
    print('Time taken :', (end - start))


if __name__ == "__main__":
    main()
