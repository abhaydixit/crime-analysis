import sys
import json
from pymongo import MongoClient


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