import sys
import pandas as pd
import numpy as np


def get_df(dir_path):
    austin_df = get_austin_df(dir_path)
    baltimore_df = get_baltimore_df(dir_path)
    chicago_df = get_chicago_df(dir_path)
    la_df = get_la_df(dir_path)
    rochester_df = get_rochester_df(dir_path)

    return austin_df, baltimore_df, chicago_df, la_df, rochester_df


def format_date():
    pass


def format_time():
    pass


def get_shift():
    pass


def get_austin_df(dir_path):
    austin = pd.read_csv(dir_path + 'Austin.csv',
                         usecols=["Highest Offense Description", "Occurred Date Time", "Address", "Latitude",
                                  "Longitude"])

    # Split Date and Time
    # austin[["Occurred Date", "Occurred Time"]] = austin["Occurred Date Time"].str.split("/", expand=True)

    # Split Date into 3 separate columns
    # austin[["day", "month", "year"]] = austin["Occurred Date"].str.split("/", expand=True)

    # Process Time to get shift. Call get_shift function
    # <Add here>

    # Delete Columns
    # del austin["Occurred Date Time"]

    return austin


def get_chicago_df(dir_path):
    chicago = pd.read_csv(dir_path + 'Chicago.csv',
                         usecols=["Date", "Occurred Date Time", "Address", "Latitude",
                                  "Longitude"])
    # Split Date and Time
    # Split Date into 3 separate columns
    # Process Time to get shift. Call get_shift function
    # Delete Columns
    pass


def get_baltimore_df(dir_path):
    baltimore = pd.read_csv(dir_path + 'Baltimore.csv',
                            usecols=["CrimeDate", "CrimeTime", "Location", "Description", "Weapon", "Latitude",
                                     "Longitude"])

    # Rename columns
    baltimore.rename({'CrimeTime': 'Time', 'Description': 'Offense', 'Location': 'Address'}, axis=1,
                  inplace=True)

    # Drop rows with NA values in CrimeDate
    baltimore.dropna(subset=['CrimeDate'], inplace=True)

    # Process Time
    baltimore['Time'] = baltimore['Time'].str[:-2].str.replace(':', '').str.strip()

    # Split Date into 3 separate columns
    baltimore[["month", "day", "year"]] = baltimore["CrimeDate"].str.split("/", expand=True).astype(np.int32)
    baltimore.drop(baltimore[baltimore['year'] < 2011].index, inplace=True)

    # Process Time to get shift. Call get_shift function

    # Process Location to get the street name

    # Delete Columns
    return baltimore


def get_la_df(dir_path):
    lacity = pd.read_csv(dir_path + 'LACity.csv',
                         usecols=["DATE OCC", "TIME OCC", "Crm Cd Desc", "Vict Age", "Vict Sex", "Vict Descent",
                                  "Weapon Desc", "LOCATION", "LAT", "LON"])
    # Rename columns
    lacity.rename({'LON': 'Longitude', 'LAT': 'Latitude', 'Crm Cd Desc': 'Offense', 'LOCATION': 'Address'}, axis=1,
                  inplace=True)

    # Drop rows with NA values in Date Occurred
    lacity.dropna(subset=['DATE OCC'], inplace=True)

    # Split Date into 3 separate columns
    lacity[["month", "day", "year"]] = lacity['DATE OCC'].str[:11].str.split("/", expand=True).astype(np.int32)
    lacity.drop(lacity[lacity['year'] < 2011].index, inplace=True)

    # Process Location to get the street name
    # lacity['Address'] = lacity['Address'].str.replace('\\d+', '').str.strip()

    # Process Time to get shift. Call get_shift function
    # Delete Columns
    return lacity


def get_rochester_df(dir_path):
    rochester = pd.read_csv(dir_path + 'Rochester.csv',
                            usecols=["Geocode_Street", "OccurredFrom_Time", "OccurredFrom_Timestamp", "Statute_Text",
                                     "Weapon_Description", "X", "Y"])
    # Rename columns
    rochester.rename({'X': 'Longitude', 'Y': 'Latitude', 'Statute_Text': 'Offense', 'Weapon_Description': 'Weapon',
                      'Geocode_Street': 'Street', "OccurredFrom_Time": 'Time'}, axis=1, inplace=True)

    # Split Date into 3 separate columns
    rochester[["year", "month", "day"]] = rochester['OccurredFrom_Timestamp'].str[:11].str.split("/", expand=True).astype(np.int32)
    rochester.drop(rochester[rochester['year'] < 2011].index, inplace=True)
    # Process Time to get shift. Call get_shift function
    # Delete Columns
    return rochester


def main():
    # dir_path = sys.argv[1]
    dir_path = "/Users/abhayrajendradixit/Documents/Assignments and Projects/Projects/Big Data Analytics/"
    # dir_path = "../Datasets/"


    aus, balt, chic, la, roch = get_df(dir_path)


if __name__ == "__main__":
    main()
