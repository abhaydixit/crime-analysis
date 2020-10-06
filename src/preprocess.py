__author__ = "Abhay Rajendra Dixit"
__author__ = "Adya Shrivastava"
__author__ = "Pranjal Pandey"

import pandas as pd
import numpy as np
import json

pd.options.mode.chained_assignment = None


def get_df(dir_path):
    austin_df = get_austin_df(dir_path)
    baltimore_df = get_baltimore_df(dir_path)
    chicago_df = get_chicago_df(dir_path)
    la_df = get_la_df(dir_path)
    rochester_df = get_rochester_df(dir_path)

    return austin_df, baltimore_df, la_df, chicago_df, rochester_df


def format_time():
    pass


def get_shift():
    pass


def clean_address(df, col):
    # df[col] = df[col].str.replace('\\d+', '').str.strip()
    df[col].replace(
        to_replace=[" N ", " E ", " S ", " W ", " roa$| ROA$", " rd$| RD$", " ave$| AVE$", " stree$| STREE$", " ln|LN",
                    " boulev$| BOULEV$", " blvd$| BLVD$", " blvd | BLVD ", " dr$| DR$", " parkwa$| PARKWA$",
                    " st$| ST$"],
        value=[" NORTH ", " EAST ", " SOUTH ", " WEST ", " ROAD", " ROAD", " AVENUE", " STREET", " LINE",
               " BOULEVARD", " BOULEVARD", " BOULEVARD ", " DRIVE", " PARKWAY", " STREET"], regex=True, inplace=True)


def delete_columns(df, col_list):
    df.drop(col_list, axis=1, inplace=True)


def get_austin_df(dir_path):
    austin = pd.read_csv(dir_path + 'Austin.csv', na_values=None, keep_default_na=False, skip_blank_lines=True,
                         usecols=["Highest Offense Description", "Occurred Date Time", "Address", "Latitude",
                                  "Longitude"])

    austin.dropna(subset=['Occurred Date Time'], inplace=True)

    # Split Date and Time
    austin[["date", "time", "am/pm"]] = austin['Occurred Date Time'].str.split(" ", expand=True)
    austin['time'] = austin["time"].str.cat(austin["am/pm"], sep="")

    austin['date'].replace('', np.nan, inplace=True)
    austin.dropna(subset=["date"], inplace=True)

    # Split Date into 3 separate columns
    austin[["day", "month", "year"]] = austin["date"].str.split("/", expand=True).astype(np.int32)
    # austin.dropna(subset=["year"], inplace=True)
    # austin['year'] = austin['year'].astype(np.int32)
    # austin = austin[(austin['year'] > 2010)]
    austin.drop(austin[austin['year'] < 2011].index, inplace=True)

    # Process Time to get shift. Call get_shift function
    # <Add here>

    # Delete Columns
    delete_columns(austin, ['Occurred Date Time', 'date', 'am/pm'])

    # Clean address
    clean_address(austin, "Address")

    print("Processed Austin data!")
    return austin


def get_chicago_df(dir_path):
    chicago = pd.read_csv(dir_path + 'Chicago.csv', na_values=None, keep_default_na=False, skip_blank_lines=True,
                          usecols=["Date", "Block", "Primary Type", "Latitude", "Longitude", "Year"])

    chicago.dropna(subset=["Year"], inplace=True)
    # chicago.drop(chicago[chicago['Year'] < 2011].index, inplace=True)

    # Rename columns
    chicago.rename({'Primary Type': 'offense', 'Block': 'address'}, axis=1, inplace=True)

    # Split Date and Time
    chicago[["date", "time", "am/pm"]] = chicago['Date'].str.split(" ", expand=True)
    chicago['time'] = chicago["time"].str.cat(chicago["am/pm"], sep="")

    # Split Date into 3 separate columns
    chicago[["day", "month", "year"]] = chicago["date"].str.split("/", expand=True).astype(np.int32)
    chicago.dropna(subset=["year"], inplace=True)

    # Process Time to get shift. Call get_shift function
    # Delete Columns
    delete_columns(chicago, ['Year', 'Date', 'date', 'am/pm'])

    # Clean address
    clean_address(chicago, "address")

    print("Processed Chicago data!")
    return chicago


def get_baltimore_df(dir_path):
    baltimore = pd.read_csv(dir_path + 'Baltimore.csv', na_values=None, keep_default_na=False, skip_blank_lines=True,
                            usecols=["CrimeDate", "CrimeTime", "Location", "Description", "Weapon", "Latitude",
                                     "Longitude"])

    # Rename columns
    baltimore.rename({'CrimeTime': 'time', 'Description': 'offense', 'Location': 'address'}, axis=1,
                     inplace=True)

    # Drop rows with NA values in CrimeDate
    baltimore.dropna(subset=['CrimeDate'], inplace=True)

    # Process Time
    baltimore['time'] = baltimore['time'].str[:-2].str.replace(':', '').str.strip()

    # Split Date into 3 separate columns
    baltimore[["month", "day", "year"]] = baltimore["CrimeDate"].str.split("/", expand=True).astype(np.int32)
    baltimore.drop(baltimore[baltimore['year'] < 2011].index, inplace=True)

    # Process Time to get shift. Call get_shift function

    # Clean address
    clean_address(baltimore, ["address"])

    # Delete Columns
    delete_columns(baltimore, ["CrimeDate"])

    print("Processed Baltimore data!")
    return baltimore


def get_la_df(dir_path):
    lacity = pd.read_csv(dir_path + 'LACity.csv', na_values=None, keep_default_na=False, skip_blank_lines=True,
                         usecols=["DATE OCC", "TIME OCC", "Crm Cd Desc", "Vict Age", "Vict Sex", "Vict Descent",
                                  "Weapon Desc", "LOCATION", "LAT", "LON"])
    # Rename columns
    lacity.rename({'LON': 'longitude', 'LAT': 'latitude', 'Crm Cd Desc': 'offense', 'LOCATION': 'address'}, axis=1,
                  inplace=True)

    # Drop rows with NA values in Date Occurred
    lacity.dropna(subset=['DATE OCC'], inplace=True)

    # Split Date into 3 separate columns
    lacity[["month", "day", "year"]] = lacity['DATE OCC'].str[:11].str.split("/", expand=True).astype(np.int32)
    lacity.drop(lacity[lacity['year'] < 2011].index, inplace=True)

    # Process Time to get shift. Call get_shift function
    clean_address(lacity, ["address"])

    # Delete Columns
    delete_columns(lacity, ["DATE OCC"])

    print("Processed LA City data!")
    return lacity


def get_rochester_df(dir_path):
    rochester = pd.read_csv(dir_path + 'Rochester.csv',
                            usecols=["Geocode_Street", "OccurredFrom_Time", "OccurredFrom_Timestamp", "Statute_Text",
                                     "Weapon_Description", "X", "Y"])
    # Rename columns
    rochester.rename({'X': 'longitude', 'Y': 'latitude', 'Statute_Text': 'offense', 'Weapon_Description': 'weapon',
                      'Geocode_Street': 'address', "OccurredFrom_Time": 'time'}, axis=1, inplace=True)

    # Split Date into 3 separate columns
    rochester[["year", "month", "day"]] = rochester['OccurredFrom_Timestamp'].str[:11].str.split("/",
                                                                                                 expand=True).astype(
        np.int32)
    rochester.drop(rochester[rochester['year'] < 2011].index, inplace=True)

    # Process Time to get shift. Call get_shift function
    clean_address(rochester, ["address"])

    # Delete Columns
    delete_columns(rochester, ["OccurredFrom_Timestamp"])

    print("Processed Rochester data!")
    return rochester


def get_dataset_path(file_path):
    print("Reading datasets from", file_path)
    with open(file_path, 'r') as f:
        base_path = json.loads(f.read())
        return str(base_path['base_path'])


def get_mongo_parameters(mongo_file):
    print('Getting mongo params from', mongo_file)
    with open(mongo_file, 'r') as f:
        connection_details = json.loads(f.read())
        return dict(
            host=connection_details['host'],
            port=connection_details['port'],
            database=connection_details['database']
        )


def main():
    config_file_path = '../config'
    dir_path = get_dataset_path(config_file_path + '/path.json')
    aus, balt, chicago, la, roch = get_df(dir_path)

    mongo_connection_details = get_mongo_parameters(config_file_path + '/connection.json')
    print(mongo_connection_details)


if __name__ == "__main__":
    main()
