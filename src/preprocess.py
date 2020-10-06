__author__ = "Abhay Rajendra Dixit"
__author__ = "Adya Shrivastava"
__author__ = "Pranjal Pandey"


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


def clean_address(df, col):
    # df[col] = df[col].str.replace('\\d+', '').str.strip()
    df[col].replace(to_replace=[" N ", " E ", " S ", " W ",  " roa$| ROA$", " rd$| RD$", " ave$| AVE$", " stree$| STREE$", " ln|LN",
                                " boulev$| BOULEV$", " blvd$| BLVD$", " blvd | BLVD ",  " dr$| DR$", " parkwa$| PARKWA$", " st$| ST$"],
                                value=[" NORTH ", " EAST ", " SOUTH ", " WEST ", " ROAD", " ROAD", " AVENUE", " STREET", " LINE",
                                " BOULEVARD", " BOULEVARD", " BOULEVARD ", " DRIVE", " PARKWAY", " STREET"], regex=True, inplace=True)
    return df


def delete_columns(df, col_list):
    df.drop(col_list, axis=1, inplace=True)
    return df



# def my_split(row):
#     # print(row)
#     values = str(row["Occurred Date Time"]).split()
#     return pd.Series({
#         'date': values[0] if len(values) == 2 else None,
#         'time': values[1] if len(values) == 2 else None,
#     })


def get_austin_df(dir_path):
    austin = pd.read_csv(dir_path + 'Austin.csv', na_values=None,keep_default_na=False, skip_blank_lines=True,
                      usecols=["Highest Offense Description", "Occurred Date Time",  "Address", "Latitude", "Longitude"])
    print(austin.shape)

    # Split Date and Time
    austin[["date", "time", "am/pm"]] = austin['Occurred Date Time'].str.split(" ", expand=True)
    austin['time'] = austin["time"].str.cat(austin["am/pm"], sep="")


    # Split Date into 3 seperate columns
    austin[["day", "month", "year"]] = austin["date"].str.split("/", expand=True)
    austin.dropna(subset=["year"], inplace=True)
    austin['year'] = austin['year'].astype(np.int32)
    austin = austin[(austin['year'] > 2010)]


    # Process Time to get shift. Call get_shift function
    # <Add here>

    # Delete Columns
    austin = delete_columns(austin, ['Occurred Date Time', 'date', 'am/pm'])

    # Clean address
    austin = clean_address(austin, "Address")

    print(austin["Address"])
    print(austin)

    return austin


def get_chicago_df(dir_path):
    # Split Date and Time
    # Split Date into 3 seperate columns
    # Process Time to get shift. Call get_shift function
    # Delete Columns
    pass


def get_baltimore_df(dir_path):
    # Split Date and Time
    # Split Date into 3 seperate columns
    # Process Time to get shift. Call get_shift function
    # Delete Columns
    pass


def get_la_df(dir_path):
    # Split Date and Time
    # Split Date into 3 seperate columns
    # Process Time to get shift. Call get_shift function
    # Delete Columns
    pass


def get_rochester_df(dir_path):
    # Split Date and Time
    # Split Date into 3 seperate columns
    # Process Time to get shift. Call get_shift function
    # Delete Columns
    pass


def main():
    # dir_path = sys.argv[1]
    dir_path = "/Users/abhayrajendradixit/Documents/Assignments and Projects/Projects/Big Data Analytics/"

    aus = get_df(dir_path)
    # aus, balt, chic, la, roch = get_df(dir_path)


if __name__ == "__main__":
    main()
