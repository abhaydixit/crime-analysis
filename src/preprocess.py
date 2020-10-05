import sys
import pandas as pd


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
                      usecols=["Highest Offense Description", "Occurred Date Time", "Address", "Latitude", "Longitude"])

    # Split Date and Time
    austin[["Occurred Date", "Occurred Time"]] = austin["Occurred Date Time"].str.split("/", expand=True)

    # Split Date into 3 separate columns
    austin[["day", "month", "year"]] = austin["Occurred Date"].str.split("/", expand=True)

    # Process Time to get shift. Call get_shift function
    # <Add here>

    # Delete Columns
    del austin["Occurred Date Time"]

    return austin


def get_chicago_df(dir_path):
    # Split Date and Time
    # Split Date into 3 separate columns
    # Process Time to get shift. Call get_shift function
    # Delete Columns
    pass


def get_baltimore_df(dir_path):
    # Split Date and Time
    # Split Date into 3 separate columns
    # Process Time to get shift. Call get_shift function
    # Delete Columns
    pass


def get_la_df(dir_path):
    # Split Date and Time
    # Split Date into 3 separate columns
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
