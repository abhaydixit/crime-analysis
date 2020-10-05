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


def get_austin_df(dir_path):
    aus = pd.read_csv(dir_path + 'Austin.csv',
                      usecols=["Highest Offense Description", "Occurred Date", "Address", "Latitude", "Longitude"])
    aus[["day", "mm", "year"]] = aus["Occurred Date"].str.split("/", expand=True)
    return aus


def get_chicago_df(dir_path):
    pass


def get_baltimore_df(dir_path):
    pass


def get_la_df(dir_path):
    pass


def get_rochester_df(dir_path):
    pass


def main():
    # dir_path = sys.argv[1]
    dir_path = "/Users/abhayrajendradixit/Documents/Assignments and Projects/Projects/Big Data Analytics/"

    aus = get_df(dir_path)
    # aus, balt, chic, la, roch = get_df(dir_path)


if __name__ == "__main__":
    main()
