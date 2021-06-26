import datetime
import pandas as pd


def generate_date_df(start_year, start_month, start_date):
    dt = datetime.datetime(start_year, start_month, start_date)
    end = datetime.datetime.now()
    step = datetime.timedelta(days=1)

    result = []

    while dt < end:
        result.append(dt.strftime("%Y-%m-%d %H:%M:%S"))
        dt += step

    date_df = pd.DataFrame(columns=["datetime"], data=result)
    date_df["datetime"] = pd.to_datetime(date_df["datetime"])
    date_df["date"] = date_df["datetime"].dt.date
    date_df["year"] = date_df["datetime"].dt.year
    date_df["month"] = date_df["datetime"].dt.month
    date_df["week"] = date_df["datetime"].dt.week
    date_df["dayofweek"] = date_df["datetime"].dt.dayofweek
    date_df["dayofmonth"] = date_df["datetime"].dt.day
    date_df["dayname"] = date_df["datetime"].dt.day_name()
    return date_df


if __name__ == "__main__":
    df = generate_date_df(2010, 1, 1)
    df.to_csv("data/date_table.csv")