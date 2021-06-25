"""
Downloads data on all posts in all Reddit subreddits dedicated to investing. 
The time axis is as long as the Reddit API allows (typically several months).
To run this code, it is necessary to 
"""
# Imports
import praw  # imports praw for reddit access
import pandas as pd  # imports pandas for data manipulation
import datetime as dt  # imports datetime to deal with dates
import attr
import json


# List of investing subreddits obtained from https://thehiveindex.com/topics/investing/platform/reddit/
SUBREDDITS = [
    "investing",
    "stocks",
    "economics",
    "stockmarket",
    "globalmarkets",
    "wallstreetbets",
    "wallstreetbetsnew",
    "weedstocks",
    "smallstreetbets",
    "canadianinvestor",
    "spacs",
    "options",
    "finance",
    "dividends",
    "securityanalysis",
    "algotrading",
    "daytrading",
    "pennystocks",
    "investmentclub",
    "valueinvesting",
    "investing_discussion",
    "stonks",
    "shroomstocks",
    "educatedinvesting",
    "greeninvestor",
    "canadapennystocks",
    "undervaluedstonks",
    "undervaluedstocks",
]


@attr.s
class Connection:
    """
    Connection to your own reddit API
    """

    client_id: str = attr.ib()
    client_secret: str = attr.ib()
    user_agent: str = attr.ib()
    username: str = attr.ib()
    password: str = attr.ib()


def get_connection():
    """
    To download using reddit API, you need
    - a reddit account
    - to set up the api connection from your account here https://www.reddit.com/prefs/apps
    Follow the guide here to obtain your own connection https://medium.com/social-media-theories-ethics-and-analytics/automating-data-collection-from-reddit-to-invest-in-stocks-2c86fe365db9

    you need to save the dictionary with the connection keys as "reddit_connection.json"
    """
    with open("reddit_connection.json", "r") as fp:
        cd = json.load(fp)
        return Connection(
            client_id=cd.get("client_id"),
            client_secret=cd.get("client_secret"),
            user_agent=cd.get("user_agent"),
            username=cd.get("username"),
            password=cd.get("password"),
        )


def get_reddit(connection: Connection):
    reddit = praw.Reddit(
        client_id=connection.client_id,
        client_secret=connection.client_secret,
        user_agent=connection.user_agent,
        username=connection.username,
        password=connection.password,
    )

    # Create a dictionary with the variables we want to save
    DD_dict = {
        "title": [],
        "score": [],
        "id": [],
        "url": [],
        "num_comments": [],
        "date": [],
        "subreddit": [],
        "body": [],
    }  # We now loop through the posts we collected and store the data

    # Loop through all investment related subreddits
    for subreddit_name in SUBREDDITS:
        print(f"Downloading data from subreddit {subreddit_name}")
        try:
            subreddit = reddit.subreddit(subreddit_name)
            # Pull latest posts within each flair sorted from newest to oldest
            # for flair in ["DD", "Discussion", "YOLO", "Gain", "Loss"]:
            #    DD_subreddit = subreddit.search(f'flair:{flair}',limit=None,sort='new')

            #     for posts in DD_subreddit:
            for posts in subreddit.top("all", limit=None):
                DD_dict["title"].append(posts.title)
                DD_dict["score"].append(posts.score)
                DD_dict["id"].append(posts.id)
                DD_dict["url"].append(posts.url)
                DD_dict["num_comments"].append(posts.num_comments)
                DD_dict["date"].append(posts.created)
                DD_dict["body"].append(posts.selftext)
                DD_dict["subreddit"].append(subreddit_name)
        except:
            print(f"failed in subreddit `{subreddit_name}`")
    # First convert dictionary to DataFrame
    DD_data = pd.DataFrame(
        DD_dict
    )  # Function takes a variable type numeric and converts to date

    DD_data["date"] = DD_data["date"].apply(
        get_date
    )  # We replace the previous date variable with the new date variableDD_data = DD_data.assign(date = _date)# Let's check the output table
    return DD_data


def get_date(date):
    return dt.datetime.fromtimestamp(
        date
    )  # We run this function and save the result in a new object


if __name__ == "__main__":
    connection = get_connection()
    df = get_reddit(connection)
    df.to_csv("data/reddit.csv")