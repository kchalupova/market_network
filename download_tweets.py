import twint
import pandas as pd
import time
import os


ready_files = os.listdir("./Ticker_tweets/")

tickers = set(pd.read_csv("~/Downloads/market_processed.csv")["company_id"].to_list())

# configuration
config = twint.Config()

config.Lang = "en"
config.Limit = 100000
config.Since = "2021-05-25 00:00:00"
config.Store_csv = True

timer_start = time.time()

try:
    for ticker in tickers:
        if f"{ticker}.csv" in ready_files:
            continue

        config.Output = f"./Ticker_tweets/{ticker}.csv"
        config.Search = f"${ticker}"

        # run search
        twint.run.Search(config)

except:
    print(ticker)


timer_end = time.time()
print(timer_end - timer_start)
