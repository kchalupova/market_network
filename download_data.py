#!/usr/bin/env python

from pathlib import Path
import re

import ftplib  # for get_tickers
import yfinance as yf  # for get_market_data


def get_tickers(folder_path="data"):
    """
    Downlaods current tickers from http://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs

    The exchanges in `otherlisted` are:
    A = NYSE MKT
    N = New York Stock Exchange (NYSE)
    P = NYSE ARCA
    Z = BATS Global Markets (BATS)
    V = Investors' Exchange, LLC (IEXG)

    The exchange in `nasdaqlisted` is NASDAQ.
    """
    # Connect to ftp.nasdaqtrader.com
    ftp = ftplib.FTP("ftp.nasdaqtrader.com", "anonymous", "anonymous@debian.org")

    # Download files nasdaqlisted.txt and otherlisted.txt from ftp.nasdaqtrader.com
    for fname in ["nasdaqlisted.txt", "otherlisted.txt"]:
        ftp.cwd("/SymbolDirectory")
        localfile = open(Path(folder_path, fname), "wb")
        ftp.retrbinary("RETR " + fname, localfile.write)
        localfile.close()
    ftp.quit()

    # Grep for common stock in nasdaqlisted.txt and otherlisted.txt
    for fname in ["nasdaqlisted.txt", "otherlisted.txt"]:
        localfile = open(Path(folder_path, fname), "r")
        for line in localfile:
            if re.search("Common Stock", line):
                ticker = line.split("|")[0]
                # Append tickers to file tickers.txt
                open(Path(folder_path, "tickers.txt"), "a+").write(ticker + "\n")


def get_market_data(tickers):
    """
    Downloads daily market data of all tickers,
    from the past 1 month using Yahoo finance API

    Arguments:
    tickers (list or str): list or string of ticker symbols to be downloaded
    """
    print("Downloading market data...")
    df = yf.download(
        tickers=tickers,
        period="1mo",
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        prepost=False,
        threads=True,
        proxy=None,
    )
    return df


if __name__ == "__main__":
    # Download tickers from NASDAQ API
    get_tickers(folder_path="data")
    with open("data/tickers", "r") as f:
        tickers = f.read().splitlines()

    # Download market data from yahoo finance for all tickers
    df = get_market_data(tickers=tickers)
    df.to_csv("data/market.csv")