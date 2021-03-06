import pandas as pd


def process_market_data(df):
    """
    reformat dataframe
    """
    df = df.stack(level=0)  # stack ticker symbols from multiindex columns to rows
    df.index.names = ["date", "company_id"]
    df = df.round(3)
    df.drop(columns="Close", inplace=True)
    df.rename(
        columns={
            "Adj Close": "close_value",
            "High": "high_value",
            "Low": "low_value",
            "Open": "open_value",
            "Volume": "volume",
        },
        inplace=True,
    )
    return df


def process_tickers(df):
    df = df[df["Test Issue"] == "N"]
    try:
        df = df[["Symbol", "Security Name", "ETF"]]
        df.rename(
            columns={"Symbol": "company_id", "Security Name": "name", "ETF": "etf"},
            inplace=True,
        )
    except:
        df = df[["ACT Symbol", "Security Name", "ETF"]]
        df.rename(
            columns={"ACT Symbol": "company_id", "Security Name": "name", "ETF": "etf"},
            inplace=True,
        )
    return df


if __name__ == "__main__":
    # Process market data
    df = pd.read_csv("data/market.csv", header=[0, 1], index_col=0)
    df = process_market_data(df)
    df.to_csv("data/market_processed.csv")

    # Process ticker data
    df_1 = pd.read_csv("data/nasdaqlisted.txt", sep="|")
    df_1 = process_tickers(df_1)
    df_2 = pd.read_csv("data/otherlisted.txt", sep="|")
    df_2 = process_tickers(df_2)
    df = pd.concat([df_1, df_2])
    df.sort_values("company_id", inplace=True)
    df.to_csv("data/tickers_processed.csv")
