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


if __name__ == "__main__":
    df = pd.read_csv("data/market.csv", header=[0, 1], index_col=0)
    df = process_market_data(df)
    df.to_csv("data/market_processed.csv")