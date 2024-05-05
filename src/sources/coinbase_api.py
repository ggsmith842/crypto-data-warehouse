import requests
import json
import datetime
import polars as pl


def get_daily_candles(
    product_id: str = "BTC-USD", granularity: int = 300, delta_val: int = 0
) -> pl.DataFrame:

    base_url = "https://api.pro.coinbase.com/products/"
    # Your desired product (e.g., ETH-EUR, "BTC-USD")
    endpoint = f"{base_url}{product_id}/candles"
    print(endpoint)

    # Define timeframe (granularity) in seconds (supported values: 60, 300, 900, 3600, 86400)

    # Set timestamps for start and end of data range (in ISO 8601 format)
    now = datetime.datetime.now() - datetime.timedelta(days=delta_val)
    start_time = int((now - datetime.timedelta(days=1)).timestamp())  # One day back
    end_time = int(now.timestamp())

    params = {
        "start": start_time,
        "end": end_time,
        "granularity": granularity,
    }

    headers = {}  # Add any required headers (refer to Coinbase API docs)

    try:
        response = requests.get(endpoint, params=params, headers=headers)
        response.raise_for_status()  # Raise an exception for unsuccessful requests
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        exit()

    json_response = response.json()

    df = pl.DataFrame(
        json_response,
        schema={
            "time": float,
            "low": float,
            "high": float,
            "open": float,
            "close": float,
            "volume": float,
        },
    ).with_columns(
        tradetime=pl.from_epoch(pl.col("time"), time_unit="s"),
        product_id=pl.lit(product_id),
    )
    return df


def join_dataframes(tmp_dfs: list):
    return pl.concat(tmp_dfs)


def get_candle_period(num_days: int) -> pl.DataFrame:

    deltas = [i for i in range(num_days)]
    tmp_dfs = [get_daily_candles(delta_val=d) for d in deltas]

    return join_dataframes(tmp_dfs)


if __name__ == "__main__":
    data = get_candle_period(3)
