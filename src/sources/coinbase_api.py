import datetime
import aiohttp
import asyncio
import polars as pl


async def get_daily_candles(session: aiohttp.ClientSession,
        
    product_id: str = "BTC-USD", granularity: int = 300, delta_val: int = 0
) -> pl.DataFrame:
    '''
    Get the candles for a single day 
    '''

    base_url = "https://api.pro.coinbase.com/products/"
    # Your desired product (e.g., ETH-EUR, "BTC-USD")
    
    # Define timeframe (granularity) in seconds (supported values: 60, 300, 900, 3600, 86400)

    # Set timestamps for start and end of data range (in ISO 8601 format)
    now = datetime.datetime.now() - datetime.timedelta(days=delta_val)
    start_time = int((now - datetime.timedelta(days=1)).timestamp())  # One day back
    end_time = int(now.timestamp())


    endpoint = f"{base_url}{product_id}/candles?granularity=300&start={start_time}&end={end_time}"

    async with session.get(endpoint) as response:
        if response.status == 200:
            json_response = await response.json()
            
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
        else:
            return None


async def join_dataframes(tmp_dfs: list):
    return pl.concat(tmp_dfs)


async def get_candle_period(num_days: int) -> pl.DataFrame:
    '''Get candles for a period of n days
    num_days: the number of days we want data for
    '''

    deltas = [i for i in range(num_days)]
    # endpoints = [ await get_daily_candles(session=session,delta_val=d) for d in deltas]
    tasks = []

    async with aiohttp.ClientSession() as session:
        for i in deltas:
            tasks.append(get_daily_candles(session=session, delta_val=i))

        tmp_dfs = await asyncio.gather(*tasks)

    return await join_dataframes(tmp_dfs)
  

async def main():
    data = await get_candle_period(15)
    print(data.head())
    print(data.shape)

if __name__ == "__main__":
    asyncio.run(main())

