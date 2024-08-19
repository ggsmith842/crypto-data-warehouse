import duckdb
import asyncio
from sources.coinbase_api import get_candle_period


async def get_data(n: int):
    data = await get_candle_period(n)
    return data


if __name__ == "__main__":

    # seems to be an issue trying to execute more than 16 at once
    data = asyncio.run(get_data(n=15))
    print(data.shape)

    try:
        data.write_parquet("data\\candle_data.parquet")

        with duckdb.connect("data\\cdw.db") as conn:
            table_exists = conn.execute(
                f"SELECT * FROM information_schema.tables WHERE table_name='CANDLES'"
            ).fetchone()

            if table_exists:
                query = """INSERT INTO 
                            CANDLES 
                                SELECT 
                                * FROM parquet_scan('data\\candle_data.parquet')
                    """

            else:
                query = """CREATE TABLE IF NOT EXISTS CANDLES AS 
                                SELECT
                                    * 
                                FROM 
                                    parquet_scan('data\\candle_data.parquet')
                        """
            duckdb.sql(query, connection=conn)
    except Exception as e:
        print("Error writing to target: ", e)
