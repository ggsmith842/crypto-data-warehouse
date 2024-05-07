import duckdb
from sources.coinbase_api import get_candle_period

if __name__ == "__main__":

    data = get_candle_period(25)

    try:
        data.write_parquet("data\\candle_data.parquet") 

        with duckdb.connect("data\\cdw.db") as conn:
            query = """CREATE OR REPLACE TABLE candles AS 
                            SELECT
                                * 
                            FROM 
                                parquet_scan('data\\candle_data.parquet')
                    """
            duckdb.sql(query, connection=conn)
    except Exception as e:
        print('Error writing to target: ', e)
