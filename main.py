import yfinance as yf
import snowflake.connector
import datetime
from pandas import DataFrame

# Connect to Snowflake
def create_snowflake_connection():
    conn = snowflake.connector.connect(
        user='YOUR_SNOWFLAKE_USERNAME',
        password='YOUR_SNOWFLAKE_PASSWORD',
        account='YOUR_SNOWFLAKE_ACCOUNT_URL',
        warehouse='YOUR_WAREHOUSE',
        database='YOUR_DATABASE',
        schema='YOUR_SCHEMA'
    )
    return conn

def create_table():
    conn = create_snowflake_connection()
    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stock_prices (
        Date DATE,
        Open FLOAT,
        High FLOAT,
        Low FLOAT,
        Close FLOAT,
        Volume BIGINT,
        Dividends FLOAT,
        Stock_Splits FLOAT,
        Ticker VARCHAR
    )
    """
    cursor.execute(create_table_query)
    cursor.close()
    conn.close()

def load_data_to_snowflake(ticker_symbol: str):
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    
    stock = yf.Ticker(ticker_symbol)
    stock_data: DataFrame = stock.history(start=yesterday.strftime('%Y-%m-%d'), end=today.strftime('%Y-%m-%d'))
    
    stock_data.reset_index(inplace=True)
    stock_data['Ticker'] = ticker_symbol

    conn = create_snowflake_connection()
    cursor = conn.cursor()

    for _, row in stock_data.iterrows():
        insert_query = f"""
        INSERT INTO stock_prices(Date, Open, High, Low, Close, Volume, Dividends, Stock_Splits, Ticker)
        VALUES('{row['Date'].strftime('%Y-%m-%d')}', {row['Open']}, {row['High']}, {row['Low']}, {row['Close']}, {row['Volume']}, {row['Dividends']}, {row['Stock Splits']}, '{ticker_symbol}')
        """
        cursor.execute(insert_query)

    cursor.close()
    conn.close()

def main():
    create_table()
    tickers = ["AAPL", "MSFT", "GOOGL"]  # You can add more tickers if needed
    for ticker in tickers:
        load_data_to_snowflake(ticker)

if __name__ == '__main__':
    main()
