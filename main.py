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


import boto3

# ... [Your existing code here] ...

def send_sms_alert(ticker, price, threshold):
    """
    Send SMS alert if ticker price is higher than threshold.
    """
    sns_client = boto3.client('sns', region_name='YOUR_AWS_REGION')
    message = f'ALERT: {ticker} price is now ${price}, which is higher than your threshold of ${threshold}.'
    response = sns_client.publish(
        PhoneNumber='YOUR_PHONE_NUMBER',
        Message=message
    )
    return response

def monitor_and_alert(ticker_symbol, threshold):
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    
    stock = yf.Ticker(ticker_symbol)
    stock_data: DataFrame = stock.history(start=yesterday.strftime('%Y-%m-%d'), end=today.strftime('%Y-%m-%d'))

    # Assuming you're interested in the closing price.
    today_price = stock_data['Close'].iloc[-1]

    if today_price > threshold:
        send_sms_alert(ticker_symbol, today_price, threshold)

# ... [The rest of your code here] ...




if __name__ == '__main__':
    main()
