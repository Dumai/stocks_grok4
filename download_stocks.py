import psycopg2
import requests
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
import pandas as pd  # For date handling

# FMP API key (get from site.financialmodelingprep.com/register)
API_KEY = 'kDRMK9MfzSIanjRO18pAn5xIeWQnIsC9'  # Replace with your key

# DB connection
conn = psycopg2.connect(dbname="stocks", user="stock_user",
                        password="master", host="localhost")
cur = conn.cursor()

# Get original tickers
cur.execute("SELECT ticker FROM stocks.public.interested_tickers;")
original_tickers = [row[0] for row in cur.fetchall()]

# Date range: past 30 months (FMP format YYYY-MM-DD)
end_date = datetime.now().date()
start_date = end_date - timedelta(days=30*30)
start_str = start_date.strftime('%Y-%m-%d')
end_str = end_date.strftime('%Y-%m-%d')


def fetch_fmp_data(api_ticker, full=False):
    """Fetch daily adjusted OHLCV from FMP using API-compatible ticker."""
    if full:
        from_date = start_str
    else:
        from_date = (end_date - timedelta(days=5)).strftime('%Y-%m-%d')
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{api_ticker}?from={from_date}&to={end_str}&apikey={API_KEY}"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error fetching {api_ticker}: {response.text}")
        return []
    json_data = response.json()
    historical = json_data.get('historical', [])
    if not historical:
        return []
    df = pd.DataFrame(historical)
    df['date'] = pd.to_datetime(df['date']).dt.date
    df = df[df['date'] >= start_date]  # Vectorized filter
    insert_data = [
        (row['date'], row['open'], row['high'],
         row['low'], row['close'], int(row['volume']))
        for _, row in df.iterrows()
    ]
    return insert_data


def check_integrity(original_ticker):
    """Check if latest DB data matches source, using original ticker for DB."""
    cur.execute("""
        SELECT date, close FROM stocks.public.stock_data
        WHERE ticker = %s ORDER BY date DESC LIMIT 1;
    """, (original_ticker,))
    row = cur.fetchone()
    if row is None:
        return False
    db_latest_date, db_close = row
    api_ticker = original_ticker.replace(
        '-', '') if '-' in original_ticker else original_ticker
    recent_data = fetch_fmp_data(api_ticker, full=False)
    if not recent_data:
        return True
    source_close = next((d[4] for d in recent_data if d[0]
                        == db_latest_date), None)  # Index 4 is close
    if source_close is None:
        return True
    return abs(float(db_close) - source_close) < 0.01  # Cast Decimal to float


def cleanup_old_data():
    """Delete rows older than 30 months."""
    cleanup_date = start_date
    cur.execute(
        "DELETE FROM stocks.public.stock_data WHERE date < %s;", (cleanup_date,))
    conn.commit()
    print(f"Cleaned up data older than {cleanup_date}")


# Main loop
for original_ticker in original_tickers:
    api_ticker = original_ticker.replace(
        '-', '') if '-' in original_ticker else original_ticker
    if not check_integrity(original_ticker):
        print(
            f"Mismatch detected for {original_ticker}. Reloading full history.")
        cur.execute(
            "DELETE FROM stocks.public.stock_data WHERE ticker = %s;", (original_ticker,))
        conn.commit()
        raw_data = fetch_fmp_data(api_ticker, full=True)
        insert_data = [(original_ticker, d[0], d[1], d[2], d[3], d[4], d[5])
                       for d in raw_data]  # Prepend original_ticker
    else:
        cur.execute(
            "SELECT MAX(date) FROM stocks.public.stock_data WHERE ticker = %s;", (original_ticker,))
        max_date = cur.fetchone()[0]
        if max_date:
            raw_data = fetch_fmp_data(api_ticker, full=False)
            insert_data = [(original_ticker, d[0], d[1], d[2], d[3], d[4], d[5])
                           for d in raw_data if d[0] > max_date]
        else:
            raw_data = fetch_fmp_data(api_ticker, full=True)
            insert_data = [(original_ticker, d[0], d[1], d[2],
                            d[3], d[4], d[5]) for d in raw_data]

    if insert_data:
        execute_values(cur, """
            INSERT INTO stocks.public.stock_data (ticker, date, open, high, low, close, volume)
            VALUES %s
            ON CONFLICT (ticker, date) DO UPDATE SET
                open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, 
                close = EXCLUDED.close, volume = EXCLUDED.volume;
        """, insert_data)
        conn.commit()

# Run cleanup
cleanup_old_data()

cur.close()
conn.close()
