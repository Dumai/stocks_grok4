import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

# Database connection


def connect_db():
    try:
        conn = psycopg2.connect(
            dbname="stocks", user="stock_user", password="master", host="localhost")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to stocks database: {e}")
        return None

# Get interested tickers


def get_interested_tickers(conn):
    try:
        query = "SELECT ticker FROM stocks.public.interested_tickers"
        tickers = pd.read_sql_query(query, conn)
        return tickers['ticker'].tolist()
    except psycopg2.Error as e:
        print(f"Error retrieving tickers: {e}")
        return []

# Truncate inflections


def truncate_inflections(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM stocks.public.ticker_inflections")
        conn.commit()
        print("Truncated ticker_inflections table")
    except psycopg2.Error as e:
        print(f"Error truncating ticker_inflections: {e}")
        conn.rollback()

# Find inflections (adapted from ticker_analysis.py)


def find_inflections(df, window=2, min_change_percent=5.0):
    if df.empty:
        return []
    highs = df['high'].to_numpy()
    lows = df['low'].to_numpy()
    dates = df['date']
    inflections = []

    high_rolling = pd.Series(highs).rolling(
        window=2*window+1, center=True).max()
    low_rolling = pd.Series(lows).rolling(window=2*window+1, center=True).min()

    for i in range(window, len(highs) - window):
        if highs[i] == high_rolling[i]:
            inflections.append(
                ('high', dates.iloc[i], highs[i], df['close'].iloc[i]))
        elif lows[i] == low_rolling[i]:
            inflections.append(
                ('low', dates.iloc[i], lows[i], df['close'].iloc[i]))

    inflections.sort(key=lambda x: x[1])
    if not inflections:
        return []

    filtered_inflections = [inflections[0]]
    for i in range(1, len(inflections)):
        curr_type, curr_date, curr_price, curr_close = inflections[i]
        prev_type, prev_date, prev_price, prev_close = filtered_inflections[-1]
        if curr_type == prev_type:
            if (curr_type == 'high' and curr_price > prev_price) or \
               (curr_type == 'low' and curr_price < prev_price):
                filtered_inflections[-1] = inflections[i]
            continue
        price_change = abs(curr_price - prev_price) / prev_price * 100
        if price_change < min_change_percent:
            continue
        filtered_inflections.append(inflections[i])

    return filtered_inflections

# Insert inflection row


def insert_inflection_row(conn, ticker, date, type_, price, close, target_change):
    try:
        cursor = conn.cursor()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        query = """
            INSERT INTO stocks.public.ticker_inflections (ticker, date, type, price, close, target_change, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (ticker, date, type_, round(float(price), 4), round(
            float(close), 4), round(float(target_change), 4), timestamp))
        conn.commit()
        print(
            f"Inserted new row: {ticker} {date} {type_} (price={price}, close={close}, target_change={target_change})")
        return 1
    except psycopg2.Error as e:
        print(f"Error inserting row for {ticker} {date} {type_}: {e}")
        conn.rollback()
        return 0

# Rebuild inflections with per-inflection target_change logic


def rebuild_inflections(conn, tickers, lookback_days=450):
    try:
        inserted_rows = 0
        start_date = (datetime.now() -
                      timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        for ticker in tickers:
            query = """
                SELECT date, high, low, close 
                FROM stocks.public.stock_data 
                WHERE ticker = %s AND date >= %s
                ORDER BY date
            """
            df = pd.read_sql_query(query, conn, params=(ticker, start_date))
            if df.empty:
                print(f"No data for {ticker} since {start_date}")
                continue

            inflections = find_inflections(
                df, window=2, min_change_percent=5.0)
            print(f"Found {len(inflections)} inflections for {ticker}")
            if not inflections:
                print(f"No inflections for {ticker}")
                continue

            # For each inflection, compute target_change from prior same-type % changes in 450-day window
            for idx, (type_, date, price, close) in enumerate(inflections):
                window_start = date - timedelta(days=450)
                prior_inflections = [inf for j, inf in enumerate(
                    inflections[:idx]) if inf[1] >= window_start and inf[0] == type_]
                if not prior_inflections:
                    target_change = 0.05
                else:
                    prior_changes = []
                    opp_type = 'high' if type_ == 'low' else 'low'
                    for p in prior_inflections:
                        p_global_idx = inflections.index(p)
                        next_opp_idx = next((j for j in range(
                            p_global_idx + 1, idx) if inflections[j][0] == opp_type), None)
                        if next_opp_idx is not None:
                            next_price = inflections[next_opp_idx][2]
                            p_price = p[2]
                            change = ((next_price - p_price) / p_price) if type_ == 'low' else abs(
                                (next_price - p_price) / p_price)
                            prior_changes.append(change)
                    target_change = np.mean(
                        prior_changes) if prior_changes else 0.05

                inserted = insert_inflection_row(
                    conn, ticker, date, type_, price, close, target_change)
                inserted_rows += inserted

        print(f"Rebuilt ticker_inflections with {inserted_rows} rows")
        return inserted_rows
    except psycopg2.Error as e:
        print(f"Error rebuilding ticker_inflections: {e}")
        conn.rollback()
        return 0

# Update target_price


def update_target_price(conn):
    try:
        cursor = conn.cursor()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        query = """
            UPDATE stocks.public.ticker_inflections
            SET target_price = CASE
                WHEN type = 'low' THEN ROUND(close * (1 + target_change), 4)
                WHEN type = 'high' THEN ROUND(close * (1 - target_change), 4)
                ELSE NULL
            END,
            last_updated = %s
            WHERE target_price IS NULL
        """
        cursor.execute(query, (timestamp,))
        conn.commit()
        print(f"Updated {cursor.rowcount} rows for target_price")
        return cursor.rowcount
    except psycopg2.Error as e:
        print(f"Error updating target_price: {e}")
        conn.rollback()
        return 0

# Update days_to_target


def update_days_to_target(conn):
    try:
        cursor = conn.cursor()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute("""
            SELECT ticker, date, type, close, target_price
            FROM stocks.public.ticker_inflections
            WHERE days_to_target IS NULL
        """)
        rows = cursor.fetchall()
        updated_rows = 0

        for ticker, inflection_date, type_, close, target_price in rows:
            if target_price is None:
                print(
                    f"Skipping {ticker} {inflection_date}: target_price is NULL")
                continue

            if type_ == 'low':
                query = """
                    SELECT MIN(date)
                    FROM stocks.public.stock_data
                    WHERE ticker = %s AND date >= %s AND high >= %s
                """
                params = (ticker, inflection_date, target_price)
            else:  # high
                query = """
                    SELECT MIN(date)
                    FROM stocks.public.stock_data
                    WHERE ticker = %s AND date >= %s AND low <= %s
                """
                params = (ticker, inflection_date, target_price)

            cursor.execute(query, params)
            first_match_date = cursor.fetchone()[0]

            if first_match_date is None:
                cursor.execute("""
                    UPDATE stocks.public.ticker_inflections
                    SET days_to_target = NULL, last_updated = %s
                    WHERE ticker = %s AND date = %s
                """, (timestamp, ticker, inflection_date))
                print(
                    f"Set days_to_target to NULL for {ticker} {inflection_date}")
            else:
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM stocks.public.stock_data
                    WHERE ticker = %s AND date >= %s AND date <= %s
                """, (ticker, inflection_date, first_match_date))
                trading_days = cursor.fetchone()[0]
                cursor.execute("""
                    UPDATE stocks.public.ticker_inflections
                    SET days_to_target = %s, last_updated = %s
                    WHERE ticker = %s AND date = %s
                """, (trading_days, timestamp, ticker, inflection_date))
                print(
                    f"Set days_to_target to {trading_days} for {ticker} {inflection_date}")

            conn.commit()
            updated_rows += 1

        print(f"Updated {updated_rows} rows for days_to_target")
        return updated_rows
    except psycopg2.Error as e:
        print(f"Error updating days_to_target: {e}")
        conn.rollback()
        return 0

# Main function


def main():
    conn = connect_db()
    if conn is None:
        return

    tickers = get_interested_tickers(conn)
    if not tickers:
        print("No tickers found in interested_tickers")
        conn.close()
        return

    print("Starting rebuild of ticker_inflections")
    truncate_inflections(conn)
    inserted_rows = rebuild_inflections(conn, tickers)
    print(f"Rebuild complete: {inserted_rows} rows")

    print("Starting target_price population")
    updated_rows = update_target_price(conn)
    print(f"Target_price complete: {updated_rows} rows")

    print("Starting days_to_target population")
    updated_rows = update_days_to_target(conn)
    print(f"Days_to_target complete: {updated_rows} rows")

    conn.close()
    print("Analysis complete")


if __name__ == "__main__":
    main()
