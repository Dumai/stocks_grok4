def rebuild_inflections(conn, tickers, lookback_days=900):
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
            if not inflections:
                print(f"No inflections for {ticker}")
                continue

            target_change = calculate_target_change(inflections)
            for type_, date, price, close in inflections:
                inserted = insert_inflection_row(
                    conn, ticker, date, type_, price, close, target_change)
                inserted_rows += inserted

        print(f"Rebuilt ticker_inflections with {inserted_rows} rows")
        return inserted_rows
    except psycopg2.Error as e:
        print(f"Error rebuilding ticker_inflections: {e}")
        conn.rollback()
        return 0