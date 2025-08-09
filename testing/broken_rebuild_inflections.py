# Rebuild inflections with new per-inflection target_change logic
def rebuild_inflections(conn, tickers, lookback_days=900):  # 30 months for full inflections
    try:
        inserted_rows = 0
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        for ticker in tickers:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT date, high, low, close 
                FROM stocks.public.stock_data 
                WHERE ticker = %s AND date >= %s
                ORDER BY date
            """, (ticker, start_date))
            rows = cursor.fetchall()
            if not rows:
                print(f"No data for {ticker} since {start_date}")
                continue
            df = pd.DataFrame(rows, columns=['date', 'high', 'low', 'close'])

            inflections = find_inflections(df, window=2, min_change_percent=5.0)
            if not inflections:
                print(f"No inflections for {ticker}")
                continue

            # For each inflection, compute target_change from prior same-type % changes in 450-day window
            for idx, (type_, date, price, close) in enumerate(inflections):
                window_start = date - timedelta(days=450)
                prior_inflections = [inf for inf in inflections[:idx] if inf[1] >= window_start and inf[0] == type_]
                if not prior_inflections:
                    target_change = 5.0
                else:
                    prior_changes = []
                    opp_type = 'high' if type_ == 'low' else 'low'
                    for p_idx, p in enumerate(prior_inflections):
                        # Find next opposite after p (must be before current date)
                        next_opp_idx = next((j for j in range(p_idx + 1, idx) if inflections[j][0] == opp_type), None)
                        if next_opp_idx is not None:
                            next_price = inflections[next_opp_idx][2]
                            p_price = p[2]
                            change = ((next_price - p_price) / p_price) * 100 if type_ == 'low' else abs((next_price - p_price) / p_price) * 100
                            prior_changes.append(change)
                    target_change = np.mean(prior_changes) if prior_changes else 5.0

                inserted = insert_inflection_row(conn, ticker, date, type_, price, close, target_change)
                inserted_rows += inserted

        print(f"Rebuilt ticker_inflections with {inserted_rows} rows")
        return inserted_rows
    except psycopg2.Error as e:
        print(f"Error rebuilding ticker_inflections: {e}")
        conn.rollback()
        return 0