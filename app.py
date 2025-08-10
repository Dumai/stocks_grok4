import streamlit as st
import sqlalchemy as sa
import pandas as pd
import yfinance as yf
from datetime import datetime, date

# DB engine (SQLAlchemy)
engine = sa.create_engine(
    'postgresql+psycopg2://stock_user:master@localhost/stocks')

# Fetch tickers


def fetch_tickers():
    with engine.connect() as conn:
        df = pd.read_sql(
            "SELECT ticker, golden, notes FROM stocks.public.interested_tickers ORDER BY ticker;", conn)
    return df

# Fetch stock data for a ticker (historical)


def fetch_stock_data(ticker):
    with engine.connect() as conn:
        df = pd.read_sql("""
            SELECT date, open, high, low, close, volume 
            FROM stocks.public.stock_data 
            WHERE ticker = %s ORDER BY date DESC LIMIT 30;
        """, conn, params=(ticker,))
    return df

# Fetch current prices


def fetch_current_prices():
    with engine.connect() as conn:
        df = pd.read_sql(
            "SELECT * FROM stocks.public.current_prices ORDER BY ticker;", conn)
    return df

# Populate current prices (truncate and reload)


def populate_current_prices():
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(sa.text("TRUNCATE stocks.public.current_prices;"))
            tickers = fetch_tickers()['ticker'].tolist()
            inserted = 0
            for ticker in tickers:
                yf_ticker = get_yfinance_ticker(ticker)
                try:
                    data = yf.Ticker(yf_ticker).info
                    current = data.get('currentPrice')
                    open_ = data.get('regularMarketOpen')
                    high = data.get('regularMarketDayHigh')
                    low = data.get('regularMarketDayLow')
                    vol = data.get('regularMarketVolume')
                    if current is not None:
                        timestamp = datetime.now()
                        conn.execute(sa.text("""
                            INSERT INTO stocks.public.current_prices (ticker, current_price, day_open, day_high, day_low, volume, last_updated)
                            VALUES (:ticker, :current, :open_, :high, :low, :vol, :timestamp);
                        """), {'ticker': ticker, 'current': round(float(current), 4),
                               'open_': round(float(open_), 4) if open_ else None,
                               'high': round(float(high), 4) if high else None,
                               'low': round(float(low), 4) if low else None,
                               'vol': int(vol) if vol else None, 'timestamp': timestamp})
                        inserted += 1
                except Exception as e:
                    st.error(f"Error fetching current for {ticker}: {e}")
    return inserted


def get_yfinance_ticker(ticker):
    if ticker.endswith('USD') and ticker[:-3].isalpha() and len(ticker[:-3]) == 3:
        return ticker[:-3] + '-USD'
    return ticker

# Add new ticker


def add_ticker(ticker, golden, notes):
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(sa.text("""
                INSERT INTO stocks.public.interested_tickers (ticker, golden, notes) 
                VALUES (:ticker, :golden, :notes) 
                ON CONFLICT (ticker) DO UPDATE SET golden = EXCLUDED.golden, notes = EXCLUDED.notes;
            """), {'ticker': ticker.upper(), 'golden': golden, 'notes': notes})

# Fetch accounts


def fetch_accounts():
    with engine.connect() as conn:
        df = pd.read_sql(
            "SELECT account_name FROM stocks.public.accounts ORDER BY account_name;", conn)
    return df

# Add new position


def add_position(account, ticker, buy_date, invested_amount, shares, notes):
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(sa.text("""
                INSERT INTO stocks.public.open_positions (account, ticker, buy_date, invested_amount, shares, notes) 
                VALUES (:account, :ticker, :buy_date, :invested_amount, :shares, :notes);
            """), {'account': account, 'ticker': ticker.upper(), 'buy_date': buy_date,
                   'invested_amount': round(invested_amount, 2), 'shares': shares,
                   'notes': notes})


# Streamlit app
st.title("Stock Tracker Dashboard")

# Load current prices on startup if not already loaded
if 'prices_loaded' not in st.session_state:
    st.session_state.prices_loaded = True
    with st.spinner("Loading current prices..."):
        inserted = populate_current_prices()
        st.success(f"Loaded current prices for {inserted} tickers on startup")

# Sidebar for adding tickers
with st.sidebar:
    st.header("Add Ticker")
    new_ticker = st.text_input("Ticker Symbol")
    golden = st.checkbox("Golden (Priority)")
    notes = st.text_area("Notes")
    if st.button("Add"):
        if new_ticker:
            add_ticker(new_ticker, golden, notes)
            st.success(f"Added {new_ticker}")

# Display tickers
tickers_df = fetch_tickers()
st.header("Interested Tickers")
st.dataframe(tickers_df.style.apply(
    lambda row: ['background: gold' if row.golden else ''] * len(row), axis=1))

# Current prices section
st.header("Current Prices")
if st.button("Refresh Current Prices"):
    with st.spinner("Refreshing current prices..."):
        inserted = populate_current_prices()
        st.success(f"Updated current prices for {inserted} tickers")
current_df = fetch_current_prices()
if not current_df.empty:
    st.dataframe(current_df)
else:
    st.info("No current prices loaded yet.")

# Select ticker for details
selected_ticker = st.selectbox("View Data For", tickers_df['ticker'])
if selected_ticker:
    data_df = fetch_stock_data(selected_ticker)
    if not data_df.empty:
        st.subheader(f"Recent 30 Days for {selected_ticker}")
        st.dataframe(data_df)
    else:
        st.info("No historical data yet—run the download script.")

    # Current details for selected ticker
    if not current_df.empty:
        row = current_df[current_df['ticker'] == selected_ticker]
        if not row.empty:
            st.subheader(f"Current Details for {selected_ticker}")
            col1, col2, col3 = st.columns(3)
            col1.metric("Current Price",
                        f"${row['current_price'].values[0]:.2f}")
            col2.metric(
                "Day High", f"${row['day_high'].values[0]:.2f}" if row['day_high'].values[0] else "N/A")
            col3.metric(
                "Day Low", f"${row['day_low'].values[0]:.2f}" if row['day_low'].values[0] else "N/A")
            st.metric(
                "Volume", f"{row['volume'].values[0]:,}" if row['volume'].values[0] else "N/A")
            st.caption(f"Last updated: {row['last_updated'].values[0]}")
        else:
            st.info("No current data for this ticker—refresh above.")

# Add Position section
st.header("Add Position")
accounts_df = fetch_accounts()
if accounts_df.empty:
    st.info("No accounts available. Please add accounts to the database.")
else:
    position_ticker = st.selectbox("Ticker", tickers_df['ticker'].tolist())
    account_options = accounts_df['account_name'].tolist()
    selected_account = st.selectbox("Account", account_options)
    buy_date = st.date_input("Buy Date", value=date.today())
    invested_amount = st.number_input(
        "Invested Amount", min_value=0.0, step=0.01)
    shares = st.number_input("Shares", min_value=0.0, step=0.01)
    position_notes = st.text_area("Position Notes")
    if st.button("Add Position"):
        if position_ticker and selected_account and invested_amount > 0 and shares > 0:
            add_position(selected_account, position_ticker,
                         buy_date, invested_amount, shares, position_notes)
            st.success(
                f"Added position for {position_ticker} in {selected_account}")
        else:
            st.error("Please fill in all required fields.")
