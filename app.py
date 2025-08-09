import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px

# DB connection function


def get_db_connection():
    return psycopg2.connect(dbname="Stocks", user="stock_user", password="master", host="localhost")

# Fetch tickers


def fetch_tickers():
    conn = get_db_connection()
    df = pd.read_sql(
        "SELECT ticker, golden, notes FROM stocks.public.interested_tickers ORDER BY ticker;", conn)
    conn.close()
    return df

# Fetch stock data for a ticker


def fetch_stock_data(ticker):
    conn = get_db_connection()
    df = pd.read_sql("""
        SELECT date, open, high, low, close, volume 
        FROM stocks.public.stock_data 
        WHERE ticker = %s ORDER BY date DESC LIMIT 30;
    """, conn, params=(ticker,))
    conn.close()
    return df

# Add new ticker


def add_ticker(ticker, golden, notes):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO stocks.public.interested_tickers (ticker, golden, notes) 
        VALUES (%s, %s, %s) 
        ON CONFLICT (ticker) DO UPDATE SET golden = EXCLUDED.golden, notes = EXCLUDED.notes;
    """, (ticker.upper(), golden, notes))
    conn.commit()
    cur.close()
    conn.close()


# Streamlit app
st.title("Stock Tracker Dashboard")

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

# Select ticker for details
selected_ticker = st.selectbox("View Data For", tickers_df['ticker'])
if selected_ticker:
    data_df = fetch_stock_data(selected_ticker)
    if not data_df.empty:
        st.subheader(f"Recent 30 Days for {selected_ticker}")
        st.dataframe(data_df)

        # Candlestick chart
        fig = px.candlestick(data_df, x='date', open='open', high='high',
                             low='low', close='close', title=f"{selected_ticker} Candlestick")
        st.plotly_chart(fig)
    else:
        st.info("No data yet—run the download script.")
