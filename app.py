import streamlit as st
import sqlalchemy as sa
import pandas as pd
import yfinance as yf
from datetime import datetime, date

# Set wide layout for better screen utilization
st.set_page_config(layout="wide")

# DB engine (SQLAlchemy)
engine = sa.create_engine(
    'postgresql+psycopg2://stock_user:master@localhost/stocks')

# Fetch tickers


def fetch_tickers():
    with engine.connect() as conn:
        df = pd.read_sql(
            "SELECT ticker, golden, notes FROM stocks.public.interested_tickers ORDER BY ticker;", conn)
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

# Delete ticker by ticker symbol


def delete_ticker(ticker):
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(sa.text("""
                DELETE FROM stocks.public.interested_tickers WHERE ticker = :ticker;
            """), {'ticker': ticker})

# Fetch accounts


def fetch_accounts():
    with engine.connect() as conn:
        df = pd.read_sql(
            "SELECT account_name FROM stocks.public.accounts ORDER BY account_name;", conn)
    return df

# Updated add_position to support original_position_id


def add_position(account, ticker, buy_date, invested_amount, shares, notes, profit_position=False, original_position_id=None):
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(sa.text("""
                INSERT INTO stocks.public.open_positions (account, ticker, buy_date, invested_amount, shares, notes, profit_position, original_position_id) 
                VALUES (:account, :ticker, :buy_date, :invested_amount, :shares, :notes, :profit_position, :original_position_id);
            """), {'account': account, 'ticker': ticker.upper(), 'buy_date': buy_date,
                   'invested_amount': round(invested_amount, 2), 'shares': shares,
                   'notes': notes, 'profit_position': profit_position, 'original_position_id': original_position_id})

# Updated sell_position to set original_position_id on new profit position


def sell_position(position_id, sell_date, sell_amount, shares_sold):
    with engine.connect() as conn:
        with conn.begin():
            # Fetch original position details
            orig_df = pd.read_sql(sa.text("""
                SELECT account, ticker, buy_date, invested_amount, shares, notes, profit_position
                FROM stocks.public.open_positions WHERE id = :id;
            """), conn, params={'id': position_id})
            if orig_df.empty:
                raise ValueError("Position not found")
            orig = orig_df.iloc[0]
            total_shares = float(orig['shares'])
            invested_amount = float(orig['invested_amount'])
            is_profit_position = orig['profit_position']

            if shares_sold > total_shares or shares_sold <= 0:
                raise ValueError("Invalid shares sold")

            # Calculate proportional invested for sold portion
            prop_invested = invested_amount * (shares_sold / total_shares)

            # Check if sell_amount is within 5% of original invested_amount
            within_5pct = abs(sell_amount - invested_amount) / \
                invested_amount <= 0.05 if invested_amount != 0 else False

            if within_5pct:
                closed_invested = sell_amount
                remaining_invested = invested_amount - sell_amount
                new_profit_position = True
            else:
                if is_profit_position:
                    closed_invested = 0.0
                    remaining_invested = invested_amount - sell_amount
                    new_profit_position = True
                else:
                    closed_invested = prop_invested
                    remaining_invested = invested_amount - prop_invested
                    new_profit_position = False

            # Round for storage
            closed_invested = round(closed_invested, 2)
            if 'remaining_invested' in locals():
                remaining_invested = round(remaining_invested, 2)

            # Insert into closed_positions
            conn.execute(sa.text("""
                INSERT INTO stocks.public.closed_positions 
                (account, ticker, buy_date, invested_amount, shares, sell_date, sell_amount, shares_retained, profit_position, original_position_id)
                VALUES (:account, :ticker, :buy_date, :invested_amount, :shares, :sell_date, :sell_amount, :shares_retained, :profit_position, :orig_id);
            """), {
                'account': orig['account'], 'ticker': orig['ticker'], 'buy_date': orig['buy_date'],
                'invested_amount': closed_invested, 'shares': float(shares_sold),
                'sell_date': sell_date, 'sell_amount': round(float(sell_amount), 2),
                'shares_retained': float(total_shares - shares_sold), 'profit_position': False,
                'orig_id': position_id
            })

            # If partial sell, create new open_position for remaining (use sell_date as buy_date for new)
            if shares_sold < total_shares:
                remaining_shares = float(total_shares - shares_sold)
                orig_pos_id = position_id if new_profit_position else None
                add_position(
                    account=orig['account'], ticker=orig['ticker'], buy_date=sell_date,
                    invested_amount=remaining_invested, shares=remaining_shares,
                    notes=orig['notes'], profit_position=new_profit_position,
                    original_position_id=orig_pos_id
                )

            # Delete original open_position
            conn.execute(sa.text("DELETE FROM stocks.public.open_positions WHERE id = :id;"), {
                         'id': position_id})


# Sell Recommendations section (updated with original_position_id from view)
st.header("Sell Recommendations")
sell_df = pd.read_sql(
    "SELECT * FROM stocks.public.sell_recommendations ORDER BY account, ticker;", engine)
if not sell_df.empty:
    st.dataframe(sell_df)

    # Options include position_id and buy_date for uniqueness
    rec_options = [
        f"{row['account']} - {row['ticker']} (ID: {row['original_position_id']}, Buy Date: {row['buy_date']})" for _, row in sell_df.iterrows()]
    selected_rec = st.selectbox(
        "Select Recommendation to Sell", options=rec_options, index=0)
    if st.button("Proceed to Sell for Selected"):
        if selected_rec:
            import re
            match = re.match(
                r"(.+) - (.+) \(ID: (\d+), Buy Date: (.+)\)", selected_rec)
            if match:
                account, ticker, pos_id, buy_date_str = match.groups()
                st.session_state.sell_account = account
                st.session_state.sell_ticker = ticker
                st.session_state.sell_position_id = int(pos_id)
                st.session_state.sell_buy_date = buy_date_str  # Optional, for display
                st.success(
                    f"Pre-filled Sell Position with {account} - {ticker} (ID: {pos_id}). Scroll down to complete.")
else:
    st.info("No sell recommendations at this time.")

# Sell Position section (always allow manual selection, pre-fill if from rec)
st.header("Sell Position")

# Fetch all open positions for manual selection
open_positions_df = pd.read_sql(
    "SELECT id, account, ticker, buy_date, invested_amount, shares, profit_position FROM stocks.public.open_positions ORDER BY account, ticker, buy_date;", engine)

if not open_positions_df.empty:
    # Create options
    pos_options = [
        f"{row['account']} - {row['ticker']} (ID: {row['id']}, Buy Date: {row['buy_date']})" for _, row in open_positions_df.iterrows()]

    # Find index if pre-filled
    pre_position_id = st.session_state.get('sell_position_id', None)
    default_index = 0
    if pre_position_id:
        for idx, row in open_positions_df.iterrows():
            if row['id'] == pre_position_id:
                default_index = idx
                break

    selected_pos = st.selectbox(
        "Select Position to Sell", pos_options, index=default_index)

    if selected_pos:
        import re
        match = re.match(
            r"(.+) - (.+) \(ID: (\d+), Buy Date: (.+)\)", selected_pos)
        if match:
            account, ticker, pos_id_str, buy_date_str = match.groups()
            position_id = int(pos_id_str)

            # Fetch details for the selected position
            orig = open_positions_df[open_positions_df['id']
                                     == position_id].iloc[0]
            total_shares = orig['shares']
            st.info(
                f"Selected Position: {account} - {ticker} | Buy Date: {buy_date_str} | Shares: {total_shares} | Invested: ${orig['invested_amount']:.2f} | Profit Position: {orig['profit_position']}")

            sell_date = st.date_input("Sell Date", value=date.today())
            sell_amount = st.number_input(
                "Sell Amount ($)", min_value=0.0, step=0.01)
            shares_sold = st.number_input(
                "Shares Sold", min_value=0.0, max_value=total_shares, step=0.01)
            if st.button("Confirm Sell"):
                if sell_amount > 0 and 0 < shares_sold <= total_shares:
                    try:
                        sell_position(position_id, sell_date,
                                      sell_amount, shares_sold)
                        st.success(
                            f"Sold {shares_sold} shares of {ticker} for ${sell_amount:.2f}")
                        # Clear pre-fill
                        st.session_state.pop('sell_account', None)
                        st.session_state.pop('sell_ticker', None)
                        st.session_state.pop('sell_position_id', None)
                        st.session_state.pop('sell_buy_date', None)
                        st.rerun()  # Refresh
                    except Exception as e:
                        st.error(f"Error: {e}")
                else:
                    st.error(
                        "Please enter valid sell amount and shares (cannot exceed total shares).")
else:
    st.info("No open positions available to sell.")

# Fetch exclusions


def fetch_exclusions():
    with engine.connect() as conn:
        df = pd.read_sql(
            "SELECT id, account, ticker, notes, created_at, updated_at FROM stocks.public.account_ticker_exclusions ORDER BY account, ticker;", conn)
    return df

# Add exclusion


def add_exclusion(account, ticker, notes):
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(sa.text("""
                INSERT INTO stocks.public.account_ticker_exclusions (account, ticker, notes) 
                VALUES (:account, :ticker, :notes) 
                ON CONFLICT (account, ticker) DO UPDATE SET notes = EXCLUDED.notes;
            """), {'account': account, 'ticker': ticker.upper(), 'notes': notes})

# Delete exclusion by ID


def delete_exclusion(exclusion_id):
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(sa.text("""
                DELETE FROM stocks.public.account_ticker_exclusions WHERE id = :id;
            """), {'id': exclusion_id})


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

# Refresh Current Prices button
st.header("Current Prices Management")
if st.button("Refresh Current Prices"):
    with st.spinner("Refreshing current prices..."):
        inserted = populate_current_prices()
        st.success(f"Updated current prices for {inserted} tickers")

# Manage Tickers section (for removal)
st.header("Manage Interested Tickers")
tickers_df = fetch_tickers()
if not tickers_df.empty:
    st.dataframe(tickers_df)

    # Delete tickers
    selected_tickers = st.multiselect(
        "Select Tickers to Delete", tickers_df['ticker'].tolist())
    if st.button("Delete Selected Tickers"):
        for ticker in selected_tickers:
            delete_ticker(ticker)
        st.success(f"Deleted {len(selected_tickers)} ticker(s)")
        st.rerun()  # Refresh the page to show updated list
else:
    st.info("No interested tickers yet.")

# Buy Initial Position Recommendations section
st.header("Buy Initial Position Recommendations")
buy_initial_df = pd.read_sql(
    "SELECT * FROM stocks.public.buy_initial_position_recommendations ORDER BY account, ticker;", engine)
if not buy_initial_df.empty:
    st.dataframe(buy_initial_df)

    # Interaction: Select a recommendation to pre-fill Add Position
    rec_options = [f"{row['account']} - {row['ticker']}" for _,
                   row in buy_initial_df.iterrows()]
    selected_rec = st.selectbox(
        "Select Recommendation to Buy Initial Position", options=rec_options, index=0)
    if st.button("Proceed to Add Position for Selected Initial"):
        if selected_rec:
            account, ticker = selected_rec.split(' - ')
            # Reusing the same keys for pre-fill
            st.session_state.buy_more_account = account
            st.session_state.buy_more_ticker = ticker
            st.success(
                f"Pre-filled Add Position with {account} - {ticker}. Scroll down to complete.")
else:
    st.info("No buy initial position recommendations at this time.")

# Buy More Recommendations section
st.header("Buy More Recommendations")
buy_more_df = pd.read_sql(
    "SELECT * FROM stocks.public.buy_more_recommendations ORDER BY account, ticker;", engine)
if not buy_more_df.empty:
    st.dataframe(buy_more_df)

    # Interaction: Select a recommendation to pre-fill Add Position
    rec_options = [f"{row['account']} - {row['ticker']}" for _,
                   row in buy_more_df.iterrows()]
    selected_rec = st.selectbox(
        "Select Recommendation to Buy More", options=rec_options, index=0)
    if st.button("Proceed to Add Position for Selected"):
        if selected_rec:
            account, ticker = selected_rec.split(' - ')
            st.session_state.buy_more_account = account
            st.session_state.buy_more_ticker = ticker
            st.success(
                f"Pre-filled Add Position with {account} - {ticker}. Scroll down to complete.")
else:
    st.info("No buy more recommendations at this time.")

# Add Position section
st.header("Add Position")
accounts_df = fetch_accounts()
if accounts_df.empty:
    st.info("No accounts available. Please add accounts to the database.")
else:
    ticker_options = tickers_df['ticker'].tolist()
    account_options = accounts_df['account_name'].tolist()

    # Pre-fill if from buy more or initial
    pre_ticker = st.session_state.get('buy_more_ticker', None)
    pre_account = st.session_state.get('buy_more_account', None)

    ticker_index = ticker_options.index(
        pre_ticker) if pre_ticker and pre_ticker in ticker_options else 0
    position_ticker = st.selectbox(
        "Ticker", ticker_options, index=ticker_index)

    account_index = account_options.index(
        pre_account) if pre_account and pre_account in account_options else 0
    selected_account = st.selectbox(
        "Account", account_options, index=account_index)

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
            # Clear pre-fill session state
            if 'buy_more_account' in st.session_state:
                del st.session_state.buy_more_account
            if 'buy_more_ticker' in st.session_state:
                del st.session_state.buy_more_ticker
            st.rerun()  # Auto-refresh to update recommendations
        else:
            st.error("Please fill in all required fields.")

# Manage Exclusions section
st.header("Manage Account-Ticker Exclusions")
exclusions_df = fetch_exclusions()
if not exclusions_df.empty:
    st.subheader("Current Exclusions")
    st.dataframe(exclusions_df)

    # Delete exclusions
    selected_ids = st.multiselect(
        "Select Exclusions to Delete (by ID)", exclusions_df['id'].tolist())
    if st.button("Delete Selected Exclusions"):
        for exclusion_id in selected_ids:
            delete_exclusion(exclusion_id)
        st.success(f"Deleted {len(selected_ids)} exclusion(s)")
        st.rerun()  # Refresh the page to show updated list

else:
    st.info("No exclusions yet.")

# Add new exclusion
st.subheader("Add Exclusion")
if accounts_df.empty:
    st.info("No accounts available. Please add accounts to the database.")
else:
    exclusion_account = st.selectbox("Account for Exclusion", account_options)
    exclusion_ticker = st.selectbox(
        "Ticker for Exclusion", tickers_df['ticker'].tolist())
    exclusion_notes = st.text_area("Exclusion Notes")
    if st.button("Add Exclusion"):
        if exclusion_account and exclusion_ticker:
            add_exclusion(exclusion_account, exclusion_ticker, exclusion_notes)
            st.success(
                f"Added exclusion for {exclusion_account} - {exclusion_ticker}")
            st.rerun()  # Refresh to show updated list
        else:
            st.error("Please select an account and ticker.")
