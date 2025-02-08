import sqlite3
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
fake = Faker()
import pycountry

# Get absolute path to database
current_dir = os.path.dirname(os.path.abspath(__file__))
db_dir = os.path.join(os.path.dirname(current_dir), 'database')
db_path = os.path.join(db_dir, 'deriv_db.sqlite')

# Create connection
conn = sqlite3.connect(db_path)

# Define constants for data generation
platforms = ['Deriv MT5', 'Deriv cTrader', 'Deriv X', 'Deriv GO', 'Deriv Trader']
markets = ['Forex', 'Stocks', 'Cryptocurrencies', 'Commodities']
payment_methods = ['Credit & debit cards', 'Online banking', 'Mobile Payments', 'E-wallets', 'Cryptocurrencies', 'On-ramp / Off-ramp', 'Voucher']
professions = ['Engineer', 'Business Owner', 'Student', 'Trader', 'IT Professional', 'Retired']
countries = [country.name for country in pycountry.countries]
kyc_statuses = ['verified', 'pending', 'rejected']

trade_pairs = {
    "Forex": [
        "EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "AUD/USD",
        "USD/CAD", "NZD/USD", "EUR/GBP", "EUR/JPY", "GBP/JPY",
        "EUR/CHF", "AUD/JPY", "CAD/JPY", "GBP/CHF", "USD/SGD"
    ],
    "Stocks": [
    "AAPL/USD", "MSFT/USD", "GOOGL/USD", "AMZN/USD", "META/USD",
    "TSLA/USD", "NVDA/USD", "JPM/USD", "BAC/USD", "WMT/USD",
        "PG/USD", "JNJ/USD", "V/USD", "MA/USD", "DIS/USD"
    ],
    "Cryptocurrencies": [
        "BTC/USD", "ETH/USD", "BNB/USD", "XRP/USD", "ADA/USD",
        "DOGE/USD", "DOT/USD", "LINK/USD", "LTC/USD", "BCH/USD",
        "BTC/EUR", "ETH/EUR", "BTC/GBP", "ETH/GBP", "BTC/JPY"
    ],
    "Commodities": [
        "XAUUSD", "XAGUSD", "XPTUSD", "XPDUSD",  # Gold, Silver, Platinum, Palladium
        "CL/USD",  # Crude Oil
        "NG/USD",  # Natural Gas
        "HG/USD",  # Copper
        "ZC/USD",  # Corn
        "ZW/USD",  # Wheat
        "KC/USD",  # Coffee
        "CT/USD",  # Cotton
        "CC/USD",  # Cocoa
        "ZS/USD",  # Soybeans
        "SB/USD",  # Sugar
        "BZ/USD"   # Brent Crude
    ]
}


# Set random seed for reproducibility
np.random.seed(42)

# Generate user data with random values

additional_users = 100
user_accounts = []

# Get maximum transaction ID from database
cursor = conn.cursor()
cursor.execute("SELECT MAX(user_id) FROM user")
max_user_id = cursor.fetchone()[0]
user_id_initial = max_user_id if max_user_id is not None else 1001

user_ids = range(user_id_initial, user_id_initial + additional_users)

for user_id in user_ids:
    user_name = fake.name()
    user_email = f'{user_name.lower().replace(" ", ".")}{np.random.choice(["@gmail.com", "@yahoo.com", "@hotmail.com", "@outlook.com"])}'
    user_accounts.append({
        'user_id': user_id,
        'user_name': user_name,
        'user_email': user_email,
        'user_kycstatus': np.random.choice(kyc_statuses),
        'user_status': 'active',
        'user_country': np.random.choice(countries),
        'user_profession': np.random.choice(professions),
        'user_income': np.random.randint(1000, 200000),
        'user_createdat': datetime.now() - timedelta(days=np.random.randint(1, 365)),
        'user_lastlogin': datetime.now() - timedelta(hours=np.random.randint(1, 24)),
    })

user = pd.DataFrame(user_accounts)

# Generate transaction data with random values
transaction_records = []

# Get maximum transaction ID from database
cursor = conn.cursor()
cursor.execute("SELECT MAX(transaction_id) FROM user_transaction")
max_transaction_id = cursor.fetchone()[0]
transaction_id = max_transaction_id if max_transaction_id is not None else 0

for user_id in user_ids:
    num_transactions = np.random.randint(5, 20)
    
    for id in range(num_transactions):
        days = np.random.randint(1, 7) # max 7 days ago
        hours = np.random.randint(1, 24)
        minutes = np.random.randint(1, 60)
        seconds = np.random.randint(1, 60)
        transaction_id += 1
        transaction_records.append({
            'transaction_id': transaction_id,
            'transaction_userid': user_id,
            'transaction_datetime': datetime.now() - timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds),
            'transaction_type': np.random.choice(['deposit', 'withdrawal']),
            'transaction_amount': round(np.random.uniform(1, 10000), 2),
            'transaction_paymentmethod': np.random.choice(payment_methods)
        })
transactions = pd.DataFrame(transaction_records)

# Generate trade data with random values
trade_records = []
# Get maximum trade ID from database
cursor = conn.cursor()
cursor.execute("SELECT MAX(trade_id) FROM user_trading")
max_trade_id = cursor.fetchone()[0]

trade_id = max_trade_id if max_trade_id is not None else 0

for user_id in user_ids:
    num_trades = np.random.choice([
        np.random.randint(1, 10),      # Few trades (40% chance)
        np.random.randint(10, 100),    # Moderate trades (30% chance) 
        np.random.randint(100, 3000)   # Many trades (30% chance)
    ], p=[0.6, 0.35, 0.05])
    for _ in range(num_trades):
        days = np.random.randint(1, 7) # max 7 days ago
        hours = np.random.randint(1, 24)
        minutes = np.random.randint(1, 60)
        seconds = np.random.randint(1, 60)
        open_price = np.random.uniform(1, 1000)
        volume = np.random.uniform(0.1, 10)
        close_price = np.random.uniform(0.5, 1.5) * open_price
        trade_cost = volume * open_price
        trade_profit = (volume * close_price) - trade_cost

        trade_market = np.random.choice(markets)
        trade_id += 1
        
        trade_records.append({
            'trade_id': trade_id,
            'trade_userid': user_id,
            'trade_timestamp': datetime.now() - timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds),
            'trade_durationminutes': np.random.randint(1, 43200), # Random between 1 min to 1 month (43200 minutes)
            'trade_market': trade_market,
            'trade_pair': np.random.choice(trade_pairs[trade_market]),
            'trade_cost': trade_cost,
            'trade_profit': trade_profit,
            'trade_profitratio': trade_profit / trade_cost
        })
trades = pd.DataFrame(trade_records)

# Save to database
user.to_sql('user', conn, if_exists='append', index=False)
transactions.to_sql('user_transaction', conn, if_exists='append', index=False)

# Calculate derived columns
# Calculate time windows
# Sort trades by timestamp first
trades = trades.sort_values('trade_timestamp')

# Calculate cumulative counts for each user within time windows
trades['trades_past_month'] = trades.groupby('trade_userid').apply(
    lambda x: x.rolling(
        window=f'30D', 
        on='trade_timestamp'
    )['trade_timestamp'].count()
).reset_index(level=0, drop=True)

trades['trades_past_week'] = trades.groupby('trade_userid').apply(
    lambda x: x.rolling(
        window=f'7D',
        on='trade_timestamp'
    )['trade_timestamp'].count()
).reset_index(level=0, drop=True)

trades['trades_past_day'] = trades.groupby('trade_userid').apply(
    lambda x: x.rolling(
        window=f'1D',
        on='trade_timestamp'
    )['trade_timestamp'].count()
).reset_index(level=0, drop=True)

trades['trades_past_hour'] = trades.groupby('trade_userid').apply(
    lambda x: x.rolling(
        window=f'1H',
        on='trade_timestamp'
    )['trade_timestamp'].count()
).reset_index(level=0, drop=True)

# Fill NaN values with 0
trades[['trades_past_month', 'trades_past_week', 'trades_past_day', 'trades_past_hour']] = trades[
    ['trades_past_month', 'trades_past_week', 'trades_past_day', 'trades_past_hour']
].fillna(0).astype(int)

# Save to database with new columns
trades.to_sql('user_trading', conn, if_exists='append', index=False)


conn.commit()
conn.close()

print("Data generated successfully")