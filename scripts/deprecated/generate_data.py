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
markets = ['Forex', 'Derived Indices', 'Stocks', 'Cryptocurrencies', 'Commodities']
payment_methods = ['Credit Card', 'Crypto', 'Bank Transfer']
professions = ['Engineer', 'Business Owner', 'Student', 'Trader', 'IT Professional', 'Retired']
countries = [country.name for country in pycountry.countries]
kyc_statuses = ['verified', 'pending', 'rejected']
currency_pairs = ['EUR', 'USD', 'GBP', 'JPY']

# Set random seed for reproducibility
np.random.seed(42)

# Generate user data with random values

num_users = 10
user_accounts = []
user_ids = range(1001, 1001 + num_users)

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
    num_trades = np.random.randint(10, 50)
    for _ in range(num_trades):
        days = np.random.randint(1, 7) # max 7 days ago
        hours = np.random.randint(1, 24)
        minutes = np.random.randint(1, 60)
        seconds = np.random.randint(1, 60)
        trade_type = np.random.choice(['buy', 'sell'])
        open_price = np.random.uniform(10, 1000)
        is_closed = trade_type == 'sell'
        current_price = np.random.uniform(10, 1000)
        close_price = current_price if is_closed else None
        volume = np.random.uniform(0.1, 10.0)
        profit = (current_price - open_price) if is_closed else None
        
    
        pair1, pair2 = np.random.choice(currency_pairs, size=2, replace=False)
        trade_id += 1
        trade_records.append({
            'trade_id': trade_id,
            'trade_userid': user_id,
            'trade_timestamp': datetime.now() - timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds),
            'trade_type': np.random.choice(['buy', 'sell']),
            'trade_market': np.random.choice(markets),
            'trade_pair': f"{pair1}/{pair2}",
            'trade_volume': volume,
            'trade_openprice': open_price,
            'trade_cost': volume * open_price,
            'trade_currentprice': current_price,
            'trade_closeprice': close_price,
            'trade_profit': profit,
        })
trades = pd.DataFrame(trade_records)

# Save to database
user.to_sql('user', conn, if_exists='append', index=False)
transactions.to_sql('user_transaction', conn, if_exists='append', index=False)
trades.to_sql('user_trading', conn, if_exists='append', index=False)

conn.commit()
conn.close()
