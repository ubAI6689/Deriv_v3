import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def create_database():
    # Get absolute path to database
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_dir = os.path.join(os.path.dirname(current_dir), 'database')
    db_path = os.path.join(db_dir, 'fraud_db.sqlite')
    
    # Create database directory if it doesn't exist
    os.makedirs(db_dir, exist_ok=True)
    
    # Create connection
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create tables with Deriv-specific fields
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS suspicious_accounts (
        client_id INTEGER PRIMARY KEY,
        detection_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        platform TEXT,          -- MT5, cTrader, Deriv X, etc.
        market_type TEXT,       -- Forex, Derived Indices, Stocks, etc.
        trade_type TEXT,        -- CFDs or Options
        country TEXT,           -- Client's country
        profession TEXT,        -- Client's profession
        annual_income FLOAT,    -- Declared annual income
        account_age_days INTEGER,  -- Days since account creation
        trading_volume FLOAT,   -- Total trading volume
        win_rate FLOAT,        -- Percentage of winning trades
        avg_trade_size FLOAT,  -- Average trade size
        trade_frequency TEXT,  -- High/Medium/Low
        preferred_markets TEXT, -- Comma-separated list of frequently traded markets
        kyc_status TEXT,       -- Verified/Pending/Failed
        device_count INTEGER,  -- Number of devices used
        ip_count INTEGER,      -- Number of unique IPs
        last_login TIMESTAMP,  -- Last login time
        risk_score FLOAT       -- Internal risk score (0-100)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS locked_accounts (
        client_id INTEGER PRIMARY KEY,
        lock_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        reason TEXT
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS financial_impact (
        client_id INTEGER PRIMARY KEY,
        fee FLOAT,
        amount FLOAT,
        payment_method TEXT,
        total_deposits FLOAT,
        total_withdrawals FLOAT,
        deposit_frequency TEXT,     -- High/Medium/Low
        withdrawal_frequency TEXT,  -- High/Medium/Low
        avg_deposit_size FLOAT,
        avg_withdrawal_size FLOAT,
        last_deposit_date TIMESTAMP,
        last_withdrawal_date TIMESTAMP,
        deposit_methods TEXT,       -- Comma-separated list
        withdrawal_methods TEXT,    -- Comma-separated list
        chargeback_count INTEGER,
        failed_deposits INTEGER
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS suspicious_groups (
        region TEXT,
        payment_method TEXT,
        client_count INTEGER,
        client_ids TEXT,
        platform TEXT
    )
    ''')

    # Add new table for AI decisions
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS ai_decisions (
        client_id INTEGER PRIMARY KEY,
        decision_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        risk_level TEXT,
        recommended_action TEXT,
        justification TEXT,
        investigation_notes TEXT
    )
    ''')

    # Add trading_activity table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS trading_activity (
        client_id INTEGER,
        trade_date TIMESTAMP,
        trade_type TEXT,        -- Buy/Sell
        market TEXT,
        position_size FLOAT,
        leverage INTEGER,
        profit_loss FLOAT,
        duration_minutes INTEGER,
        entry_price FLOAT,
        exit_price FLOAT,
        strategy_type TEXT,     -- Day Trading, Scalping, etc.
        PRIMARY KEY (client_id, trade_date)
    )
    ''')

    # Generate mock data based on Deriv's actual platforms and markets
    platforms = ['Deriv MT5', 'Deriv cTrader', 'Deriv X', 'Deriv GO', 'Deriv Trader']
    markets = ['Forex', 'Derived Indices', 'Stocks', 'Cryptocurrencies', 'Commodities']
    payment_methods = ['Credit Card', 'Crypto', 'Bank Transfer']
    regions = ['EU', 'ASIA', 'NA']  # Based on Deriv's global presence

    # Generate enhanced mock data
    professions = ['Engineer', 'Business Owner', 'Student', 'Trader', 'IT Professional', 'Retired']
    countries = ['UK', 'Germany', 'Singapore', 'Australia', 'UAE', 'Hong Kong']
    trade_frequencies = ['High', 'Medium', 'Low']
    kyc_statuses = ['Verified', 'Pending', 'Failed']
    strategy_types = ['Day Trading', 'Scalping', 'Swing Trading', 'Position Trading']

    # Generate suspicious accounts with enhanced data
    suspicious_clients = pd.DataFrame({
        'client_id': range(1001, 1011),
        'detection_date': [datetime.now() - timedelta(hours=np.random.randint(1, 48)) 
                          for _ in range(10)],
        'platform': np.random.choice(platforms, 10),
        'market_type': np.random.choice(markets, 10),
        'trade_type': np.random.choice(['CFDs', 'Options'], 10),
        'country': np.random.choice(countries, 10),
        'profession': np.random.choice(professions, 10),
        'annual_income': np.random.uniform(50000, 500000, 10),
        'account_age_days': np.random.randint(1, 365, 10),
        'trading_volume': np.random.uniform(10000, 1000000, 10),
        'win_rate': np.random.uniform(0.3, 0.7, 10),
        'avg_trade_size': np.random.uniform(100, 5000, 10),
        'trade_frequency': np.random.choice(trade_frequencies, 10),
        'preferred_markets': [','.join(np.random.choice(markets, 3)) for _ in range(10)],
        'kyc_status': np.random.choice(kyc_statuses, 10),
        'device_count': np.random.randint(1, 5, 10),
        'ip_count': np.random.randint(1, 10, 10),
        'last_login': [datetime.now() - timedelta(hours=np.random.randint(1, 24)) 
                      for _ in range(10)],
        'risk_score': np.random.uniform(60, 95, 10)  # High risk scores for suspicious accounts
    })

    # Enhanced financial impact data
    financial_data = pd.DataFrame({
        'client_id': range(1001, 1011),
        'fee': np.random.uniform(10, 100, 10),
        'amount': np.random.uniform(1000, 10000, 10),
        'payment_method': np.random.choice(payment_methods, 10),
        'total_deposits': np.random.uniform(5000, 50000, 10),
        'total_withdrawals': np.random.uniform(1000, 40000, 10),
        'deposit_frequency': np.random.choice(['High', 'Medium', 'Low'], 10),
        'withdrawal_frequency': np.random.choice(['High', 'Medium', 'Low'], 10),
        'avg_deposit_size': np.random.uniform(500, 5000, 10),
        'avg_withdrawal_size': np.random.uniform(500, 5000, 10),
        'last_deposit_date': [datetime.now() - timedelta(days=np.random.randint(1, 30)) 
                             for _ in range(10)],
        'last_withdrawal_date': [datetime.now() - timedelta(days=np.random.randint(1, 30)) 
                                for _ in range(10)],
        'deposit_methods': [','.join(np.random.choice(payment_methods, 2)) for _ in range(10)],
        'withdrawal_methods': [','.join(np.random.choice(payment_methods, 2)) for _ in range(10)],
        'chargeback_count': np.random.randint(0, 3, 10),
        'failed_deposits': np.random.randint(0, 5, 10)
    })

    # Generate trading activity data
    trading_records = []
    for client_id in range(1001, 1011):
        num_trades = np.random.randint(10, 50)
        for _ in range(num_trades):
            trade_date = datetime.now() - timedelta(days=np.random.randint(1, 30))
            trading_records.append({
                'client_id': client_id,
                'trade_date': trade_date,
                'trade_type': np.random.choice(['Buy', 'Sell']),
                'market': np.random.choice(markets),
                'position_size': np.random.uniform(100, 10000),
                'leverage': np.random.choice([5, 10, 20, 50, 100]),
                'profit_loss': np.random.uniform(-1000, 1000),
                'duration_minutes': np.random.randint(1, 1440),
                'entry_price': np.random.uniform(10, 1000),
                'exit_price': np.random.uniform(10, 1000),
                'strategy_type': np.random.choice(strategy_types)
            })
    
    trading_activity = pd.DataFrame(trading_records)

    # Save to database
    suspicious_clients.to_sql('suspicious_accounts', conn, if_exists='replace', index=False)
    financial_data.to_sql('financial_impact', conn, if_exists='replace', index=False)
    trading_activity.to_sql('trading_activity', conn, if_exists='replace', index=False)

    # 2. Locked accounts with Deriv-specific reasons
    reasons = [
        'Suspicious rapid deposits/withdrawals',
        'Minimal trading activity',
        'Pattern of small trades without profit/loss',
        'Multiple accounts detected'
    ]
    locked_clients = pd.DataFrame({
        'client_id': range(1001, 1008),
        'lock_date': [datetime.now() - timedelta(hours=np.random.randint(1, 24)) 
                     for _ in range(7)],
        'reason': np.random.choice(reasons, 7)
    })
    locked_clients.to_sql('locked_accounts', conn, if_exists='replace', index=False)

    # 4. Suspicious groups by platform and region
    groups_data = pd.DataFrame({
        'region': ['EU', 'ASIA', 'NA', 'EU'],
        'payment_method': payment_methods[:3] + ['Crypto'],
        'client_count': [3, 2, 2, 3],
        'client_ids': [
            '[1001,1002,1003]',
            '[1004,1005]',
            '[1006,1007]',
            '[1008,1009,1010]'
        ],
        'platform': ['Deriv MT5', 'Deriv cTrader', 'Deriv X', 'Deriv Trader']
    })
    groups_data.to_sql('suspicious_groups', conn, if_exists='replace', index=False)

    conn.commit()
    conn.close()

if __name__ == '__main__':
    create_database()
    print("Database initialized successfully!") 