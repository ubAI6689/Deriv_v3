import sqlite3
import os

def create_database():
    # Get absolute path to database
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_dir = os.path.join(os.path.dirname(current_dir), 'database')
    db_path = os.path.join(db_dir, 'deriv_db.sqlite')
    
    # Create database directory if it doesn't exist
    os.makedirs(db_dir, exist_ok=True)
    
    # Create connection
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create tables
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS user (
        user_id INTEGER PRIMARY KEY,
        user_name TEXT NOT NULL,
        user_email TEXT NOT NULL,
        user_kycstatus TEXT NOT NULL CHECK (user_kycstatus IN ('verified', 'pending', 'rejected')),
        user_status TEXT NOT NULL CHECK (user_status IN ('active', 'monitor','locked','disabled')),
        user_country TEXT NOT NULL,
        user_profession TEXT NOT NULL,
        user_income FLOAT,
        user_createdat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        user_lastlogin TIMESTAMP
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS user_transaction (
        transaction_id INTEGER PRIMARY KEY,
        transaction_userid INTEGER NOT NULL REFERENCES user(id),
        transaction_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        transaction_type TEXT NOT NULL CHECK (transaction_type IN ('deposit', 'withdrawal')),
        transaction_amount FLOAT NOT NULL,
        transaction_paymentmethod FLOAT NOT NULL
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS user_trading (
        trade_id INTEGER PRIMARY KEY,
        trade_userid INTEGER NOT NULL REFERENCES user(id),
        trade_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        trade_durationminutes FLOAT NOT NULL,
        trade_market TEXT NOT NULL,
        trade_pair TEXT,
        trade_cost FLOAT NOT NULL,
        trade_profit FLOAT,
        trade_profitratio FLOAT,
        trades_past_month INTEGER,
        trades_past_week INTEGER,
        trades_past_day INTEGER,
        trades_past_hour INTEGER
    )
    ''')


    cursor.execute('''
    CREATE TABLE IF NOT EXISTS fraud (
        fraud_id INTEGER PRIMARY KEY,
        fraud_userid INTEGER NOT NULL REFERENCES user(id) UNIQUE,
        fraud_detecteddate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        fraud_risk TEXT NOT NULL CHECK (fraud_risk IN ('medium', 'high')),
        fraud_clarificationemaildate TIMESTAMP,
        fraud_resolved BOOLEAN NOT NULL

    )
    ''')


    conn.commit()
    conn.close()

if __name__ == '__main__':
    create_database()
    print("Database initialized successfully!") 