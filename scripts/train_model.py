# scripts/train_model.py
import os
import sys
import sqlite3
import pandas as pd
from sklearn.model_selection import train_test_split

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from src.utils.feature_engineering import FeatureEngineer
from src.models.fraud_detector import FraudDetector

def get_training_data():
    """Get training data from database"""
    conn = sqlite3.connect('../database/deriv_db.sqlite')
    
    # Get fraudulent samples
    fraudulent_query = """
        SELECT 
            t.*,
            tr.transaction_datetime,
            tr.transaction_type,
            tr.transaction_amount,
            tr.transaction_paymentmethod
        FROM user_trading t
        JOIN user_transaction tr ON t.trade_userid = tr.transaction_userid
        WHERE t.trade_userid IN (
            SELECT user_id FROM user 
            WHERE user_status IN ('locked', 'monitor')
        )
    """
    fraudulent_samples = pd.read_sql(fraudulent_query, conn)
    
    # Get non-fraudulent samples
    non_fraudulent_query = """
        SELECT 
            t.*,
            tr.transaction_datetime,
            tr.transaction_type,
            tr.transaction_amount,
            tr.transaction_paymentmethod
        FROM user_trading t
        JOIN user_transaction tr ON t.trade_userid = tr.transaction_userid
        WHERE t.trade_userid IN (
            SELECT user_id FROM user 
            WHERE user_status = 'active'
        )
        LIMIT ?
    """
    non_fraudulent_samples = pd.read_sql(
        non_fraudulent_query, 
        conn, 
        params=[len(fraudulent_samples)]
    )
    
    conn.close()
    
    return fraudulent_samples, non_fraudulent_samples

def main():
    # Create models directory if it doesn't exist
    model_dir = os.path.join(project_root, 'models')
    os.makedirs(model_dir, exist_ok=True)
    
    # Get training data
    print("Getting training data...")
    fraudulent_samples, non_fraudulent_samples = get_training_data()
    
    # Prepare features
    print("Engineering features...")
    feature_engineer = FeatureEngineer()
    
    # Process fraudulent samples
    fraud_features = []
    for user_id in fraudulent_samples['trade_userid'].unique():
        user_trades = fraudulent_samples[
            fraudulent_samples['trade_userid'] == user_id
        ]
        user_transactions = fraudulent_samples[
            fraudulent_samples['trade_userid'] == user_id
        ]
        features = feature_engineer.calculate_user_features(
            user_trades, user_transactions
        )
        fraud_features.append(features)
    
    # Process non-fraudulent samples
    non_fraud_features = []
    for user_id in non_fraudulent_samples['trade_userid'].unique():
        user_trades = non_fraudulent_samples[
            non_fraudulent_samples['trade_userid'] == user_id
        ]
        user_transactions = non_fraudulent_samples[
            non_fraudulent_samples['trade_userid'] == user_id
        ]
        features = feature_engineer.calculate_user_features(
            user_trades, user_transactions
        )
        non_fraud_features.append(features)
    
    # Combine features and create labels
    X = pd.concat(fraud_features + non_fraud_features, ignore_index=True)
    y = [1] * len(fraud_features) + [0] * len(non_fraud_features)
    
    # Train model
    print("Training model...")
    model = FraudDetector()
    model.train(X, y)
    
    # Save model
    model_path = os.path.join(model_dir, 'fraud_detector.joblib')
    print(f"Saving model to {model_path}")
    model.save_model(model_path)
    
    print("Done!")

if __name__ == "__main__":
    main()