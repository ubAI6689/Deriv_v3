# src/utils/feature_engineering.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class FeatureEngineer:
    def __init__(self):
        """Initialize the FeatureEngineer class"""
        pass

    def _calculate_time_based_features(self, df, timestamp_col):
        """Calculate time-based features from timestamp data"""
        df = df.copy()
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        
        # Sort by timestamp
        df = df.sort_values(timestamp_col)
        
        if len(df) > 1:
            # Time differences between consecutive actions
            df['time_diff'] = df[timestamp_col].diff().dt.total_seconds()
        else:
            df['time_diff'] = 0
            
        # Time of day features
        df['hour'] = df[timestamp_col].dt.hour
        df['is_weekend'] = df[timestamp_col].dt.weekday.isin([5, 6]).astype(int)
        
        return df

    def _calculate_trade_features(self, trades_df):
        """Calculate trading-related features"""
        if trades_df.empty:
            return pd.Series({
                'trade_count': 0,
                'avg_trade_duration': 0,
                'avg_trade_cost': 0,
                'avg_trade_profit': 0,
                'total_trade_volume': 0,
                'profit_ratio': 0,
                'trades_past_month_avg': 0,
                'trades_past_week_avg': 0,
                'trades_past_day_avg': 0,
                'trades_past_hour_avg': 0,
                'weekend_trade_ratio': 0
            })

        trades_df = self._calculate_time_based_features(trades_df, 'trade_timestamp')
        
        features = {
            'trade_count': len(trades_df),
            'avg_trade_duration': trades_df['trade_durationminutes'].mean(),
            'avg_trade_cost': trades_df['trade_cost'].mean(),
            'avg_trade_profit': trades_df['trade_profit'].mean(),
            'total_trade_volume': trades_df['trade_cost'].sum(),
            'profit_ratio': trades_df['trade_profitratio'].mean(),
            'trades_past_month_avg': trades_df['trades_past_month'].mean(),
            'trades_past_week_avg': trades_df['trades_past_week'].mean(),
            'trades_past_day_avg': trades_df['trades_past_day'].mean(),
            'trades_past_hour_avg': trades_df['trades_past_hour'].mean(),
            'weekend_trade_ratio': trades_df['is_weekend'].mean()
        }
        
        return pd.Series(features)

    def _calculate_transaction_features(self, transactions_df):
        """Calculate transaction-related features"""
        if transactions_df.empty:
            return pd.Series({
                'deposit_count': 0,
                'withdrawal_count': 0,
                'total_deposit_amount': 0,
                'total_withdrawal_amount': 0,
                'avg_deposit_amount': 0,
                'avg_withdrawal_amount': 0,
                'deposit_withdrawal_ratio': 0,
                'weekend_transaction_ratio': 0
            })

        transactions_df = self._calculate_time_based_features(
            transactions_df, 'transaction_datetime'
        )
        
        deposits = transactions_df[transactions_df['transaction_type'] == 'deposit']
        withdrawals = transactions_df[transactions_df['transaction_type'] == 'withdrawal']
        
        features = {
            'deposit_count': len(deposits),
            'withdrawal_count': len(withdrawals),
            'total_deposit_amount': deposits['transaction_amount'].sum() if not deposits.empty else 0,
            'total_withdrawal_amount': withdrawals['transaction_amount'].sum() if not withdrawals.empty else 0,
            'avg_deposit_amount': deposits['transaction_amount'].mean() if not deposits.empty else 0,
            'avg_withdrawal_amount': withdrawals['transaction_amount'].mean() if not withdrawals.empty else 0,
            'deposit_withdrawal_ratio': len(deposits) / max(1, len(withdrawals)),
            'weekend_transaction_ratio': transactions_df['is_weekend'].mean()
        }
        
        return pd.Series(features)

    def calculate_user_features(self, trades_df, transactions_df):
        """Calculate all features for fraud detection"""
        trade_features = self._calculate_trade_features(trades_df)
        transaction_features = self._calculate_transaction_features(transactions_df)
        
        all_features = pd.concat([trade_features, transaction_features])
        
        if not trades_df.empty and not transactions_df.empty:
            all_features['trade_to_deposit_ratio'] = (
                trade_features['total_trade_volume'] / 
                max(1, transaction_features['total_deposit_amount'])
            )
        else:
            all_features['trade_to_deposit_ratio'] = 0
        
        return pd.DataFrame([all_features])