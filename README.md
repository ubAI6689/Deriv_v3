# Deriv Fraud Detection System

A machine learning-based system for detecting fraudulent trading patterns and suspicious user behavior in Deriv's trading platforms.

## Overview

This system monitors user trading activities and transactions to identify potential fraud patterns such as:
- Deposit and withdrawal without trading
- Quick succession of minimal trades
- Suspicious trading patterns
- Unusual transaction behaviors

## Features

- Real-time fraud detection using Random Forest model
- User behavior analysis
- Transaction pattern monitoring
- Trading activity analysis
- High-risk user identification
- Financial impact tracking
- Automated account locking for suspicious activities
- Email notification system

## Project Structure

```
deriv_fraud_detection/
├── dashboard/              # Streamlit dashboard
│   ├── app_v3.py          # Main dashboard application
│   └── update_db.py       # Database update utilities
├── database/              # SQLite database
│   └── deriv_db.sqlite    
├── models/               # Trained ML models
│   └── fraud_detector.joblib
├── src/                  # Source code
│   ├── models/          # ML model definitions
│   │   └── fraud_detector.py
│   └── utils/           # Utility modules
│       └── feature_engineering.py
└── scripts/             # Scripts for data generation and maintenance
    ├── generate_data_v3.py
    ├── init_database_v3.py
    └── train_model.py
```

## Setup

1. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # Unix/macOS
.\venv\Scripts\activate   # Windows
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Initialize database:
```bash
python scripts/init_database_v3.py
```

4. Generate sample data:
```bash
python scripts/generate_data_v3.py
```

5. Train the model:
```bash
python scripts/train_model.py
```

## Running the Dashboard

Start the Streamlit dashboard:
```bash
cd dashboard
streamlit run app_v3.py
```

Access the dashboard at http://localhost:8502

## Key Components

### Fraud Detection Model
- Random Forest classifier
- Feature engineering for trading patterns
- Real-time prediction capabilities

### Feature Engineering
- Time-based features
- Trading pattern analysis
- Transaction behavior metrics
- User activity profiling

### Dashboard Features
- High-risk user monitoring
- Transaction analysis
- Trading pattern visualization
- Financial impact tracking
- User risk assessment

## Database Schema

### User Table
- User identification
- KYC status
- Account details
- Geographic information

### Trading Table
- Trade history
- Market information
- Profit/loss tracking
- Trading patterns

### Transaction Table
- Deposits and withdrawals
- Payment methods
- Transaction timing
- Amount tracking

### Fraud Table
- Risk assessment
- Detection timestamps
- Resolution status
- Email communication tracking

## Configuration

The system uses various configuration settings that can be modified:
- Risk thresholds
- Trading pattern parameters
- Transaction monitoring rules
- Email notification settings

## Development

1. Feature Engineering
```python
from src.utils.feature_engineering import FeatureEngineer

engineer = FeatureEngineer()
features = engineer.calculate_user_features(trades_df, transactions_df)
```

2. Model Prediction
```python
from src.models.fraud_detector import FraudDetector

model = FraudDetector.load_model('models/fraud_detector.joblib')
prediction = model.predict(features)
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

Proprietary - All rights reserved

## Authors

- Syed Mohamed Syakir

## Acknowledgments

- Deriv Anti-Fraud Team
- Trading Pattern Analysis Team
- Risk Management Department
