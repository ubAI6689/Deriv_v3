# .aidigestignore

```
# Python virtual environment
venv/
.env
.venv
env/
ENV/

# Python cache files
__pycache__/
*.py[cod]
*$py.class
.pytest_cache/
.coverage
htmlcov/

# Distribution / packaging
dist/
build/
*.egg-info/

# IDEs and editors
.idea/
.vscode/
*.swp
*.swo
.DS_Store

# Logs
*.log
logs/

# Local environment files
.env.local
.env.*.local

# Temporary files
*.tmp
*.bak
*.swp
*~

# Jupyter Notebook
.ipynb_checkpoints
*.ipynb_checkpoints/

# csv
*.csv

# ipynb
*.ipynb
```

# .gitignore

```
# Python virtual environment
venv/
.env
.venv
env/
ENV/

# Python cache files
__pycache__/
*.py[cod]
*$py.class
.pytest_cache/
.coverage
htmlcov/

# Distribution / packaging
dist/
build/
*.egg-info/

# IDEs and editors
.idea/
.vscode/
*.swp
*.swo
.DS_Store

# Logs
*.log
logs/

# Local environment files
.env.local
.env.*.local

# Temporary files
*.tmp
*.bak
*.swp
*~

# Jupyter Notebook
.ipynb_checkpoints
*.ipynb_checkpoints/
```

# analysis_output/20250209/004836/1001.json

```json
{"analysis_output": "\`\`\`json\n{\n    \"risk_level\": \"High\",\n    \"justification\": [\n        {\n            \"dataset\": \"trades_past_hour\",\n            \"row_number\": 70,\n            \"explanation\": \"The user has made a significant number of trades (497) in a short period of time (<1 hour), indicating potential fraud.\"\n        },\n        {\n            \"dataset\": \"trade_durationminutes\",\n            \"row_number\": [68, 69],\n            \"explanation\": \"The trade duration is extremely short (<15 minutes), which could be an attempt to manipulate the system or avoid losses.\"\n        }\n    ]\n}\n\`\`\`\n\nNote: The justification section includes two datasets (`trades_past_hour` and `trade_durationminutes`) and their corresponding row numbers, as well as explanations for why these data points indicate potential fraud.", "user_id": 1001}
```

# analysis_output/20250209/010603/1001.json

```json
{"analysis_output": "\`\`\`json\n{\n    \"risk_level\": \"High\",\n    \"justification\": [\n        {\n            \"dataset\": \"trades_past_day\",\n            \"row_number\": 72,\n            \"explanation\": \"User made a large number of trades (buy & sell) in a short period of time, indicating potential fraudulent activity.\"\n        },\n        {\n            \"dataset\": \"trade_durationminutes\",\n            \"row_number\": [71, 72],\n            \"explanation\": \"Short trade duration (less than 1 minute) and high trading volume indicate suspicious behavior.\"\n        }\n    ]\n}\n\`\`\`\n\nThis response indicates a High risk level due to the user's activity on trade number 72, which shows a large number of trades made in a short period of time. Additionally, the same row also has a very short trade duration (less than 1 minute), further supporting this conclusion.\n\nThe justification is based on data from `trades_past_day` and `trade_durationminutes`, specifically rows 72 and [71, 72] respectively. These datasets suggest that the user may be engaging in suspicious behavior, such as pretending to trade with short trades in quick succession using minimal amounts without significant profit or loss.\n\nNote: The risk level could also be classified as Medium if only one of these conditions was met, but given the combination of factors, High is the most appropriate classification.", "user_id": 1001}
```

# analysis_output/20250209/033932/1001.json

```json
{"user_id": 1001, "model_output": {"error": "Analysis error: 'trade_userid'"}}
```

# analysis_output/20250209/034105/1001.json

```json
{"user_id": 1001, "model_output": {"error": "Analysis error: 'trade_userid'"}}
```

# analysis_output/20250209/034226/1001.json

```json
{"user_id": 1001, "model_output": {"error": "Analysis error: 'trade_userid'"}}
```

# analysis_output/20250209/034307/1001.json

```json
{"user_id": 1001, "model_output": {"error": "Analysis error: local variable 'trades_df' referenced before assignment"}}
```

# analysis_output/20250209/040818/1001.json

```json
{"user_id": 1001, "model_output": {"error": "Analysis error: 'Rolling' object has no attribute 'size'"}}
```

# analysis_output/20250209/040925/1001.json

```json
{"user_id": 1001, "model_output": {"error": "Analysis error: 'Rolling' object has no attribute 'size'"}}
```

# analysis_output/20250209/041453/1001.json

```json
{"user_id": 1001, "model_output": {"fraud_probability": 0.39, "risk_level": "medium"}}
```

# analysis_output/high_risk_sample_2.json

```json
{
    "user_id": 1003,
    "risk_level": "high",
    "justification": "The user has made 10 trades in the past hour on 2025-02-02 03:49:56, with a total volume of 5,000. This is highly unusual and suspicious."
}
```

# analysis_output/high_risk_sample_3.json

```json
{
    "user_id": 1050,
    "risk_level": "high",
    "justification": "The user has made 10 trades in the past hour on 2025-02-02 03:49:56, with a total volume of 5,000. This is highly unusual and suspicious."
}
```

# analysis_output/high_risk_sample.json

```json
{
    "user_id": 1005,
    "risk_level": "high",
    "justification": "The user has made 10 trades in the past hour on 2025-02-02 03:49:56, with a total volume of 5,000. This is highly unusual and suspicious."
}
```

# analysis_output/medium_risk_sample_2.json

```json
{
    "user_id": 1010,
    "risk_level": "medium",
    "justification": "The user has made 5 trades in the past hour on 2025-02-02 03:49:56, with a total volume of 2,500. This is unusual but not suspicious."
}
```

# analysis_output/medium_risk_sample.json

```json
{
    "user_id": 1006,
    "risk_level": "medium",
    "justification": "The user has made 5 trades in the past hour on 2025-02-02 03:49:56, with a total volume of 2,500. This is unusual but not suspicious."
}
```

# dashboard/app_v2.py

```py
import streamlit as st
import pandas as pd
import plotly.express as px
import sqlite3
import os
import requests
import json

st.set_page_config(page_title="Deriv Fraud Detection Dashboard", page_icon="🔒", layout="wide")

# Setup database connection
def get_connection():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(os.path.dirname(current_dir), 'database', 'deriv_db.sqlite')
    return sqlite3.connect(db_path)

# Helper function to run SQL queries
@st.cache_data
def run_query(query, params=None):
    with get_connection() as conn:
        if params:
            return pd.read_sql(query, conn, params=params)
        return pd.read_sql(query, conn)

# Add Ollama AI helper function
def get_ai_analysis(data, prompt):
    try:
        # Increase timeout and add connection timeout
        response = requests.post(
            'http://localhost:11434/api/generate', 
            json={
                "model": "deepseek-r1:1.5b",
                "prompt": f"""As a fraud detection expert at Deriv, analyze this data and provide insights:
                {data}
                
                {prompt}
                
                Provide a detailed analysis focusing on:
                1. Suspicious patterns and anomalies (based on deposit/withdrawal amount, duration of trade, trading frequency)
                2. Risk indicators and severity levels
                3. Potential fraud scenarios
                4. Recommended actions with justification
                
                Format your response in json format with the following keys:
                - user_id: user_id
                - user_name: user_name
                - risk_level: high/medium/low
                - justification: summary ofjustification for the recommended actions and reference to data
                """,
                "stream": False,
                "options": {
                    "temperature": 0.4,
                    "top_p": 0.9,
                    "num_predict": 8192,  # Increased from 256
                    "top_k": 40,
                    "repeat_penalty": 1.1
                }
            }, 
            timeout=(5, 120)  # Increased timeout
        )
        
        if response.status_code != 200:
            return f"API Error: Status code {response.status_code}"
            
        response_data = response.json()
        if 'response' not in response_data:
            return f"API Error: Unexpected response format - {str(response_data)}"
            
        return response_data['response']
        
    except requests.exceptions.ConnectTimeout:
        return "Error: Cannot connect to Ollama (connection timeout). Make sure Ollama is running."
    except requests.exceptions.ReadTimeout:
        return "Error: Ollama response took too long. Try again or use a smaller data sample."
    except requests.exceptions.ConnectionError:
        return "Error: Cannot connect to Ollama. Make sure Ollama is running (ollama run deepseek-r1:7b-qwen-distill-q8_0)"
    except Exception as e:
        return f"AI Analysis Error: {str(e)}\nFull error: {type(e).__name__}"

# Add AI decision function
def get_ai_decision(client_data):
    try:
        prompt = f"""As a senior fraud detection expert at Deriv, analyze this client data and provide a comprehensive decision:
        {client_data}
        
        Consider the following factors:
        1. Transaction patterns and amounts
        2. Platform usage and trading behavior
        3. Payment methods and frequency
        4. Account history and status
        5. Regional risk factors
        6. Similar patterns in other accounts
        
        Provide a detailed decision in the following format:

        RISK ASSESSMENT:
        - Risk Level: [High/Medium/Low]
        - Primary Risk Factors:
          * [List key risk indicators]
          * [Include specific metrics/thresholds]
        
        RECOMMENDED ACTIONS:
        1. Immediate Action: [Lock Account/Enhanced Monitoring/Clear]
        2. Additional Steps:
           * [List specific actions]
           * [Include timeline recommendations]
        
        JUSTIFICATION:
        - [Detailed explanation of decision]
        - [Reference to specific data points]
        - [Comparison to known fraud patterns]
        
        INVESTIGATION REQUIREMENTS:
        1. Priority Areas:
           * [List specific areas to investigate]
           * [Include required documentation]
        2. Follow-up Actions:
           * [List verification steps]
           * [Include timeline]
        """
        
        response = requests.post(
            'http://localhost:11434/api/generate', 
            json={
                "model": "deepseek-r1:1.5b",
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.7,
                    "top_p": 0.9,
                    "num_predict": 2048,  # Increased significantly
                    "top_k": 40,
                    "repeat_penalty": 1.1
                }
            }, 
            timeout=(5, 180)  # Increased timeout for longer analysis
        )
        
        if response.status_code == 200:
            return response.json()['response']
        return "Error: Unable to get AI decision"
        
    except Exception as e:
        return f"Decision Error: {str(e)}"

st.title("Deriv Fraud Detection Dashboard")
st.subheader("Anti-Fraud Team Analysis Tool")

# # Sidebar filters
# st.sidebar.header("Filters")
# selected_platform = st.sidebar.multiselect(
#     "Platform",
#     ["Deriv MT5", "Deriv cTrader", "Deriv X", "Deriv GO", "Deriv Trader"],
#     default=["Deriv MT5"]
# )

# selected_market = st.sidebar.multiselect(
#     "Market",
#     ["Forex", "Derived Indices", "Stocks", "Cryptocurrencies", "Commodities"],
#     default=["Forex"]
# )

def display_descriptive_stats():
    st.header("User Information")
    user_data = run_query('SELECT * FROM user')

    col1, col2, col3, col4 = st.columns(4)

    # user_id
    # user_email
    # user_profession
    # user_country
    # user_createdat
    # user_lastlogin
    # user_kycstatus
    # user_lockstatus

    col1.metric(
        "Users", 
        f"{user_data['user_id'].nunique()}"
    )
    col2.metric(
        "Professions", 
        f"{user_data['user_profession'].nunique()}"
    )
    col3.metric(
        "Countries",
        f"{user_data['user_country'].nunique()}"
    )
    col4.metric(
        "Locked Accounts",
        f"{user_data['user_lockstatus'].nunique()}"
    )

    st.header("Transaction Information")
    transaction_data = run_query('SELECT * FROM user_transaction')

    cols_ = st.columns(6)

    # transaction_id
    # transaction_userid
    # transaction_datetime
    # transaction_type
    # transaction_amount
    # transaction_paymentmethod

    cols_[0].metric(
        "Total Transactions", 
        f"{transaction_data['transaction_id'].nunique()}"
    )
    cols_[1].metric(
        "Total Deposits", 
        f"{transaction_data['transaction_type'].value_counts()['deposit']}"
    )
    cols_[2].metric(
        "Total Withdrawals",
        f"{transaction_data['transaction_type'].value_counts()['withdrawal']}"
    )
    cols_[3].metric(
        "Total Amount Deposited (RM)",
        f"{transaction_data[transaction_data['transaction_type'] == 'deposit']['transaction_amount'].sum():,.2f}"
    )
    cols_[4].metric(
        "Total Amount Withdrawn (RM)",
        f"{transaction_data[transaction_data['transaction_type'] == 'withdrawal']['transaction_amount'].sum():,.2f}"
    )
    cols_[5].metric(
        "Total Payment Methods",
        f"{transaction_data['transaction_paymentmethod'].nunique()}"
    )

    st.header("Trading Information")
    trade_data = run_query('SELECT * FROM user_trading')

    cols_ = st.columns(7)

    # trade_id
    # trade_userid
    # trade_timestamp
    # trade_type
    # trade_market
    # trade_pair
    # trade_volume
    # trade_openprice
    # trade_closeprice
    # trade_currentprice
    # trade_profit
    # trade_platform

    cols_[0].metric(
        "Total Trades", 
        f"{trade_data['trade_id'].nunique()}"
    )
    cols_[1].metric(
        "Total Buys", 
        f"{trade_data['trade_type'].value_counts()['buy']}"
    )
    cols_[2].metric(
        "Total Sells",
        f"{trade_data['trade_type'].value_counts()['sell']}"
    )
    cols_[3].metric(
        "Total Volume",
        f"{trade_data['trade_volume'].sum():,.2f}"
    )
    cols_[4].metric(
        "Total Profit",
        f"{trade_data['trade_profit'].sum():,.2f}"
    )

    cols_[5].metric(
        "Top Market",
        f"{trade_data['trade_market'].value_counts().idxmax()}"
    )

    cols_[6].metric(
        "Top Pair",
        f"{trade_data['trade_pair'].value_counts().idxmax()}"
    )

    # Payment Method Dist ribution
    cols = st.columns(2)

    with cols[0]:
        st.subheader("Payment Method Distribution")
        payment_data = transaction_data['transaction_paymentmethod'].value_counts()
        fig_payment = px.pie(
            values=payment_data.values,
            names=payment_data.index,
            title="Distribution by Payment Method"
        )
        st.plotly_chart(fig_payment)

    with cols[1]:
        st.subheader("Market Distribution") 
        market_data = trade_data['trade_market'].value_counts()
        fig_market = px.pie(
            values=market_data.values,
            names=market_data.index,
            title="Distribution by Market"
        )   
        st.plotly_chart(fig_market)

# # Suspicious Groups Analysis
# st.header("Suspicious Group Patterns")
# group_data = run_query('SELECT * FROM suspicious_groups')
# fig_groups = px.scatter(
#     group_data,
#     x='region',
#     y='client_count',
#     size='client_count',
#     color='platform',
#     hover_data=['payment_method', 'client_ids'],
#     title="Suspicious Groups by Region and Platform"
# )
# st.plotly_chart(fig_groups)

# # Detailed Suspicious Accounts
# st.header("Suspicious Account Details")
# suspicious_accounts = run_query('''
#     SELECT 
#         sa.client_id,
#         datetime(sa.detection_date) as detected_on,
#         sa.platform,
#         sa.market_type,
#         sa.trade_type,
#         fi.amount as transaction_amount,
#         fi.payment_method,
#         CASE 
#             WHEN la.lock_date IS NOT NULL THEN 'Locked 🔒'
#             ELSE 'Active ✅'
#         END as account_status,
#         la.lock_date
#     FROM suspicious_accounts sa
#     LEFT JOIN financial_impact fi ON sa.client_id = fi.client_id
#     LEFT JOIN locked_accounts la ON sa.client_id = la.client_id
#     ORDER BY sa.detection_date DESC
# ''')

# # Add AI Decision column
# decisions = []
# for _, row in suspicious_accounts.iterrows():
#     if st.button(f"Analyze Client {row['client_id']}", key=f"analyze_{row['client_id']}"):
#         with st.spinner("AI analyzing client..."):
#             # Get comprehensive client data
#             client_data = run_query(f'''
#                 SELECT 
#                     sa.*,
#                     fi.amount,
#                     fi.fee,
#                     fi.payment_method,
#                     CASE 
#                         WHEN la.client_id IS NOT NULL THEN 'Locked'
#                         ELSE 'Active'
#                     END as current_status
#                 FROM suspicious_accounts sa
#                 LEFT JOIN financial_impact fi ON sa.client_id = fi.client_id
#                 LEFT JOIN locked_accounts la ON sa.client_id = la.client_id
#                 WHERE sa.client_id = ?
#             ''', [row['client_id']]).to_dict('records')[0]
            
#             decision = get_ai_decision(client_data)
#             st.write(f"### AI Analysis for Client {row['client_id']}")
#             st.write(decision)

# # Display enhanced dataframe
# st.dataframe(
#     suspicious_accounts,
#     column_config={
#         "client_id": st.column_config.NumberColumn(
#             "Client ID",
#             help="Unique identifier for the client"
#         ),
#         "detected_on": st.column_config.DatetimeColumn(
#             "Detected On",
#             format="D MMM YYYY, HH:mm"
#         ),
#         "platform": st.column_config.TextColumn("Trading Platform"),
#         "market_type": st.column_config.TextColumn("Market"),
#         "trade_type": st.column_config.TextColumn("Trade Type"),
#         "transaction_amount": st.column_config.NumberColumn(
#             "Transaction Amount",
#             format="$%.2f"
#         ),
#         "payment_method": st.column_config.TextColumn("Payment Method"),
#         "account_status": st.column_config.TextColumn(
#             "Account Status",
#             help="Account status: Locked 🔒 or Active ✅"
#         ),
#         "lock_date": st.column_config.DatetimeColumn(
#             "Lock Date",
#             format="D MMM YYYY, HH:mm",
#             help="Date when account was locked"
#         )
#     },
#     hide_index=True
# )

# # Geographic Distribution
# st.header("Geographic Distribution")
# country_data = run_query('''
#     SELECT 
#         sa.country,  -- Using country field from suspicious_accounts
#         COUNT(*) as client_count,
#         AVG(sa.risk_score) as avg_risk,
#         AVG(fi.amount) as avg_volume,
#         COUNT(CASE WHEN sa.platform = 'Deriv MT5' THEN 1 END) as platform_count
#     FROM suspicious_accounts sa
#     LEFT JOIN financial_impact fi ON sa.client_id = fi.client_id
#     GROUP BY sa.country
# ''')

# # Add this before creating the choropleth map
# COUNTRY_NAME_MAP = {
#     'UK': 'United Kingdom',
#     'USA': 'United States',
#     'UAE': 'United Arab Emirates',
#     # Add more mappings as needed
# }

# # Update country names in the dataframe
# country_data['country'] = country_data['country'].map(COUNTRY_NAME_MAP).fillna(country_data['country'])

# # Create two columns for country stats
# col1, col2 = st.columns(2)

# with col1:
#     # Country distribution map
#     fig_map = px.choropleth(
#         country_data,
#         locations=country_data['country'],
#         locationmode="country names",
#         color="avg_risk",
#         hover_data=["client_count", "avg_volume", "platform_count"],
#         title="Risk Distribution by Country",
#         color_continuous_scale="Reds"
#     )
#     st.plotly_chart(fig_map)

# with col2:
#     # Country metrics table
#     st.dataframe(
#         country_data,
#         column_config={
#             "country": "Country",
#             "client_count": st.column_config.NumberColumn("Suspicious Clients"),
#             "avg_risk": st.column_config.NumberColumn("Avg Risk Score", format="%.2f"),
#             "avg_volume": st.column_config.NumberColumn("Avg Volume", format="$%.2f"),
#             "platform_count": st.column_config.NumberColumn("MT5 Users")
#         },
#         hide_index=True
#     )

# # Country Details Section
# st.header("Country Risk Analysis")
# selected_country = st.selectbox(
#     "Select Country for Detailed Analysis",
#     options=country_data['country'].unique()
# )

# if selected_country:
#     country_details = run_query(f'''
#         SELECT 
#             sa.*,
#             fi.total_deposits,
#             fi.total_withdrawals,
#             fi.payment_method,
#             fi.chargeback_count,
#             COALESCE(ta.trade_count, 0) as trade_count,
#             COALESCE(ta.total_profit_loss, 0) as total_pnl
#         FROM suspicious_accounts sa
#         LEFT JOIN financial_impact fi ON sa.client_id = fi.client_id
#         LEFT JOIN (
#             SELECT 
#                 client_id,
#                 COUNT(*) as trade_count,
#                 SUM(profit_loss) as total_profit_loss
#             FROM trading_activity
#             GROUP BY client_id
#         ) ta ON sa.client_id = ta.client_id
#         WHERE sa.country = ?
#     ''', [selected_country])

#     # Show country statistics
#     st.subheader(f"Risk Profile: {selected_country}")
#     col1, col2, col3, col4 = st.columns(4)
    
#     col1.metric(
#         "Average Risk Score", 
#         f"{country_details['risk_score'].mean():.1f}"
#     )
#     col2.metric(
#         "Total Trading Volume", 
#         f"${country_details['trading_volume'].sum():,.0f}"
#     )
#     col3.metric(
#         "Failed KYC Rate", 
#         f"{(country_details['kyc_status'] == 'Failed').mean()*100:.1f}%"
#     )
#     col4.metric(
#         "Avg Profit/Loss", 
#         f"${country_details['total_pnl'].mean():,.2f}"
#     )

#     # Show client details for the country
#     st.subheader(f"Suspicious Clients in {selected_country}")
    
#     # Enhanced client details with tabs
#     client_tabs = st.tabs(["Overview", "Trading Activity", "Transaction Activity"])
    
#     with client_tabs[0]:
#         st.dataframe(
#             country_details[[
#                 'client_id', 'profession', 'annual_income', 
#                 'risk_score', 'kyc_status', 'trade_frequency'
#             ]],
#             column_config={
#                 "client_id": "Client ID",
#                 "profession": "Profession",
#                 "annual_income": st.column_config.NumberColumn("Annual Income", format="$%.2f"),
#                 "risk_score": st.column_config.NumberColumn("Risk Score", format="%.1f"),
#                 "kyc_status": "KYC Status",
#                 "trade_frequency": "Trading Frequency"
#             },
#             hide_index=True
#         )

#     with client_tabs[1]:
#         # Get trading activity for selected country
#         trading_data = run_query(f'''
#             SELECT 
#                 ta.*,
#                 sa.country
#             FROM trading_activity ta
#             JOIN suspicious_accounts sa ON ta.client_id = sa.client_id
#             WHERE sa.country = ?
#             ORDER BY trade_date DESC
#         ''', [selected_country])
        
#         st.dataframe(
#             trading_data,
#             column_config={
#                 "trade_date": st.column_config.DatetimeColumn("Trade Date"),
#                 "position_size": st.column_config.NumberColumn("Position Size", format="$%.2f"),
#                 "profit_loss": st.column_config.NumberColumn("P/L", format="$%.2f"),
#                 "leverage": "Leverage",
#                 "strategy_type": "Strategy"
#             },
#             hide_index=True
#         )

#     with client_tabs[2]:
#         # Get financial activity
#         financial_data = run_query(f'''
#             SELECT 
#                 fi.*,
#                 sa.country
#             FROM financial_impact fi
#             JOIN suspicious_accounts sa ON fi.client_id = sa.client_id
#             WHERE sa.country = ?
#         ''', [selected_country])
        
#         st.dataframe(
#             financial_data[[
#                 'client_id', 'total_deposits', 'total_withdrawals',
#                 'deposit_frequency', 'withdrawal_frequency', 'chargeback_count'
#             ]],
#             column_config={
#                 "total_deposits": st.column_config.NumberColumn("Total Deposits", format="$%.2f"),
#                 "total_withdrawals": st.column_config.NumberColumn("Total Withdrawals", format="$%.2f"),
#                 "chargeback_count": "Chargebacks"
#             },
#             hide_index=True
#         )

# Add AI Analysis Section
st.header("🤖 AI-Powered Analysis")
user_ids = run_query('SELECT DISTINCT user_id FROM user')['user_id'].tolist() # Get list of user IDs
selected_user = st.selectbox("Select User ID", user_ids) # Create dropdown for user selection

analysis_tab1, analysis_tab2 = st.tabs(["Transaction Analysis", "Trading Analysis"])
   
with analysis_tab1:
    st.subheader("User Transaction Analysis")
    
    if st.button("Analyze User Transactions"):
        with st.spinner("Analyzing user transactions..."):
            # Get transaction data for selected user
            user_transactions = run_query('''
                SELECT 
                    transaction_datetime,
                    transaction_type,
                    transaction_amount,
                    transaction_paymentmethod
                FROM user_transaction
                WHERE transaction_userid = ?
                ORDER BY transaction_datetime DESC
            ''', [selected_user])
            
            if not user_transactions.empty:
                st.write("### Transaction History")
                st.dataframe(user_transactions)
                
                analysis = get_ai_analysis(
                    user_transactions.to_string(),
                    "Analyze this user's transaction patterns and identify any suspicious behavior."
                )
                st.write("### AI Analysis")
                st.write(analysis)
            else:
                st.write("No transactions found for this user.")

with analysis_tab2:
    st.subheader("User Trading Analysis")
    
    # Get list of user IDs
    
    if st.button("Analyze User Trading"):
        with st.spinner("Analyzing user trading activity..."):
            # Get trading data for selected user
            user_transactions = run_query('''
                SELECT 
                    trade_timestamp,
                    trade_type,
                    trade_market,
                    trade_pair,
                    trade_volume,
                    trade_openprice,
                    trade_closeprice,
                    trade_currentprice,
                    trade_profit
                FROM user_trading
                WHERE trade_userid = ?
                ORDER BY trade_timestamp DESC
            ''', [selected_user])
            
            if not user_transactions.empty:
                st.write("### Trading History")
                st.dataframe(user_transactions)
                
                analysis = get_ai_analysis(
                    user_transactions.to_string(),
                    "Analyze this user's trading patterns and identify any suspicious behavior."
                )
                st.write("### AI Analysis")
                st.write(analysis)
            else:
                st.write("No transactions found for this user.")
    
#     if st.button("Assess Risks"):
#         with st.spinner("AI assessing risks..."):
#             # Summarize data before sending
#             risk_summary = risk_data.groupby(['region', 'platform'])['client_count'].sum().to_string()
#             analysis = get_ai_analysis(
#                 risk_summary,
#                 "Assess risk levels by region and platform, highlighting highest risk areas."
#             )
#             st.write(analysis)

# with analysis_tab3:
#     st.subheader("Action Recommendations")
#     if st.button("Generate Recommendations"):
#         with st.spinner("AI generating recommendations..."):
#             # Summarize data before sending
#             all_data = {
#                 "suspicious_accounts": run_query("SELECT * FROM suspicious_accounts").head(5).to_string(),
#                 "financial_impact": run_query("SELECT * FROM financial_impact").agg({
#                     'fee': 'sum',
#                     'amount': 'sum'
#                 }).to_string(),
#                 "groups": run_query("SELECT region, COUNT(*) as count FROM suspicious_groups GROUP BY region").to_string()
#             }
#             analysis = get_ai_analysis(
#                 json.dumps(all_data),
#                 "Provide specific action recommendations for the anti-fraud team based on this data."
#             )
#             st.write(analysis)
    
#     print(analysis)

# # Add AI Chat Interface
# st.sidebar.header("💬 AI Assistant")
# if "messages" not in st.session_state:
#     st.session_state.messages = []

# for message in st.session_state.messages:
#     with st.sidebar.chat_message(message["role"]):
#         st.markdown(message["content"])

# if prompt := st.sidebar.chat_input("Ask about fraud patterns..."):
#     st.session_state.messages.append({"role": "user", "content": prompt})
#     with st.sidebar.chat_message("user"):
#         st.markdown(prompt)

#     with st.sidebar.chat_message("assistant"):
#         with st.spinner("Thinking..."):
#             all_data = {
#                 "suspicious_accounts": run_query("SELECT * FROM suspicious_accounts").head().to_string(),
#                 "financial_impact": run_query("SELECT * FROM financial_impact").head().to_string()
#             }
#             response = get_ai_analysis(
#                 json.dumps(all_data),
#                 f"Based on this fraud detection data, please answer: {prompt}"
#             )
#             st.markdown(response)
#     st.session_state.messages.append({"role": "assistant", "content": response})

# # Add this to the AI analysis section
# if st.button("Analyze Country Patterns"):
#     with st.spinner("AI analyzing country patterns..."):
#         country_analysis = get_ai_analysis(
#             country_data.to_string(),
#             f"""Analyze the fraud patterns across different countries, focusing on:
#             1. High-risk countries and their characteristics
#             2. Common patterns in each region
#             3. KYC failure patterns
#             4. Trading behavior variations by country
            
#             Provide specific insights for {selected_country} if available."""
#         )
#         st.write(country_analysis)

# # Non-Trading and Fake Trading Detection
# st.header("🚨 Suspicious Trading Patterns")
# tab_non_trading, tab_fake_trading, tab_groups = st.tabs([
#     "Non-Trading Clients", 
#     "Suspicious Trading Patterns",
#     "Group Analysis"
# ])

# with tab_non_trading:
#     st.subheader("Clients with Deposits but No Trading")
#     non_trading_clients = run_query('''
#         WITH client_activity AS (
#             SELECT 
#                 fi.client_id,
#                 sa.platform,
#                 sa.country,
#                 fi.total_deposits,
#                 fi.total_withdrawals,
#                 COALESCE(ta.trade_count, 0) as trade_count,
#                 fi.payment_method,
#                 sa.detection_date
#             FROM financial_impact fi
#             JOIN suspicious_accounts sa ON fi.client_id = sa.client_id
#             LEFT JOIN (
#                 SELECT client_id, COUNT(*) as trade_count
#                 FROM trading_activity
#                 GROUP BY client_id
#             ) ta ON fi.client_id = ta.client_id
#             WHERE fi.total_deposits > 0
#         )
#         SELECT *
#         FROM client_activity
#         WHERE trade_count = 0
#         ORDER BY total_deposits DESC
#     ''')
    
#     # Display non-trading clients metrics
#     col1, col2, col3 = st.columns(3)
#     col1.metric(
#         "Non-Trading Clients", 
#         len(non_trading_clients)
#     )
#     col2.metric(
#         "Total Deposits", 
#         f"${non_trading_clients['total_deposits'].sum():,.2f}"
#     )
#     col3.metric(
#         "Total Withdrawals", 
#         f"${non_trading_clients['total_withdrawals'].sum():,.2f}"
#     )
    
#     # Display detailed table
#     st.dataframe(
#         non_trading_clients,
#         column_config={
#             "client_id": "Client ID",
#             "platform": "Platform",
#             "country": "Country",
#             "total_deposits": st.column_config.NumberColumn(
#                 "Total Deposits",
#                 format="$%.2f"
#             ),
#             "total_withdrawals": st.column_config.NumberColumn(
#                 "Total Withdrawals",
#                 format="$%.2f"
#             ),
#             "payment_method": "Payment Method",
#             "detection_date": st.column_config.DatetimeColumn(
#                 "Detected On",
#                 format="D MMM YYYY, HH:mm"
#             )
#         },
#         hide_index=True
#     )

# with tab_fake_trading:
#     st.subheader("Suspicious Trading Patterns")
#     fake_trading_clients = run_query('''
#         WITH trading_metrics AS (
#             SELECT 
#                 ta.client_id,
#                 COUNT(*) as trade_count,
#                 AVG(ta.profit_loss) as avg_pnl,
#                 AVG(ta.position_size) as avg_position,
#                 MAX(ta.trade_date) - MIN(ta.trade_date) as trading_duration,
#                 COUNT(*) * 1.0 / 
#                     (JULIANDAY(MAX(ta.trade_date)) - JULIANDAY(MIN(ta.trade_date))) as trades_per_day
#             FROM trading_activity ta
#             GROUP BY ta.client_id
#             HAVING 
#                 COUNT(*) > 10  -- Minimum trades to analyze
#                 AND AVG(ABS(ta.profit_loss)) < 1.0  -- Very small P/L
#                 AND trades_per_day > 5  -- High frequency
#         )
#         SELECT 
#             tm.*,
#             sa.platform,
#             sa.country,
#             fi.total_deposits,
#             fi.payment_method
#         FROM trading_metrics tm
#         JOIN suspicious_accounts sa ON tm.client_id = sa.client_id
#         JOIN financial_impact fi ON tm.client_id = fi.client_id
#         ORDER BY trades_per_day DESC
#     ''')
    
#     # Display fake trading metrics
#     col1, col2, col3 = st.columns(3)
#     col1.metric(
#         "Suspicious Traders", 
#         len(fake_trading_clients)
#     )
#     col2.metric(
#         "Avg Trades/Day", 
#         f"{fake_trading_clients['trades_per_day'].mean():.1f}"
#     )
#     col3.metric(
#         "Avg P/L", 
#         f"${fake_trading_clients['avg_pnl'].mean():.2f}"
#     )
    
#     # Display detailed table
#     st.dataframe(
#         fake_trading_clients,
#         column_config={
#             "client_id": "Client ID",
#             "trade_count": "Total Trades",
#             "avg_pnl": st.column_config.NumberColumn(
#                 "Avg P/L",
#                 format="$%.2f"
#             ),
#             "trades_per_day": st.column_config.NumberColumn(
#                 "Trades/Day",
#                 format="%.1f"
#             ),
#             "platform": "Platform",
#             "country": "Country",
#             "payment_method": "Payment Method"
#         },
#         hide_index=True
#     )

# with tab_groups:
#     st.subheader("Similar Behavior Groups")
    
#     # Get group patterns
#     group_patterns = run_query('''
#         WITH client_groups AS (
#             SELECT 
#                 sa.country,
#                 sa.platform,
#                 fi.payment_method,
#                 COUNT(DISTINCT sa.client_id) as client_count,
#                 GROUP_CONCAT(sa.client_id) as client_ids,
#                 AVG(COALESCE(ta.trade_count, 0)) as avg_trades,
#                 AVG(fi.total_deposits) as avg_deposits
#             FROM suspicious_accounts sa
#             JOIN financial_impact fi ON sa.client_id = fi.client_id
#             LEFT JOIN (
#                 SELECT client_id, COUNT(*) as trade_count
#                 FROM trading_activity
#                 GROUP BY client_id
#             ) ta ON sa.client_id = ta.client_id
#             GROUP BY sa.country, sa.platform, fi.payment_method
#             HAVING COUNT(DISTINCT sa.client_id) >= 2
#         )
#         SELECT *
#         FROM client_groups
#         ORDER BY client_count DESC, avg_deposits DESC
#     ''')
    
#     # Display group metrics
#     col1, col2 = st.columns(2)
    
#     # Group scatter plot
#     fig_groups = px.scatter(
#         group_patterns,
#         x="avg_deposits",
#         y="avg_trades",
#         size="client_count",
#         color="platform",
#         hover_data=["country", "payment_method", "client_ids"],
#         title="Group Behavior Analysis"
#     )
#     col1.plotly_chart(fig_groups)
    
#     # Group details table
#     col2.dataframe(
#         group_patterns,
#         column_config={
#             "country": "Country",
#             "platform": "Platform",
#             "payment_method": "Payment Method",
#             "client_count": "# of Clients",
#             "avg_trades": st.column_config.NumberColumn(
#                 "Avg Trades",
#                 format="%.1f"
#             ),
#             "avg_deposits": st.column_config.NumberColumn(
#                 "Avg Deposits",
#                 format="$%.2f"
#             ),
#             "client_ids": "Client IDs"
#         },
#         hide_index=True
#     )

# # Add action buttons for bulk operations
# st.header("🔒 Bulk Actions")
# col1, col2 = st.columns(2)

# with col1:
#     if st.button("Lock Non-Trading Accounts"):
#         with st.spinner("Processing..."):
#             for _, client in non_trading_clients.iterrows():
#                 run_query('''
#                     INSERT OR REPLACE INTO locked_accounts (client_id, lock_date)
#                     VALUES (?, CURRENT_TIMESTAMP)
#                 ''', [client['client_id']])
#             st.success(f"Locked {len(non_trading_clients)} accounts")

# with col2:
#     if st.button("Lock Suspicious Trading Accounts"):
#         with st.spinner("Processing..."):
#             for _, client in fake_trading_clients.iterrows():
#                 run_query('''
#                     INSERT OR REPLACE INTO locked_accounts (client_id, lock_date)
#                     VALUES (?, CURRENT_TIMESTAMP)
#                 ''', [client['client_id']])
#             st.success(f"Locked {len(fake_trading_clients)} accounts")

# # Add financial impact analysis
# st.header("💰 Financial Impact Analysis")
# impact_data = run_query('''
#     SELECT 
#         COALESCE(nt.impact_type, 'Suspicious Trading') as activity_type,
#         COUNT(DISTINCT fi.client_id) as client_count,
#         SUM(fi.fee) as total_fees,
#         SUM(fi.amount) as total_volume
#     FROM financial_impact fi
#     LEFT JOIN (
#         SELECT 
#             subq.client_id,
#             'Non-Trading' as impact_type
#         FROM (
#             SELECT fi2.client_id
#             FROM financial_impact fi2
#             LEFT JOIN trading_activity ta ON fi2.client_id = ta.client_id
#             GROUP BY fi2.client_id
#             HAVING COUNT(ta.client_id) = 0
#         ) subq
#     ) nt ON fi.client_id = nt.client_id
#     GROUP BY COALESCE(nt.impact_type, 'Suspicious Trading')
# ''')

# col1, col2 = st.columns(2)

# with col1:
#     # Impact metrics
#     st.dataframe(
#         impact_data,
#         column_config={
#             "activity_type": "Activity Type",
#             "client_count": "# of Clients",
#             "total_fees": st.column_config.NumberColumn(
#                 "Total Fees",
#                 format="$%.2f"
#             ),
#             "total_volume": st.column_config.NumberColumn(
#                 "Total Volume",
#                 format="$%.2f"
#             )
#         },
#         hide_index=True
#     )

# with col2:
#     # Impact visualization
#     fig_impact = px.pie(
#         impact_data,
#         values="total_fees",
#         names="activity_type",
#         title="Distribution of Financial Impact"
#     )
#     st.plotly_chart(fig_impact) 
```

# dashboard/app_v3.py

```py
import streamlit as st
import pandas as pd
import plotly.express as px
import sqlite3
import os
import requests
import json
from datetime import datetime
import time
import joblib
import os
import sys

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

# Now import from src
from src.utils.feature_engineering import FeatureEngineer

# Load fraud detection model
try:
    MODEL_PATH = os.path.join(os.path.dirname(__file__), '..', 'models', 'fraud_detector.joblib')
    fraud_model = joblib.load(MODEL_PATH)
    feature_engineer = FeatureEngineer()
    MODEL_LOADED = True
except Exception as e:
    print(f"Error loading fraud detection model: {e}")
    MODEL_LOADED = False

DB_PATH = './database/deriv_db.sqlite'

st.set_page_config(page_title="Deriv Fraud Detection Dashboard", page_icon="🔒", layout="wide")

# Setup database connection
def get_connection():
    return sqlite3.connect(DB_PATH)

# Helper function to run SQL queries
@st.cache_data(ttl=5)  # Cache for 5 seconds only
def run_query(query, params=None):
    with get_connection() as conn:
        if params:
            return pd.read_sql(query, conn, params=params)
        return pd.read_sql(query, conn)
    
def analyze_with_fraud_model(user_data, transaction_data, trade_data):
    """Analyze user behavior using the fraud detection model"""
    try:
        if not MODEL_LOADED:
            return {"error": "Fraud detection model not loaded"}

        # Convert query results to dataframes
        trades_df = pd.DataFrame(trade_data)
        transactions_df = pd.DataFrame(transaction_data)

        # Engineer features
        features = feature_engineer.calculate_user_features(trades_df, transactions_df)
        
        # Get prediction probability
        fraud_prob = fraud_model.predict_proba(features)[0][1]
        
        return {
            "fraud_probability": float(fraud_prob),
            "risk_level": "high" if fraud_prob > 0.7 else "medium" if fraud_prob > 0.3 else "low"
        }
    except Exception as e:
        print(f"Debug - Error details: {str(e)}")
        return {"error": f"Analysis error: {str(e)}"}

# Add Ollama AI helper function
def analyse_user_data(user_data, transaction_data, trade_data):
    try:
        # Increase timeout and add connection timeout
        response = requests.post(
            'http://localhost:11434/api/generate', 
            json={
                # "model": "deepseek-r1:1.5b",
                "model": "llama3.2:latest",
                # "model": "qwen2-math:latest",
                "prompt": f"""As a fraud detection expert at Deriv, analyze the following three datasets and provide insights for one user:

                User Information (one user):
                {user_data}

                where user_email is the email of the user,
                user_profession is the profession of the user,
                user_country is the country of the user,
                user_createdat is the timestamp when the user is created,
                user_lastlogin is the timestamp when the user is last logged in,
                user_kycstatus is the KYC status of the user.

                Transaction Information:
                {transaction_data}

                where transaction_datetime is the timestamp when the transaction is made,
                transaction_type is the type of the transaction (deposit/withdrawal),
                transaction_amount is the amount of the transaction,
                transaction_paymentmethod is the payment method used for the transaction.

                Trading Information:
                {trade_data}

                where trade_timestamp is the timestamp when trade is closed,
                trade_durationminutes is the duration of the trade in minutes (from buy to sell),
                trade_market is the market of the trade,
                trade_pair is the pair of the trade based on the market,
                trade_cost is the actual monetary cost of the trade,
                trade_profit is the profit/loss of the trade,
                trade_profitratio is the profit/loss ratio wrt cost of the trade,
                trades_past_month is the number of trades made in the past month,
                trades_past_week is the number of trades made in the past week,
                trades_past_day is the number of trades made in the past day,
                trades_past_hour is the number of trades made in the past hour.
                
                Detect any suspicious patterns and anomalies in the data based on all the available data. The main objective is to identify whether a user is new to trading, testing the platform, or committing fraud. The final result should include the risk level (Low/Medium/High) and the justification for the risk level based ONLY on the three datasets provided. For each analysis, include the dataset and row number(s) of the dataset that supports the justfication.

                Example of potential fraud cases:
                1. Deposit and withdrawal without trading
                2. Pretending to trade with short trades in quick succession using minimal amounts without significant profit or loss

                The following description may help in identifying potential fraud cases:
                1. High number of trades (buy & sell) in a short period of time. Say less than 15 minutes.
                2. Short trade duration. Say less than 1 minute.
                3. Minimal trade amount.
                4. Minimal trade profit. Say less than 5% of the trade amount.
                
                Format your response in json format with the following keys (only one output!):
                - risk_level: high/medium/low
                - justification: reference to data and explanation of the risk level 

                """,
                "stream": False,
                "options": {
                    "temperature": 0.4,
                    "top_p": 0.9,
                    "num_predict": 8192,  # Increased from 256
                    "top_k": 40,
                    "repeat_penalty": 1.1
                }
            }, 
            timeout=(5, 120)  # Increased timeout
        )
        
        if response.status_code != 200:
            return f"API Error: Status code {response.status_code}"
            
        response_data = response.json()
        if 'response' not in response_data:
            return f"API Error: Unexpected response format - {str(response_data)}"
            
        return response_data
        
    except requests.exceptions.ConnectTimeout:
        return "Error: Cannot connect to Ollama (connection timeout). Make sure Ollama is running."
    except requests.exceptions.ReadTimeout:
        return "Error: Ollama response took too long. Try again or use a smaller data sample."
    except requests.exceptions.ConnectionError:
        return "Error: Cannot connect to Ollama. Make sure Ollama is running (ollama run deepseek-r1:7b-qwen-distill-q8_0)"
    except Exception as e:
        return f"AI Analysis Error: {str(e)}\nFull error: {type(e).__name__}"

def display_user_analysis():
        # Add AI Analysis Section
        st.header("🤖 AI-Powered Analysis")
        user_ids = run_query('SELECT DISTINCT user_id FROM user')['user_id'].tolist() # Get list of user IDs
        selected_userid = st.selectbox("Select User ID", user_ids) # Create dropdown for user selection
        selected_username = run_query('SELECT user_name FROM user WHERE user_id = ?', [selected_userid]).iloc[0]['user_name']
        selected_userstatus = run_query('SELECT user_status FROM user WHERE user_id = ?', [selected_userid]).iloc[0]['user_status']

        if st.button("Analyze User Behavior"):
            if selected_userstatus == 'locked':
                st.write("User is locked.")
            elif selected_userstatus == 'monitor':
                st.write("User is already being monitored.")
            else:
                with st.spinner(f"Analyzing user behavior for {selected_username} | user_id: {selected_userid}"):
                    
                    # Get transaction data for selected user
                    user_data = run_query('''
                        SELECT 
                            user_email,
                            user_profession,
                            user_country,
                            user_createdat,
                            user_lastlogin,
                            user_kycstatus
                        FROM user
                        WHERE user_id = ?
                    ''', [selected_userid])
                    
                    transaction_data = run_query('''
                        SELECT 
                            transaction_datetime,
                            transaction_type,
                            transaction_amount,
                            transaction_paymentmethod
                        FROM user_transaction
                        WHERE transaction_userid = ?
                        ORDER BY transaction_datetime ASC
                    ''', [selected_userid])

                    trade_data = run_query('''
                        SELECT 
                            trade_timestamp,
                            trade_durationminutes,
                            trade_market,
                            trade_pair,
                            trade_cost,
                            trade_profit,
                            trade_profitratio,
                            trades_past_month,
                            trades_past_week,
                            trades_past_day,
                            trades_past_hour
                        FROM user_trading
                        WHERE trade_userid = ?
                        ORDER BY trade_timestamp ASC
                    ''', [selected_userid])
                    
                    if not user_data.empty:
                        st.write("### User Data")
                        st.dataframe(user_data)
                    else:
                        st.write("No user data found for this user!")

                    if not transaction_data.empty:
                        st.write(f"### Transaction History ({len(transaction_data)} transactions)")
                        st.dataframe(transaction_data)
                    else:
                        st.write("No transactions found for this user.")
                    
                    if not trade_data.empty:
                        st.write(f"### Trading History ({len(trade_data)} trades)")
                        st.dataframe(trade_data)
                    else:
                        st.write("No trading history found for this user.")

                    #
                    # Begin analysis
                    #
                    tab1, tab2 = st.tabs(["AI Analysis", "ML Model Analysis"])

                    with tab1:
                        # # Existing AI analysis
                        # analysis_output = analyse_user_data(
                        #     user_data.to_string(),
                        #     transaction_data.to_string(),
                        #     trade_data.to_string()
                        # )
                        # st.write("### AI Analysis")
                        # st.write(analysis_output['response'])
                        
                        # AI Analysis placeholder
                        st.write("### AI Analysis")
                        st.info("""
                            🔧 AI Analysis feature is currently under maintenance.

                            This feature will use Large Language Models to:
                            - Analyze user behavior patterns
                            - Provide detailed risk assessments
                            - Give context-aware recommendations

                            Coming soon in a future update.
                        """)
                
                    with tab2:
                        # New ML model analysis
                        st.write("### Machine Learning Model Analysis")
                        model_output = analyze_with_fraud_model(
                            user_data,
                            transaction_data,
                            trade_data
                        )

                        if "error" in model_output:
                            st.error(model_output["error"])
                        else:
                            # Display metrics
                            col1, col2 = st.columns(2)
                            col1.metric(
                                "Fraud Probability", 
                                f"{model_output['fraud_probability']:.2%}"
                            )
                            col2.metric(
                                "Risk Level", 
                                model_output['risk_level'].upper()
                            )

                            # Add some context
                            st.info("""
                            This analysis is based on a Random Forest model trained on historical fraud patterns. 
                            - High Risk: >70% probability
                            - Medium Risk: 30-70% probability
                            - Low Risk: <30% probability
                            """)

                    # output_json = {
                    #     'analysis_output': analysis_output['response'],
                    #     'user_id': selected_userid,
                    # }
                    output_json = {
                        'user_id': selected_userid,
                        'model_output': model_output  # if you're using the ML model output
                    }

                    datetime_now = datetime.now().strftime("%Y%m%d/%H%M%S")
                    output_dir = f'analysis_output/{datetime_now}'
                    os.makedirs(output_dir, exist_ok=True)
                    json.dump(output_json, open(f'{output_dir}/{selected_userid}.json', 'w'))

def display_high_risk_users():
        # Remove the header since it's already shown in the tab
        # st.header("🚨 High Risk Users")
        
        # Get transaction summary by payment method for high risk users
        @st.cache_data(ttl=0)
        def get_payment_method_losses():
            return run_query('''
                WITH high_risk_transactions AS (
                    SELECT 
                        t.transaction_paymentmethod,
                        t.transaction_amount,
                        t.transaction_type,
                        u.user_status,
                        f.fraud_risk
                    FROM user_transaction t
                    JOIN user u ON t.transaction_userid = u.user_id
                    JOIN fraud f ON u.user_id = f.fraud_userid
                    WHERE u.user_status IN ('monitor', 'locked') 
                    AND f.fraud_resolved = FALSE
                )
                SELECT 
                    transaction_paymentmethod as payment_method,
                    COUNT(*) as transaction_count,
                    SUM(CASE 
                        WHEN transaction_type = 'deposit' THEN transaction_amount * 0.03
                        WHEN transaction_type = 'withdrawal' THEN transaction_amount * 0.03
                        ELSE 0 
                    END) as total_fees,
                    SUM(transaction_amount) as total_amount
                FROM high_risk_transactions
                GROUP BY transaction_paymentmethod
                ORDER BY total_fees DESC
            ''')

        # Get high risk users data (existing query)
        @st.cache_data(ttl=0)
        def get_high_risk_users():
            return run_query('''
                SELECT 
                    u.user_id,
                    u.user_name,
                    u.user_email,
                    u.user_status,
                    f.fraud_risk,
                    f.fraud_detecteddate,
                    ROUND(JULIANDAY('now') - JULIANDAY(f.fraud_detecteddate)) as days_since_detected,
                    CASE 
                        WHEN f.fraud_risk = 'high' THEN 1
                        WHEN f.fraud_risk = 'medium' THEN 2
                        ELSE 3
                    END as risk_order
                FROM user u
                LEFT JOIN fraud f ON u.user_id = f.fraud_userid
                WHERE u.user_status IN ('monitor', 'locked') AND f.fraud_resolved = FALSE
                ORDER BY risk_order ASC, f.fraud_detecteddate DESC
            ''')

        high_risk_users = get_high_risk_users()
        payment_losses = get_payment_method_losses()

        st.subheader("🚨 High Risk Users")

        if not high_risk_users.empty:
            # Display metrics without header
            cols = st.columns(3)
            
            cols[0].metric(
                "Total High Risk Users",
                len(high_risk_users)
            )
            cols[1].metric(
                "Locked Accounts", 
                len(high_risk_users[high_risk_users['user_status'] == 'locked'])
            )
            cols[2].metric(
                "Monitored Accounts",
                len(high_risk_users[high_risk_users['user_status'] == 'monitor'])
            )

            # Create a container for the table and buttons
            table_container = st.container()
            
            # Create columns for each row's resolve button
            resolve_cols = {row['user_id']: table_container.columns([0.9, 0.1]) for _, row in high_risk_users.iterrows()}
            
            # Display user table with custom formatting in first column
            for user_id, (col1, col2) in resolve_cols.items():
                with col1:
                    user_row = high_risk_users[high_risk_users['user_id'] == user_id]
                    st.dataframe(
                        user_row,
                        column_config={
                            "user_id": st.column_config.TextColumn(
                                "ID",
                                width="small"
                            ),
                            "user_name": st.column_config.TextColumn(
                                "Name",
                                width="medium"
                            ),
                            "user_email": st.column_config.TextColumn(
                                "Email",
                                width="medium"
                            ),
                            "user_status": st.column_config.TextColumn(
                                "Status",
                                width="small"
                            ),
                            "fraud_risk": st.column_config.TextColumn(
                                "Risk Level",
                                width="small"
                            ),
                            "days_since_detected": st.column_config.NumberColumn(
                                "Days",
                                help="Days since detection",
                                width="small"
                            ),
                            "fraud_detecteddate": st.column_config.DatetimeColumn(
                                "Detected",
                                format="D MMM YY HH:mm",
                                width="small"
                            )
                        },
                        hide_index=True,
                        use_container_width=True
                    )
                
                # Add resolve button in second column
                with col2:
                    if st.button(f"Resolve", key=f"resolve_{user_id}"):
                        try:
                            with get_connection() as conn:
                                conn.execute(
                                    "UPDATE fraud SET fraud_resolved = TRUE WHERE fraud_userid = ?",
                                    (user_id,)
                                )
                                conn.commit()
                            st.success(f"Case #{user_id} marked as resolved")
                            # Clear all caches
                            st.cache_data.clear()
                            run_query.clear()
                            get_high_risk_users.clear()
                            time.sleep(0.5)  # Small delay to show success message
                            st.rerun()  # Refresh the entire page
                        except Exception as e:
                            st.error(f"Error resolving case: {str(e)}")

            # Add payment method analysis section below the table
            st.divider()  # Add visual separator
            st.subheader("💰 Fees Absorbed by Payment Method")
            
            # Create two columns for the analysis
            loss_col1, loss_col2 = st.columns([0.6, 0.4])
            
            with loss_col1:
                # Create pie chart
                if not payment_losses.empty:
                    fig = px.pie(
                        payment_losses,
                        values='total_fees',
                        names='payment_method',
                        title='Distribution of Fees by Payment Method',
                        hover_data=['transaction_count', 'total_amount']
                    )
                    # Update traces to show absolute values instead of percentages
                    fig.update_traces(
                        textposition='inside', 
                        texttemplate='$%{value:,.2f}',
                        hovertemplate=(
                            "<b>%{label}</b><br>" +
                            "Fees: $%{value:,.2f}<br>" +
                            "Transactions: %{customdata[0]}<br>" +
                            "Total Amount: $%{customdata[1]:,.2f}<br>"
                        )
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No transaction data available for analysis")

            with loss_col2:
                # Display detailed metrics
                if not payment_losses.empty:
                    st.metric(
                        "Total Fees (3%)", 
                        f"${payment_losses['total_fees'].sum():,.2f}"
                    )
                    st.metric(
                        "Total Transactions",
                        f"{payment_losses['transaction_count'].sum():,}"
                    )
                    st.metric(
                        "Total Transaction Amount",
                        f"${payment_losses['total_amount'].sum():,.2f}"
                    )
                    
                    # Show detailed breakdown
                    st.write("### Detailed Breakdown")
                    st.dataframe(
                        payment_losses,
                        column_config={
                            "payment_method": st.column_config.TextColumn(
                                "Payment Method",
                                width="medium"
                            ),
                            "transaction_count": st.column_config.NumberColumn(
                                "# Transactions",
                                width="small"
                            ),
                            "total_fees": st.column_config.NumberColumn(
                                "Fees (3%)",
                                format="$%.2f",
                                width="medium"
                            ),
                            "total_amount": st.column_config.NumberColumn(
                                "Total Amount",
                                format="$%.2f",
                                width="medium"
                            )
                        },
                        hide_index=True,
                        use_container_width=True
                    )

        else:
            st.info("No high risk users found")

st.title("Deriv Fraud Detection Dashboard")
st.subheader("Anti-Fraud Team Analysis Tool")

# Create tabs for different views
tab1, tab2 = st.tabs(["High Risk Users", "AI Analysis"])

with tab1:
    display_high_risk_users()

with tab2:
    display_user_analysis()

def display_descriptive_stats():
    st.header("User Information")
    user_data = run_query('SELECT * FROM user')

    col1, col2, col3, col4 = st.columns(4)

    # user_id
    # user_email
    # user_profession
    # user_country
    # user_createdat
    # user_lastlogin
    # user_kycstatus
    # user_status

    col1.metric(
        "Users", 
        f"{user_data['user_id'].nunique()}"
    )
    col2.metric(
        "Professions", 
        f"{user_data['user_profession'].nunique()}"
    )
    col3.metric(
        "Countries",
        f"{user_data['user_country'].nunique()}"
    )
    col4.metric(
        "Locked Accounts",
        f"{user_data['user_status'].value_counts()['locked']}"
    )

    st.header("Transaction Information")
    transaction_data = run_query('SELECT * FROM user_transaction')

    cols_ = st.columns(6)

    # transaction_id
    # transaction_userid
    # transaction_datetime
    # transaction_type
    # transaction_amount
    # transaction_paymentmethod

    cols_[0].metric(
        "Total Transactions", 
        f"{transaction_data['transaction_id'].nunique()}"
    )
    cols_[1].metric(
        "Total Deposits", 
        f"{transaction_data['transaction_type'].value_counts()['deposit']}"
    )
    cols_[2].metric(
        "Total Withdrawals",
        f"{transaction_data['transaction_type'].value_counts()['withdrawal']}"
    )
    cols_[3].metric(
        "Total Amount Deposited (RM)",
        f"{transaction_data[transaction_data['transaction_type'] == 'deposit']['transaction_amount'].sum():,.2f}"
    )
    cols_[4].metric(
        "Total Amount Withdrawn (RM)",
        f"{transaction_data[transaction_data['transaction_type'] == 'withdrawal']['transaction_amount'].sum():,.2f}"
    )
    cols_[5].metric(
        "Total Payment Methods",
        f"{transaction_data['transaction_paymentmethod'].nunique()}"
    )

    st.header("Trading Information")
    trade_data = run_query('SELECT * FROM user_trading')

    cols_ = st.columns(7)

    # trade_id
    # trade_userid
    # trade_timestamp
    # trade_type
    # trade_market
    # trade_pair
    # trade_volume
    # trade_openprice
    # trade_closeprice
    # trade_currentprice
    # trade_profit
    # trade_platform

    cols_[0].metric(
        "Total Trades", 
        f"{trade_data['trade_id'].nunique()}"
    )
    cols_[1].metric(
        "Total Buys", 
        f"{trade_data['trade_type'].value_counts()['buy']}"
    )
    cols_[2].metric(
        "Total Sells",
        f"{trade_data['trade_type'].value_counts()['sell']}"
    )
    cols_[3].metric(
        "Total Volume",
        f"{trade_data['trade_volume'].sum():,.2f}"
    )
    cols_[4].metric(
        "Total Profit",
        f"{trade_data['trade_profit'].sum():,.2f}"
    )

    cols_[5].metric(
        "Top Market",
        f"{trade_data['trade_market'].value_counts().idxmax()}"
    )

    cols_[6].metric(
        "Top Pair",
        f"{trade_data['trade_pair'].value_counts().idxmax()}"
    )

    # Payment Method Dist ribution
    cols = st.columns(2)

    with cols[0]:
        st.subheader("Payment Method Distribution")
        payment_data = transaction_data['transaction_paymentmethod'].value_counts()
        fig_payment = px.pie(
            values=payment_data.values,
            names=payment_data.index,
            title="Distribution by Payment Method"
        )
        st.plotly_chart(fig_payment)

    with cols[1]:
        st.subheader("Market Distribution") 
        market_data = trade_data['trade_market'].value_counts()
        fig_market = px.pie(
            values=market_data.values,
            names=market_data.index,
            title="Distribution by Market"
        )   
        st.plotly_chart(fig_market)

# # Suspicious Groups Analysis
# st.header("Suspicious Group Patterns")
# group_data = run_query('SELECT * FROM suspicious_groups')
# fig_groups = px.scatter(
#     group_data,
#     x='region',
#     y='client_count',
#     size='client_count',
#     color='platform',
#     hover_data=['payment_method', 'client_ids'],
#     title="Suspicious Groups by Region and Platform"
# )
# st.plotly_chart(fig_groups)

# # Detailed Suspicious Accounts
# st.header("Suspicious Account Details")
# suspicious_accounts = run_query('''
#     SELECT 
#         sa.client_id,
#         datetime(sa.detection_date) as detected_on,
#         sa.platform,
#         sa.market_type,
#         sa.trade_type,
#         fi.amount as transaction_amount,
#         fi.payment_method,
#         CASE 
#             WHEN la.lock_date IS NOT NULL THEN 'Locked 🔒'
#             ELSE 'Active ✅'
#         END as account_status,
#         la.lock_date
#     FROM suspicious_accounts sa
#     LEFT JOIN financial_impact fi ON sa.client_id = fi.client_id
#     LEFT JOIN locked_accounts la ON sa.client_id = la.client_id
#     ORDER BY sa.detection_date DESC
# ''')

# # Add AI Decision column
# decisions = []
# for _, row in suspicious_accounts.iterrows():
#     if st.button(f"Analyze Client {row['client_id']}", key=f"analyze_{row['client_id']}"):
#         with st.spinner("AI analyzing client..."):
#             # Get comprehensive client data
#             client_data = run_query(f'''
#                 SELECT 
#                     sa.*,
#                     fi.amount,
#                     fi.fee,
#                     fi.payment_method,
#                     CASE 
#                         WHEN la.client_id IS NOT NULL THEN 'Locked'
#                         ELSE 'Active'
#                     END as current_status
#                 FROM suspicious_accounts sa
#                 LEFT JOIN financial_impact fi ON sa.client_id = fi.client_id
#                 LEFT JOIN locked_accounts la ON sa.client_id = la.client_id
#                 WHERE sa.client_id = ?
#             ''', [row['client_id']]).to_dict('records')[0]
            
#             decision = get_ai_decision(client_data)
#             st.write(f"### AI Analysis for Client {row['client_id']}")
#             st.write(decision)

# # Display enhanced dataframe
# st.dataframe(
#     suspicious_accounts,
#     column_config={
#         "client_id": st.column_config.NumberColumn(
#             "Client ID",
#             help="Unique identifier for the client"
#         ),
#         "detected_on": st.column_config.DatetimeColumn(
#             "Detected On",
#             format="D MMM YYYY, HH:mm"
#         ),
#         "platform": st.column_config.TextColumn("Trading Platform"),
#         "market_type": st.column_config.TextColumn("Market"),
#         "trade_type": st.column_config.TextColumn("Trade Type"),
#         "transaction_amount": st.column_config.NumberColumn(
#             "Transaction Amount",
#             format="$%.2f"
#         ),
#         "payment_method": st.column_config.TextColumn("Payment Method"),
#         "account_status": st.column_config.TextColumn(
#             "Account Status",
#             help="Account status: Locked 🔒 or Active ✅"
#         ),
#         "lock_date": st.column_config.DatetimeColumn(
#             "Lock Date",
#             format="D MMM YYYY, HH:mm",
#             help="Date when account was locked"
#         )
#     },
#     hide_index=True
# )

# # Geographic Distribution
# st.header("Geographic Distribution")
# country_data = run_query('''
#     SELECT 
#         sa.country,  -- Using country field from suspicious_accounts
#         COUNT(*) as client_count,
#         AVG(sa.risk_score) as avg_risk,
#         AVG(fi.amount) as avg_volume,
#         COUNT(CASE WHEN sa.platform = 'Deriv MT5' THEN 1 END) as platform_count
#     FROM suspicious_accounts sa
#     LEFT JOIN financial_impact fi ON sa.client_id = fi.client_id
#     GROUP BY sa.country
# ''')

# # Add this before creating the choropleth map
# COUNTRY_NAME_MAP = {
#     'UK': 'United Kingdom',
#     'USA': 'United States',
#     'UAE': 'United Arab Emirates',
#     # Add more mappings as needed
# }

# # Update country names in the dataframe
# country_data['country'] = country_data['country'].map(COUNTRY_NAME_MAP).fillna(country_data['country'])

# # Create two columns for country stats
# col1, col2 = st.columns(2)

# with col1:
#     # Country distribution map
#     fig_map = px.choropleth(
#         country_data,
#         locations=country_data['country'],
#         locationmode="country names",
#         color="avg_risk",
#         hover_data=["client_count", "avg_volume", "platform_count"],
#         title="Risk Distribution by Country",
#         color_continuous_scale="Reds"
#     )
#     st.plotly_chart(fig_map)

# with col2:
#     # Country metrics table
#     st.dataframe(
#         country_data,
#         column_config={
#             "country": "Country",
#             "client_count": st.column_config.NumberColumn("Suspicious Clients"),
#             "avg_risk": st.column_config.NumberColumn("Avg Risk Score", format="%.2f"),
#             "avg_volume": st.column_config.NumberColumn("Avg Volume", format="$%.2f"),
#             "platform_count": st.column_config.NumberColumn("MT5 Users")
#         },
#         hide_index=True
#     )

# # Country Details Section
# st.header("Country Risk Analysis")
# selected_country = st.selectbox(
#     "Select Country for Detailed Analysis",
#     options=country_data['country'].unique()
# )

# if selected_country:
#     country_details = run_query(f'''
#         SELECT 
#             sa.*,
#             fi.total_deposits,
#             fi.total_withdrawals,
#             fi.payment_method,
#             fi.chargeback_count,
#             COALESCE(ta. unt, 0) as trade_count,
#             COALESCE(ta.total_profit_loss, 0) as total_pnl
#         FROM suspicious_accounts sa
#         LEFT JOIN financial_impact fi ON sa.client_id = fi.client_id
#         LEFT JOIN (
#             SELECT 
#                 client_id,
#                 COUNT(*) as trade_count,
#                 SUM(profit_loss) as total_profit_loss
#             FROM trading_activity
#             GROUP BY client_id
#         ) ta ON sa.client_id = ta.client_id
#         WHERE sa.country = ?
#     ''', [selected_country])

#     # Show country statistics
#     st.subheader(f"Risk Profile: {selected_country}")
#     col1, col2, col3, col4 = st.columns(4)
    
#     col1.metric(
#         "Average Risk Score", 
#         f"{country_details['risk_score'].mean():.1f}"
#     )
#     col2.metric(
#         "Total Trading Volume", 
#         f"${country_details['trading_volume'].sum():,.0f}"
#     )
#     col3.metric(
#         "Failed KYC Rate", 
#         f"{(country_details['kyc_status'] == 'Failed').mean()*100:.1f}%"
#     )
#     col4.metric(
#         "Avg Profit/Loss", 
#         f"${country_details['total_pnl'].mean():,.2f}"
#     )

#     # Show client details for the country
#     st.subheader(f"Suspicious Clients in {selected_country}")
    
#     # Enhanced client details with tabs
#     client_tabs = st.tabs(["Overview", "Trading Activity", "Transaction Activity"])
    
#     with client_tabs[0]:
#         st.dataframe(
#             country_details[[
#                 'client_id', 'profession', 'annual_income', 
#                 'risk_score', 'kyc_status', 'trade_frequency'
#             ]],
#             column_config={
#                 "client_id": "Client ID",
#                 "profession": "Profession",
#                 "annual_income": st.column_config.NumberColumn("Annual Income", format="$%.2f"),
#                 "risk_score": st.column_config.NumberColumn("Risk Score", format="%.1f"),
#                 "kyc_status": "KYC Status",
#                 "trade_frequency": "Trading Frequency"
#             },
#             hide_index=True
#         )

#     with client_tabs[1]:
#         # Get trading activity for selected country
#         trading_data = run_query(f'''
#             SELECT 
#                 ta.*,
#                 sa.country
#             FROM trading_activity ta
#             JOIN suspicious_accounts sa ON ta.client_id = sa.client_id
#             WHERE sa.country = ?
#             ORDER BY trade_date DESC
#         ''', [selected_country])
        
#         st.dataframe(
#             trading_data,
#             column_config={
#                 "trade_date": st.column_config.DatetimeColumn("Trade Date"),
#                 "position_size": st.column_config.NumberColumn("Position Size", format="$%.2f"),
#                 "profit_loss": st.column_config.NumberColumn("P/L", format="$%.2f"),
#                 "leverage": "Leverage",
#                 "strategy_type": "Strategy"
#             },
#             hide_index=True
#         )

#     with client_tabs[2]:
#         # Get financial activity
#         financial_data = run_query(f'''
#             SELECT 
#                 fi.*,
#                 sa.country
#             FROM financial_impact fi
#             JOIN suspicious_accounts sa ON fi.client_id = sa.client_id
#             WHERE sa.country = ?
#         ''', [selected_country])
        
#         st.dataframe(
#             financial_data[[
#                 'client_id', 'total_deposits', 'total_withdrawals',
#                 'deposit_frequency', 'withdrawal_frequency', 'chargeback_count'
#             ]],
#             column_config={
#                 "total_deposits": st.column_config.NumberColumn("Total Deposits", format="$%.2f"),
#                 "total_withdrawals": st.column_config.NumberColumn("Total Withdrawals", format="$%.2f"),
#                 "chargeback_count": "Chargebacks"
#             },
#             hide_index=True
#         )



#     if st.button("Assess Risks"):
#         with st.spinner("AI assessing risks..."):
#             # Summarize data before sending
#             risk_summary = risk_data.groupby(['region', 'platform'])['client_count'].sum().to_string()
#             analysis = get_ai_analysis(
#                 risk_summary,
#                 "Assess risk levels by region and platform, highlighting highest risk areas."
#             )
#             st.write(analysis)

# with analysis_tab3:
#     st.subheader("Action Recommendations")
#     if st.button("Generate Recommendations"):
#         with st.spinner("AI generating recommendations..."):
#             # Summarize data before sending
#             all_data = {
#                 "suspicious_accounts": run_query("SELECT * FROM suspicious_accounts").head(5).to_string(),
#                 "financial_impact": run_query("SELECT * FROM financial_impact").agg({
#                     'fee': 'sum',
#                     'amount': 'sum'
#                 }).to_string(),
#                 "groups": run_query("SELECT region, COUNT(*) as count FROM suspicious_groups GROUP BY region").to_string()
#             }
#             analysis = get_ai_analysis(
#                 json.dumps(all_data),
#                 "Provide specific action recommendations for the anti-fraud team based on this data."
#             )
#             st.write(analysis)
    
#     print(analysis)

# # Add AI Chat Interface
# st.sidebar.header("💬 AI Assistant")
# if "messages" not in st.session_state:
#     st.session_state.messages = []

# for message in st.session_state.messages:
#     with st.sidebar.chat_message(message["role"]):
#         st.markdown(message["content"])

# if prompt := st.sidebar.chat_input("Ask about fraud patterns..."):
#     st.session_state.messages.append({"role": "user", "content": prompt})
#     with st.sidebar.chat_message("user"):
#         st.markdown(prompt)

#     with st.sidebar.chat_message("assistant"):
#         with st.spinner("Thinking..."):
#             all_data = {
#                 "suspicious_accounts": run_query("SELECT * FROM suspicious_accounts").head().to_string(),
#                 "financial_impact": run_query("SELECT * FROM financial_impact").head().to_string()
#             }
#             response = get_ai_analysis(
#                 json.dumps(all_data),
#                 f"Based on this fraud detection data, please answer: {prompt}"
#             )
#             st.markdown(response)
#     st.session_state.messages.append({"role": "assistant", "content": response})

# # Add this to the AI analysis section
# if st.button("Analyze Country Patterns"):
#     with st.spinner("AI analyzing country patterns..."):
#         country_analysis = get_ai_analysis(
#             country_data.to_string(),
#             f"""Analyze the fraud patterns across different countries, focusing on:
#             1. High-risk countries and their characteristics
#             2. Common patterns in each region
#             3. KYC failure patterns
#             4. Trading behavior variations by country
            
#             Provide specific insights for {selected_country} if available."""
#         )
#         st.write(country_analysis)

# # Non-Trading and Fake Trading Detection
# st.header("🚨 Suspicious Trading Patterns")
# tab_non_trading, tab_fake_trading, tab_groups = st.tabs([
#     "Non-Trading Clients", 
#     "Suspicious Trading Patterns",
#     "Group Analysis"
# ])

# with tab_non_trading:
#     st.subheader("Clients with Deposits but No Trading")
#     non_trading_clients = run_query('''
#         WITH client_activity AS (
#             SELECT 
#                 fi.client_id,
#                 sa.platform,
#                 sa.country,
#                 fi.total_deposits,
#                 fi.total_withdrawals,
#                 COALESCE(ta.trade_count, 0) as trade_count,
#                 fi.payment_method,
#                 sa.detection_date
#             FROM financial_impact fi
#             JOIN suspicious_accounts sa ON fi.client_id = sa.client_id
#             LEFT JOIN (
#                 SELECT client_id, COUNT(*) as trade_count
#                 FROM trading_activity
#                 GROUP BY client_id
#             ) ta ON fi.client_id = ta.client_id
#             WHERE fi.total_deposits > 0
#         )
#         SELECT *
#         FROM client_activity
#         WHERE trade_count = 0
#         ORDER BY total_deposits DESC
#     ''')
    
#     # Display non-trading clients metrics
#     col1, col2, col3 = st.columns(3)
#     col1.metric(
#         "Non-Trading Clients", 
#         len(non_trading_clients)
#     )
#     col2.metric(
#         "Total Deposits", 
#         f"${non_trading_clients['total_deposits'].sum():,.2f}"
#     )
#     col3.metric(
#         "Total Withdrawals", 
#         f"${non_trading_clients['total_withdrawals'].sum():,.2f}"
#     )
    
#     # Display detailed table
#     st.dataframe(
#         non_trading_clients,
#         column_config={
#             "client_id": "Client ID",
#             "platform": "Platform",
#             "country": "Country",
#             "total_deposits": st.column_config.NumberColumn(
#                 "Total Deposits",
#                 format="$%.2f"
#             ),
#             "total_withdrawals": st.column_config.NumberColumn(
#                 "Total Withdrawals",
#                 format="$%.2f"
#             ),
#             "payment_method": "Payment Method",
#             "detection_date": st.column_config.DatetimeColumn(
#                 "Detected On",
#                 format="D MMM YYYY, HH:mm"
#             )
#         },
#         hide_index=True
#     )

# with tab_fake_trading:
#     st.subheader("Suspicious Trading Patterns")
#     fake_trading_clients = run_query('''
#         WITH trading_metrics AS (
#             SELECT 
#                 ta.client_id,
#                 COUNT(*) as trade_count,
#                 AVG(ta.profit_loss) as avg_pnl,
#                 AVG(ta.position_size) as avg_position,
#                 MAX(ta.trade_date) - MIN(ta.trade_date) as trading_duration,
#                 COUNT(*) * 1.0 / 
#                     (JULIANDAY(MAX(ta.trade_date)) - JULIANDAY(MIN(ta.trade_date))) as trades_per_day
#             FROM trading_activity ta
#             GROUP BY ta.client_id
#             HAVING 
#                 COUNT(*) > 10  -- Minimum trades to analyze
#                 AND AVG(ABS(ta.profit_loss)) < 1.0  -- Very small P/L
#                 AND trades_per_day > 5  -- High frequency
#         )
#         SELECT 
#             tm.*,
#             sa.platform,
#             sa.country,
#             fi.total_deposits,
#             fi.payment_method
#         FROM trading_metrics tm
#         JOIN suspicious_accounts sa ON tm.client_id = sa.client_id
#         JOIN financial_impact fi ON tm.client_id = fi.client_id
#         ORDER BY trades_per_day DESC
#     ''')
    
#     # Display fake trading metrics
#     col1, col2, col3 = st.columns(3)
#     col1.metric(
#         "Suspicious Traders", 
#         len(fake_trading_clients)
#     )
#     col2.metric(
#         "Avg Trades/Day", 
#         f"{fake_trading_clients['trades_per_day'].mean():.1f}"
#     )
#     col3.metric(
#         "Avg P/L", 
#         f"${fake_trading_clients['avg_pnl'].mean():.2f}"
#     )
    
#     # Display detailed table
#     st.dataframe(
#         fake_trading_clients,
#         column_config={
#             "client_id": "Client ID",
#             "trade_count": "Total Trades",
#             "avg_pnl": st.column_config.NumberColumn(
#                 "Avg P/L",
#                 format="$%.2f"
#             ),
#             "trades_per_day": st.column_config.NumberColumn(
#                 "Trades/Day",
#                 format="%.1f"
#             ),
#             "platform": "Platform",
#             "country": "Country",
#             "payment_method": "Payment Method"
#         },
#         hide_index=True
#     )

# with tab_groups:
#     st.subheader("Similar Behavior Groups")
    
#     # Get group patterns
#     group_patterns = run_query('''
#         WITH client_groups AS (
#             SELECT 
#                 sa.country,
#                 sa.platform,
#                 fi.payment_method,
#                 COUNT(DISTINCT sa.client_id) as client_count,
#                 GROUP_CONCAT(sa.client_id) as client_ids,
#                 AVG(COALESCE(ta.trade_count, 0)) as avg_trades,
#                 AVG(fi.total_deposits) as avg_deposits
#             FROM suspicious_accounts sa
#             JOIN financial_impact fi ON sa.client_id = fi.client_id
#             LEFT JOIN (
#                 SELECT client_id, COUNT(*) as trade_count
#                 FROM trading_activity
#                 GROUP BY client_id
#             ) ta ON sa.client_id = ta.client_id
#             GROUP BY sa.country, sa.platform, fi.payment_method
#             HAVING COUNT(DISTINCT sa.client_id) >= 2
#         )
#         SELECT *
#         FROM client_groups
#         ORDER BY client_count DESC, avg_deposits DESC
#     ''')
    
#     # Display group metrics
#     col1, col2 = st.columns(2)
    
#     # Group scatter plot
#     fig_groups = px.scatter(
#         group_patterns,
#         x="avg_deposits",
#         y="avg_trades",
#         size="client_count",
#         color="platform",
#         hover_data=["country", "payment_method", "client_ids"],
#         title="Group Behavior Analysis"
#     )
#     col1.plotly_chart(fig_groups)
    
#     # Group details table
#     col2.dataframe(
#         group_patterns,
#         column_config={
#             "country": "Country",
#             "platform": "Platform",
#             "payment_method": "Payment Method",
#             "client_count": "# of Clients",
#             "avg_trades": st.column_config.NumberColumn(
#                 "Avg Trades",
#                 format="%.1f"
#             ),
#             "avg_deposits": st.column_config.NumberColumn(
#                 "Avg Deposits",
#                 format="$%.2f"
#             ),
#             "client_ids": "Client IDs"
#         },
#         hide_index=True
#     )

# # Add action buttons for bulk operations
# st.header("🔒 Bulk Actions")
# col1, col2 = st.columns(2)

# with col1:
#     if st.button("Lock Non-Trading Accounts"):
#         with st.spinner("Processing..."):
#             for _, client in non_trading_clients.iterrows():
#                 run_query('''
#                     INSERT OR REPLACE INTO locked_accounts (client_id, lock_date)
#                     VALUES (?, CURRENT_TIMESTAMP)
#                 ''', [client['client_id']])
#             st.success(f"Locked {len(non_trading_clients)} accounts")

# with col2:
#     if st.button("Lock Suspicious Trading Accounts"):
#         with st.spinner("Processing..."):
#             for _, client in fake_trading_clients.iterrows():
#                 run_query('''
#                     INSERT OR REPLACE INTO locked_accounts (client_id, lock_date)
#                     VALUES (?, CURRENT_TIMESTAMP)
#                 ''', [client['client_id']])
#             st.success(f"Locked {len(fake_trading_clients)} accounts")

# # Add financial impact analysis
# st.header("💰 Financial Impact Analysis")
# impact_data = run_query('''
#     SELECT 
#         COALESCE(nt.impact_type, 'Suspicious Trading') as activity_type,
#         COUNT(DISTINCT fi.client_id) as client_count,
#         SUM(fi.fee) as total_fees,
#         SUM(fi.amount) as total_volume
#     FROM financial_impact fi
#     LEFT JOIN (
#         SELECT 
#             subq.client_id,
#             'Non-Trading' as impact_type
#         FROM (
#             SELECT fi2.client_id
#             FROM financial_impact fi2
#             LEFT JOIN trading_activity ta ON fi2.client_id = ta.client_id
#             GROUP BY fi2.client_id
#             HAVING COUNT(ta.client_id) = 0
#         ) subq
#     ) nt ON fi.client_id = nt.client_id
#     GROUP BY COALESCE(nt.impact_type, 'Suspicious Trading')
# ''')

# col1, col2 = st.columns(2)

# with col1:
#     # Impact metrics
#     st.dataframe(
#         impact_data,
#         column_config={
#             "activity_type": "Activity Type",
#             "client_count": "# of Clients",
#             "total_fees": st.column_config.NumberColumn(
#                 "Total Fees",
#                 format="$%.2f"
#             ),
#             "total_volume": st.column_config.NumberColumn(
#                 "Total Volume",
#                 format="$%.2f"
#             )
#         },
#         hide_index=True
#     )

# with col2:
#     # Impact visualization
#     fig_impact = px.pie(
#         impact_data,
#         values="total_fees",
#         names="activity_type",
#         title="Distribution of Financial Impact"
#     )
#     st.plotly_chart(fig_impact) 
```

# dashboard/app.py

```py
import streamlit as st
import pandas as pd
import plotly.express as px
import sqlite3
import os
import requests
import json

st.set_page_config(page_title="Deriv Fraud Detection Dashboard", page_icon="🔒", layout="wide")

# Setup database connection
def get_connection():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(os.path.dirname(current_dir), 'database', 'fraud_db.sqlite')
    return sqlite3.connect(db_path)

# Helper function to run SQL queries
@st.cache_data
def run_query(query, params=None):
    with get_connection() as conn:
        if params:
            return pd.read_sql(query, conn, params=params)
        return pd.read_sql(query, conn)

# Add Ollama AI helper function
def get_ai_analysis(data, prompt):
    try:
        # Increase timeout and add connection timeout
        response = requests.post(
            'http://localhost:11434/api/generate', 
            json={
                "model": "deepseek-r1:1.5b",
                "prompt": f"""As a fraud detection expert at Deriv, analyze this data and provide insights:
                {data}
                
                {prompt}
                
                Provide a detailed analysis focusing on:
                1. Suspicious patterns and anomalies
                2. Risk indicators and severity levels
                3. Potential fraud scenarios
                4. Recommended actions with justification
                
                Format your response in clear sections with bullet points.""",
                "stream": False,
                "options": {
                    "temperature": 0.4,
                    "top_p": 0.9,
                    "num_predict": 8192,  # Increased from 256
                    "top_k": 40,
                    "repeat_penalty": 1.1
                }
            }, 
            timeout=(5, 120)  # Increased timeout
        )
        
        if response.status_code != 200:
            return f"API Error: Status code {response.status_code}"
            
        response_data = response.json()
        if 'response' not in response_data:
            return f"API Error: Unexpected response format - {str(response_data)}"
            
        return response_data['response']
        
    except requests.exceptions.ConnectTimeout:
        return "Error: Cannot connect to Ollama (connection timeout). Make sure Ollama is running."
    except requests.exceptions.ReadTimeout:
        return "Error: Ollama response took too long. Try again or use a smaller data sample."
    except requests.exceptions.ConnectionError:
        return "Error: Cannot connect to Ollama. Make sure Ollama is running (ollama run deepseek-r1:7b-qwen-distill-q8_0)"
    except Exception as e:
        return f"AI Analysis Error: {str(e)}\nFull error: {type(e).__name__}"

# Add AI decision function
def get_ai_decision(client_data):
    try:
        prompt = f"""As a senior fraud detection expert at Deriv, analyze this client data and provide a comprehensive decision:
        {client_data}
        
        Consider the following factors:
        1. Transaction patterns and amounts
        2. Platform usage and trading behavior
        3. Payment methods and frequency
        4. Account history and status
        5. Regional risk factors
        6. Similar patterns in other accounts
        
        Provide a detailed decision in the following format:

        RISK ASSESSMENT:
        - Risk Level: [High/Medium/Low]
        - Primary Risk Factors:
          * [List key risk indicators]
          * [Include specific metrics/thresholds]
        
        RECOMMENDED ACTIONS:
        1. Immediate Action: [Lock Account/Enhanced Monitoring/Clear]
        2. Additional Steps:
           * [List specific actions]
           * [Include timeline recommendations]
        
        JUSTIFICATION:
        - [Detailed explanation of decision]
        - [Reference to specific data points]
        - [Comparison to known fraud patterns]
        
        INVESTIGATION REQUIREMENTS:
        1. Priority Areas:
           * [List specific areas to investigate]
           * [Include required documentation]
        2. Follow-up Actions:
           * [List verification steps]
           * [Include timeline]
        """
        
        response = requests.post(
            'http://localhost:11434/api/generate', 
            json={
                "model": "deepseek-r1:1.5b",
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.7,
                    "top_p": 0.9,
                    "num_predict": 2048,  # Increased significantly
                    "top_k": 40,
                    "repeat_penalty": 1.1
                }
            }, 
            timeout=(5, 180)  # Increased timeout for longer analysis
        )
        
        if response.status_code == 200:
            return response.json()['response']
        return "Error: Unable to get AI decision"
        
    except Exception as e:
        return f"Decision Error: {str(e)}"

st.title("Deriv Fraud Detection Dashboard")
st.subheader("Anti-Fraud Team Analysis Tool")

# Sidebar filters
st.sidebar.header("Filters")
selected_platform = st.sidebar.multiselect(
    "Platform",
    ["Deriv MT5", "Deriv cTrader", "Deriv X", "Deriv GO", "Deriv Trader"],
    default=["Deriv MT5"]
)

selected_market = st.sidebar.multiselect(
    "Market",
    ["Forex", "Derived Indices", "Stocks", "Cryptocurrencies", "Commodities"],
    default=["Forex"]
)

# Financial Impact Analysis
st.header("Financial Impact Analysis")
financial_data = run_query('SELECT * FROM financial_impact')
col1, col2, col3 = st.columns(3)
col1.metric(
    "Total Fees Absorbed", 
    f"${financial_data['fee'].sum():,.2f}"
)
col2.metric(
    "Total Transaction Volume", 
    f"${financial_data['amount'].sum():,.2f}"
)
col3.metric(
    "Suspicious Accounts",
    len(financial_data)
)

# Payment Method Distribution
st.subheader("Payment Method Distribution")
payment_data = financial_data['payment_method'].value_counts()
fig_payment = px.pie(
    values=payment_data.values,
    names=payment_data.index,
    title="Distribution by Payment Method"
)
st.plotly_chart(fig_payment)

# Suspicious Groups Analysis
st.header("Suspicious Group Patterns")
group_data = run_query('SELECT * FROM suspicious_groups')
fig_groups = px.scatter(
    group_data,
    x='region',
    y='client_count',
    size='client_count',
    color='platform',
    hover_data=['payment_method', 'client_ids'],
    title="Suspicious Groups by Region and Platform"
)
st.plotly_chart(fig_groups)

# Detailed Suspicious Accounts
st.header("Suspicious Account Details")
suspicious_accounts = run_query('''
    SELECT 
        sa.client_id,
        datetime(sa.detection_date) as detected_on,
        sa.platform,
        sa.market_type,
        sa.trade_type,
        fi.amount as transaction_amount,
        fi.payment_method,
        CASE 
            WHEN la.lock_date IS NOT NULL THEN 'Locked 🔒'
            ELSE 'Active ✅'
        END as account_status,
        la.lock_date
    FROM suspicious_accounts sa
    LEFT JOIN financial_impact fi ON sa.client_id = fi.client_id
    LEFT JOIN locked_accounts la ON sa.client_id = la.client_id
    ORDER BY sa.detection_date DESC
''')

# Add AI Decision column
decisions = []
for _, row in suspicious_accounts.iterrows():
    if st.button(f"Analyze Client {row['client_id']}", key=f"analyze_{row['client_id']}"):
        with st.spinner("AI analyzing client..."):
            # Get comprehensive client data
            client_data = run_query(f'''
                SELECT 
                    sa.*,
                    fi.amount,
                    fi.fee,
                    fi.payment_method,
                    CASE 
                        WHEN la.client_id IS NOT NULL THEN 'Locked'
                        ELSE 'Active'
                    END as current_status
                FROM suspicious_accounts sa
                LEFT JOIN financial_impact fi ON sa.client_id = fi.client_id
                LEFT JOIN locked_accounts la ON sa.client_id = la.client_id
                WHERE sa.client_id = ?
            ''', [row['client_id']]).to_dict('records')[0]
            
            decision = get_ai_decision(client_data)
            st.write(f"### AI Analysis for Client {row['client_id']}")
            st.write(decision)

# Display enhanced dataframe
st.dataframe(
    suspicious_accounts,
    column_config={
        "client_id": st.column_config.NumberColumn(
            "Client ID",
            help="Unique identifier for the client"
        ),
        "detected_on": st.column_config.DatetimeColumn(
            "Detected On",
            format="D MMM YYYY, HH:mm"
        ),
        "platform": st.column_config.TextColumn("Trading Platform"),
        "market_type": st.column_config.TextColumn("Market"),
        "trade_type": st.column_config.TextColumn("Trade Type"),
        "transaction_amount": st.column_config.NumberColumn(
            "Transaction Amount",
            format="$%.2f"
        ),
        "payment_method": st.column_config.TextColumn("Payment Method"),
        "account_status": st.column_config.TextColumn(
            "Account Status",
            help="Account status: Locked 🔒 or Active ✅"
        ),
        "lock_date": st.column_config.DatetimeColumn(
            "Lock Date",
            format="D MMM YYYY, HH:mm",
            help="Date when account was locked"
        )
    },
    hide_index=True
)

# Geographic Distribution
st.header("Geographic Distribution")
country_data = run_query('''
    SELECT 
        sa.country,  -- Using country field from suspicious_accounts
        COUNT(*) as client_count,
        AVG(sa.risk_score) as avg_risk,
        AVG(fi.amount) as avg_volume,
        COUNT(CASE WHEN sa.platform = 'Deriv MT5' THEN 1 END) as platform_count
    FROM suspicious_accounts sa
    LEFT JOIN financial_impact fi ON sa.client_id = fi.client_id
    GROUP BY sa.country
''')

# Add this before creating the choropleth map
COUNTRY_NAME_MAP = {
    'UK': 'United Kingdom',
    'USA': 'United States',
    'UAE': 'United Arab Emirates',
    # Add more mappings as needed
}

# Update country names in the dataframe
country_data['country'] = country_data['country'].map(COUNTRY_NAME_MAP).fillna(country_data['country'])

# Create two columns for country stats
col1, col2 = st.columns(2)

with col1:
    # Country distribution map
    fig_map = px.choropleth(
        country_data,
        locations=country_data['country'],
        locationmode="country names",
        color="avg_risk",
        hover_data=["client_count", "avg_volume", "platform_count"],
        title="Risk Distribution by Country",
        color_continuous_scale="Reds"
    )
    st.plotly_chart(fig_map)

with col2:
    # Country metrics table
    st.dataframe(
        country_data,
        column_config={
            "country": "Country",
            "client_count": st.column_config.NumberColumn("Suspicious Clients"),
            "avg_risk": st.column_config.NumberColumn("Avg Risk Score", format="%.2f"),
            "avg_volume": st.column_config.NumberColumn("Avg Volume", format="$%.2f"),
            "platform_count": st.column_config.NumberColumn("MT5 Users")
        },
        hide_index=True
    )

# Country Details Section
st.header("Country Risk Analysis")
selected_country = st.selectbox(
    "Select Country for Detailed Analysis",
    options=country_data['country'].unique()
)

if selected_country:
    country_details = run_query(f'''
        SELECT 
            sa.*,
            fi.total_deposits,
            fi.total_withdrawals,
            fi.payment_method,
            fi.chargeback_count,
            COALESCE(ta.trade_count, 0) as trade_count,
            COALESCE(ta.total_profit_loss, 0) as total_pnl
        FROM suspicious_accounts sa
        LEFT JOIN financial_impact fi ON sa.client_id = fi.client_id
        LEFT JOIN (
            SELECT 
                client_id,
                COUNT(*) as trade_count,
                SUM(profit_loss) as total_profit_loss
            FROM trading_activity
            GROUP BY client_id
        ) ta ON sa.client_id = ta.client_id
        WHERE sa.country = ?
    ''', [selected_country])

    # Show country statistics
    st.subheader(f"Risk Profile: {selected_country}")
    col1, col2, col3, col4 = st.columns(4)
    
    col1.metric(
        "Average Risk Score", 
        f"{country_details['risk_score'].mean():.1f}"
    )
    col2.metric(
        "Total Trading Volume", 
        f"${country_details['trading_volume'].sum():,.0f}"
    )
    col3.metric(
        "Failed KYC Rate", 
        f"{(country_details['kyc_status'] == 'Failed').mean()*100:.1f}%"
    )
    col4.metric(
        "Avg Profit/Loss", 
        f"${country_details['total_pnl'].mean():,.2f}"
    )

    # Show client details for the country
    st.subheader(f"Suspicious Clients in {selected_country}")
    
    # Enhanced client details with tabs
    client_tabs = st.tabs(["Overview", "Trading Activity", "Financial Activity"])
    
    with client_tabs[0]:
        st.dataframe(
            country_details[[
                'client_id', 'profession', 'annual_income', 
                'risk_score', 'kyc_status', 'trade_frequency'
            ]],
            column_config={
                "client_id": "Client ID",
                "profession": "Profession",
                "annual_income": st.column_config.NumberColumn("Annual Income", format="$%.2f"),
                "risk_score": st.column_config.NumberColumn("Risk Score", format="%.1f"),
                "kyc_status": "KYC Status",
                "trade_frequency": "Trading Frequency"
            },
            hide_index=True
        )

    with client_tabs[1]:
        # Get trading activity for selected country
        trading_data = run_query(f'''
            SELECT 
                ta.*,
                sa.country
            FROM trading_activity ta
            JOIN suspicious_accounts sa ON ta.client_id = sa.client_id
            WHERE sa.country = ?
            ORDER BY trade_date DESC
        ''', [selected_country])
        
        st.dataframe(
            trading_data,
            column_config={
                "trade_date": st.column_config.DatetimeColumn("Trade Date"),
                "position_size": st.column_config.NumberColumn("Position Size", format="$%.2f"),
                "profit_loss": st.column_config.NumberColumn("P/L", format="$%.2f"),
                "leverage": "Leverage",
                "strategy_type": "Strategy"
            },
            hide_index=True
        )

    with client_tabs[2]:
        # Get financial activity
        financial_data = run_query(f'''
            SELECT 
                fi.*,
                sa.country
            FROM financial_impact fi
            JOIN suspicious_accounts sa ON fi.client_id = sa.client_id
            WHERE sa.country = ?
        ''', [selected_country])
        
        st.dataframe(
            financial_data[[
                'client_id', 'total_deposits', 'total_withdrawals',
                'deposit_frequency', 'withdrawal_frequency', 'chargeback_count'
            ]],
            column_config={
                "total_deposits": st.column_config.NumberColumn("Total Deposits", format="$%.2f"),
                "total_withdrawals": st.column_config.NumberColumn("Total Withdrawals", format="$%.2f"),
                "chargeback_count": "Chargebacks"
            },
            hide_index=True
        )

# Add AI Analysis Section
st.header("🤖 AI-Powered Analysis")
analysis_tab1, analysis_tab2, analysis_tab3 = st.tabs(["Pattern Analysis", "Risk Assessment", "Recommendations"])

with analysis_tab1:
    st.subheader("Suspicious Pattern Analysis")
    pattern_data = run_query('''
        SELECT 
            sa.client_id,
            sa.platform,
            sa.market_type,
            fi.amount,
            fi.payment_method
        FROM suspicious_accounts sa
        JOIN financial_impact fi ON sa.client_id = fi.client_id
    ''')
    
    if st.button("Analyze Patterns"):
        with st.spinner("AI analyzing patterns..."):
            # Limit data size
            sample_data = pattern_data.head(5).to_string()  # Only analyze first 5 rows
            analysis = get_ai_analysis(
                sample_data,
                "Identify suspicious patterns in trading behavior and payment methods."
            )
            st.write(analysis)

with analysis_tab2:
    st.subheader("Risk Level Assessment")
    risk_data = run_query('''
        SELECT 
            sg.region,
            sg.payment_method,
            sg.client_count,
            sg.platform
        FROM suspicious_groups sg
    ''')
    
    if st.button("Assess Risks"):
        with st.spinner("AI assessing risks..."):
            # Summarize data before sending
            risk_summary = risk_data.groupby(['region', 'platform'])['client_count'].sum().to_string()
            analysis = get_ai_analysis(
                risk_summary,
                "Assess risk levels by region and platform, highlighting highest risk areas."
            )
            st.write(analysis)

with analysis_tab3:
    st.subheader("Action Recommendations")
    if st.button("Generate Recommendations"):
        with st.spinner("AI generating recommendations..."):
            # Summarize data before sending
            all_data = {
                "suspicious_accounts": run_query("SELECT * FROM suspicious_accounts").head(5).to_string(),
                "financial_impact": run_query("SELECT * FROM financial_impact").agg({
                    'fee': 'sum',
                    'amount': 'sum'
                }).to_string(),
                "groups": run_query("SELECT region, COUNT(*) as count FROM suspicious_groups GROUP BY region").to_string()
            }
            analysis = get_ai_analysis(
                json.dumps(all_data),
                "Provide specific action recommendations for the anti-fraud team based on this data."
            )
            st.write(analysis)
    
    print(analysis)

# Add AI Chat Interface
st.sidebar.header("💬 AI Assistant")
if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.sidebar.chat_message(message["role"]):
        st.markdown(message["content"])

if prompt := st.sidebar.chat_input("Ask about fraud patterns..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.sidebar.chat_message("user"):
        st.markdown(prompt)

    with st.sidebar.chat_message("assistant"):
        with st.spinner("Thinking..."):
            all_data = {
                "suspicious_accounts": run_query("SELECT * FROM suspicious_accounts").head().to_string(),
                "financial_impact": run_query("SELECT * FROM financial_impact").head().to_string()
            }
            response = get_ai_analysis(
                json.dumps(all_data),
                f"Based on this fraud detection data, please answer: {prompt}"
            )
            st.markdown(response)
    st.session_state.messages.append({"role": "assistant", "content": response})

# Add this to the AI analysis section
if st.button("Analyze Country Patterns"):
    with st.spinner("AI analyzing country patterns..."):
        country_analysis = get_ai_analysis(
            country_data.to_string(),
            f"""Analyze the fraud patterns across different countries, focusing on:
            1. High-risk countries and their characteristics
            2. Common patterns in each region
            3. KYC failure patterns
            4. Trading behavior variations by country
            
            Provide specific insights for {selected_country} if available."""
        )
        st.write(country_analysis)

# Non-Trading and Fake Trading Detection
st.header("🚨 Suspicious Trading Patterns")
tab_non_trading, tab_fake_trading, tab_groups = st.tabs([
    "Non-Trading Clients", 
    "Suspicious Trading Patterns",
    "Group Analysis"
])

with tab_non_trading:
    st.subheader("Clients with Deposits but No Trading")
    non_trading_clients = run_query('''
        WITH client_activity AS (
            SELECT 
                fi.client_id,
                sa.platform,
                sa.country,
                fi.total_deposits,
                fi.total_withdrawals,
                COALESCE(ta.trade_count, 0) as trade_count,
                fi.payment_method,
                sa.detection_date
            FROM financial_impact fi
            JOIN suspicious_accounts sa ON fi.client_id = sa.client_id
            LEFT JOIN (
                SELECT client_id, COUNT(*) as trade_count
                FROM trading_activity
                GROUP BY client_id
            ) ta ON fi.client_id = ta.client_id
            WHERE fi.total_deposits > 0
        )
        SELECT *
        FROM client_activity
        WHERE trade_count = 0
        ORDER BY total_deposits DESC
    ''')
    
    # Display non-trading clients metrics
    col1, col2, col3 = st.columns(3)
    col1.metric(
        "Non-Trading Clients", 
        len(non_trading_clients)
    )
    col2.metric(
        "Total Deposits", 
        f"${non_trading_clients['total_deposits'].sum():,.2f}"
    )
    col3.metric(
        "Total Withdrawals", 
        f"${non_trading_clients['total_withdrawals'].sum():,.2f}"
    )
    
    # Display detailed table
    st.dataframe(
        non_trading_clients,
        column_config={
            "client_id": "Client ID",
            "platform": "Platform",
            "country": "Country",
            "total_deposits": st.column_config.NumberColumn(
                "Total Deposits",
                format="$%.2f"
            ),
            "total_withdrawals": st.column_config.NumberColumn(
                "Total Withdrawals",
                format="$%.2f"
            ),
            "payment_method": "Payment Method",
            "detection_date": st.column_config.DatetimeColumn(
                "Detected On",
                format="D MMM YYYY, HH:mm"
            )
        },
        hide_index=True
    )

with tab_fake_trading:
    st.subheader("Suspicious Trading Patterns")
    fake_trading_clients = run_query('''
        WITH trading_metrics AS (
            SELECT 
                ta.client_id,
                COUNT(*) as trade_count,
                AVG(ta.profit_loss) as avg_pnl,
                AVG(ta.position_size) as avg_position,
                MAX(ta.trade_date) - MIN(ta.trade_date) as trading_duration,
                COUNT(*) * 1.0 / 
                    (JULIANDAY(MAX(ta.trade_date)) - JULIANDAY(MIN(ta.trade_date))) as trades_per_day
            FROM trading_activity ta
            GROUP BY ta.client_id
            HAVING 
                COUNT(*) > 10  -- Minimum trades to analyze
                AND AVG(ABS(ta.profit_loss)) < 1.0  -- Very small P/L
                AND trades_per_day > 5  -- High frequency
        )
        SELECT 
            tm.*,
            sa.platform,
            sa.country,
            fi.total_deposits,
            fi.payment_method
        FROM trading_metrics tm
        JOIN suspicious_accounts sa ON tm.client_id = sa.client_id
        JOIN financial_impact fi ON tm.client_id = fi.client_id
        ORDER BY trades_per_day DESC
    ''')
    
    # Display fake trading metrics
    col1, col2, col3 = st.columns(3)
    col1.metric(
        "Suspicious Traders", 
        len(fake_trading_clients)
    )
    col2.metric(
        "Avg Trades/Day", 
        f"{fake_trading_clients['trades_per_day'].mean():.1f}"
    )
    col3.metric(
        "Avg P/L", 
        f"${fake_trading_clients['avg_pnl'].mean():.2f}"
    )
    
    # Display detailed table
    st.dataframe(
        fake_trading_clients,
        column_config={
            "client_id": "Client ID",
            "trade_count": "Total Trades",
            "avg_pnl": st.column_config.NumberColumn(
                "Avg P/L",
                format="$%.2f"
            ),
            "trades_per_day": st.column_config.NumberColumn(
                "Trades/Day",
                format="%.1f"
            ),
            "platform": "Platform",
            "country": "Country",
            "payment_method": "Payment Method"
        },
        hide_index=True
    )

with tab_groups:
    st.subheader("Similar Behavior Groups")
    
    # Get group patterns
    group_patterns = run_query('''
        WITH client_groups AS (
            SELECT 
                sa.country,
                sa.platform,
                fi.payment_method,
                COUNT(DISTINCT sa.client_id) as client_count,
                GROUP_CONCAT(sa.client_id) as client_ids,
                AVG(COALESCE(ta.trade_count, 0)) as avg_trades,
                AVG(fi.total_deposits) as avg_deposits
            FROM suspicious_accounts sa
            JOIN financial_impact fi ON sa.client_id = fi.client_id
            LEFT JOIN (
                SELECT client_id, COUNT(*) as trade_count
                FROM trading_activity
                GROUP BY client_id
            ) ta ON sa.client_id = ta.client_id
            GROUP BY sa.country, sa.platform, fi.payment_method
            HAVING COUNT(DISTINCT sa.client_id) >= 2
        )
        SELECT *
        FROM client_groups
        ORDER BY client_count DESC, avg_deposits DESC
    ''')
    
    # Display group metrics
    col1, col2 = st.columns(2)
    
    # Group scatter plot
    fig_groups = px.scatter(
        group_patterns,
        x="avg_deposits",
        y="avg_trades",
        size="client_count",
        color="platform",
        hover_data=["country", "payment_method", "client_ids"],
        title="Group Behavior Analysis"
    )
    col1.plotly_chart(fig_groups)
    
    # Group details table
    col2.dataframe(
        group_patterns,
        column_config={
            "country": "Country",
            "platform": "Platform",
            "payment_method": "Payment Method",
            "client_count": "# of Clients",
            "avg_trades": st.column_config.NumberColumn(
                "Avg Trades",
                format="%.1f"
            ),
            "avg_deposits": st.column_config.NumberColumn(
                "Avg Deposits",
                format="$%.2f"
            ),
            "client_ids": "Client IDs"
        },
        hide_index=True
    )

# Add action buttons for bulk operations
st.header("🔒 Bulk Actions")
col1, col2 = st.columns(2)

with col1:
    if st.button("Lock Non-Trading Accounts"):
        with st.spinner("Processing..."):
            for _, client in non_trading_clients.iterrows():
                run_query('''
                    INSERT OR REPLACE INTO locked_accounts (client_id, lock_date)
                    VALUES (?, CURRENT_TIMESTAMP)
                ''', [client['client_id']])
            st.success(f"Locked {len(non_trading_clients)} accounts")

with col2:
    if st.button("Lock Suspicious Trading Accounts"):
        with st.spinner("Processing..."):
            for _, client in fake_trading_clients.iterrows():
                run_query('''
                    INSERT OR REPLACE INTO locked_accounts (client_id, lock_date)
                    VALUES (?, CURRENT_TIMESTAMP)
                ''', [client['client_id']])
            st.success(f"Locked {len(fake_trading_clients)} accounts")

# Add financial impact analysis
st.header("💰 Financial Impact Analysis")
impact_data = run_query('''
    SELECT 
        COALESCE(nt.impact_type, 'Suspicious Trading') as activity_type,
        COUNT(DISTINCT fi.client_id) as client_count,
        SUM(fi.fee) as total_fees,
        SUM(fi.amount) as total_volume
    FROM financial_impact fi
    LEFT JOIN (
        SELECT 
            subq.client_id,
            'Non-Trading' as impact_type
        FROM (
            SELECT fi2.client_id
            FROM financial_impact fi2
            LEFT JOIN trading_activity ta ON fi2.client_id = ta.client_id
            GROUP BY fi2.client_id
            HAVING COUNT(ta.client_id) = 0
        ) subq
    ) nt ON fi.client_id = nt.client_id
    GROUP BY COALESCE(nt.impact_type, 'Suspicious Trading')
''')

col1, col2 = st.columns(2)

with col1:
    # Impact metrics
    st.dataframe(
        impact_data,
        column_config={
            "activity_type": "Activity Type",
            "client_count": "# of Clients",
            "total_fees": st.column_config.NumberColumn(
                "Total Fees",
                format="$%.2f"
            ),
            "total_volume": st.column_config.NumberColumn(
                "Total Volume",
                format="$%.2f"
            )
        },
        hide_index=True
    )

with col2:
    # Impact visualization
    fig_impact = px.pie(
        impact_data,
        values="total_fees",
        names="activity_type",
        title="Distribution of Financial Impact"
    )
    st.plotly_chart(fig_impact) 
```

# dashboard/deriv_db.sqlite

```sqlite

```

# dashboard/update_db.py

```py
import json
import sqlite3
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
import os
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Email configuration
SMTP_CONFIG = {
    'server': os.environ.get('SMTP_SERVER', 'smtp.gmail.com'),
    'port': int(os.environ.get('SMTP_PORT', 587)),
    'username': os.environ.get('SMTP_USERNAME', 'syedmohamedsyakir@gmail.com'),
    'password': os.environ.get('SMTP_PASSWORD'),
    'use_tls': os.environ.get('SMTP_USE_TLS', 'True').lower() == 'true'
}

def send_email(to_email, subject, body, cc=None):
    """
    Send email with proper error handling and logging
    """
    if not SMTP_CONFIG['password']:
        logging.warning("SMTP password not configured. Skipping email send.")
        return False

    try:
        msg = MIMEMultipart()
        msg['From'] = SMTP_CONFIG['username']
        msg['To'] = to_email
        if cc:
            msg['Cc'] = cc
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        # Connect to SMTP server
        server = smtplib.SMTP(SMTP_CONFIG['server'], SMTP_CONFIG['port'])
        server.ehlo()
        
        # Use TLS if configured
        if SMTP_CONFIG['use_tls']:
            server.starttls()
            server.ehlo()

        # Login and send
        server.login(SMTP_CONFIG['username'], SMTP_CONFIG['password'])
        
        recipients = [to_email]
        if cc:
            recipients.append(cc)
            
        server.sendmail(SMTP_CONFIG['username'], recipients, msg.as_string())
        server.quit()
        
        logging.info(f"Email sent successfully to {to_email}")
        return True

    except smtplib.SMTPAuthenticationError:
        logging.error("SMTP authentication failed. Check credentials.")
        return False
    except smtplib.SMTPException as e:
        logging.error(f"SMTP error occurred: {str(e)}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error sending email: {str(e)}")
        return False
    finally:
        try:
            server.quit()
        except:
            pass

def update_database(json_file):
    with open(json_file, 'r') as f:
        data = json.load(f)

    user_id = data['user_id']
    risk_level = data['risk_level']

    if risk_level == 'high':
        # Connect to the database
        conn = sqlite3.connect('../database/deriv_db.sqlite')
        cursor = conn.cursor()

        # Update user status to locked
        cursor.execute("UPDATE user SET user_status = 'locked' WHERE user_id = ?", (user_id,))
        
        # Get user email
        cursor.execute("SELECT user_email FROM user WHERE user_id = ?", (user_id,))
        user_email = cursor.fetchone()[0]
        
        # Send initial email
        email_subject = "Account Locked - Suspicious Activity Detected"
        email_body = f"""Dear Valued Client ({user_email}),

We have detected unusual trading patterns on your account that require clarification.

Your account has been temporarily locked pending your response.

Please respond to this email with an explanation of your recent trading activity within 3 days or your account will be permanently disabled.

Best regards,
Deriv Security Team"""

        send_email(f"syedmohamedsyakir+deriv-client{user_id}@gmail.com", email_subject, email_body, "syedmohamedsyakir+deriv-antifraud@gmail.com")

        # Add entry to fraud database 
        detection_date = datetime.now()
        cursor.execute("""
            INSERT OR REPLACE INTO fraud (
                fraud_userid, 
                fraud_detecteddate,
                fraud_risk,
                fraud_clarificationemaildate,
                fraud_resolved
            ) VALUES (?, ?, ?, ?, ?)
        """, (
            user_id,
            detection_date,
            risk_level,
            detection_date,
            False
        ))
       

        conn.commit()
        conn.close()

    elif risk_level == 'medium':
        # Connect to the database
        conn = sqlite3.connect('../database/deriv_db.sqlite')
        cursor = conn.cursor()

        # Update user status to locked
        cursor.execute("UPDATE user SET user_status = 'monitor' WHERE user_id = ?", (user_id,))
        
        # Get user email
        cursor.execute("SELECT user_email FROM user WHERE user_id = ?", (user_id,))
        user_email = cursor.fetchone()[0]
        
        # Send initial email
        email_subject = "Account Locked - Suspicious Activity Detected"
        email_body = f"""Dear Valued Client,

We have detected unusual trading patterns on your account that require clarification.

Your account has is being monitored pending your response.

Please respond to this email with an explanation of your recent trading activity within 3 days or your account will be locked.

Best regards,
Deriv Security Team"""

        send_email(f"syedmohamedsyakir+deriv-client{user_id}@gmail.com", email_subject, email_body, "syedmohamedsyakir+deriv-antifraud@gmail.com")

        # Add entry to fraud database 
        detection_date = datetime.now()
        cursor.execute("""
            INSERT OR REPLACE INTO fraud (
                fraud_userid, 
                fraud_detecteddate,
                fraud_risk,
                fraud_clarificationemaildate,
                fraud_resolved
            ) VALUES (?, ?, ?, ?, ?)
        """, (
            user_id,
            detection_date,
            risk_level,
            detection_date,
            False
        ))

        conn.commit()
        conn.close()

if __name__ == "__main__":
    update_database("../analysis_output/high_risk_sample.json")
    update_database("../analysis_output/high_risk_sample_2.json")
    update_database("../analysis_output/high_risk_sample_3.json")
    update_database("../analysis_output/medium_risk_sample.json")
    update_database("../analysis_output/medium_risk_sample_2.json")
    
```

# database/deriv_db.sqlite

This is a binary file of the type: Binary

# database/fraud_db.sqlite

This is a binary file of the type: Binary

# deriv_db.sqlite

```sqlite

```

# models/fraud_detector.joblib

This is a binary file of the type: Binary

# requirements.txt

```txt
altair==5.4.1
attrs==25.1.0
blinker==1.8.2
cachetools==5.5.1
certifi==2025.1.31
charset-normalizer==3.4.1
click==8.1.8
Faker==35.2.0
gitdb==4.0.12
GitPython==3.1.44
idna==3.10
importlib_resources==6.4.5
Jinja2==3.1.5
joblib==1.4.2
jsonschema==4.23.0
jsonschema-specifications==2023.12.1
markdown-it-py==3.0.0
MarkupSafe==2.1.5
mdurl==0.1.2
narwhals==1.25.2
numpy==1.24.4
packaging==24.2
pandas==2.0.3
pillow==10.4.0
pkg_resources==0.0.0
pkgutil_resolve_name==1.3.10
plotly==6.0.0
protobuf==5.29.3
pyarrow==17.0.0
pydeck==0.9.1
Pygments==2.19.1
python-dateutil==2.9.0.post0
pytz==2025.1
referencing==0.35.1
requests==2.32.3
rich==13.9.4
rpds-py==0.20.1
scikit-learn==1.3.2
scipy==1.10.1
six==1.17.0
smmap==5.0.2
streamlit==1.40.1
tenacity==9.0.0
threadpoolctl==3.5.0
toml==0.10.2
tornado==6.4.2
typing_extensions==4.12.2
tzdata==2025.1
urllib3==2.2.3
watchdog==4.0.2
zipp==3.20.2

```

# scripts/deprecated/export_database.py

```py
import sqlite3
import os
import pandas as pd
from datetime import datetime

def get_connection():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(os.path.dirname(current_dir), 'database', 'fraud_db.sqlite')
    return sqlite3.connect(db_path)

def export_database():
    with get_connection() as conn:
        # Get list of all tables
        tables = pd.read_sql("""
            SELECT name FROM sqlite_master 
            WHERE type='table'
            ORDER BY name;
        """, conn)
        
        # Create output file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"database_export_{timestamp}.txt"
        
        with open(output_file, "w", encoding='utf-8') as f:
            f.write("=== DERIV FRAUD DETECTION DATABASE EXPORT ===\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Export each table
            for table_name in tables['name']:
                f.write(f"\n{'='*80}\n")
                f.write(f"TABLE: {table_name}\n")
                f.write(f"{'='*80}\n\n")
                
                # Get schema
                schema = pd.read_sql(f"PRAGMA table_info({table_name});", conn)
                f.write("SCHEMA:\n")
                f.write("-" * 80 + "\n")
                for _, row in schema.iterrows():
                    f.write(f"Column: {row['name']}\n")
                    f.write(f"Type: {row['type']}\n")
                    f.write(f"Nullable: {'Yes' if row['notnull'] == 0 else 'No'}\n")
                    f.write(f"Default: {row['dflt_value'] if row['dflt_value'] else 'None'}\n")
                    f.write(f"Primary Key: {'Yes' if row['pk'] == 1 else 'No'}\n")
                    f.write("-" * 40 + "\n")
                
                # Get all data
                data = pd.read_sql(f"SELECT * FROM {table_name};", conn)
                f.write(f"\nDATA ({len(data)} rows):\n")
                f.write("-" * 80 + "\n")
                
                if not data.empty:
                    # Write column headers
                    f.write("| " + " | ".join(str(col) for col in data.columns) + " |\n")
                    f.write("|" + "|".join("-" * len(str(col)) for col in data.columns) + "|\n")
                    
                    # Write data rows
                    for _, row in data.iterrows():
                        f.write("| " + " | ".join(str(val) for val in row) + " |\n")
                
                f.write("\n\n")
            
            # Write summary
            f.write(f"\n{'='*80}\n")
            f.write("DATABASE SUMMARY\n")
            f.write(f"{'='*80}\n\n")
            
            for table_name in tables['name']:
                count = pd.read_sql(f"SELECT COUNT(*) as count FROM {table_name};", conn).iloc[0]['count']
                f.write(f"{table_name}: {count} rows\n")

        print(f"Database exported to {output_file}")
        return output_file

if __name__ == "__main__":
    export_database() 
```

# scripts/deprecated/generate_data.py

```py
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

```

# scripts/deprecated/init_database_v2.py

```py
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
        trade_type TEXT NOT NULL CHECK (trade_type IN ('buy', 'sell')),
        trade_market TEXT NOT NULL,
        trade_pair TEXT NOT NULL,
        trade_volume FLOAT NOT NULL,
        trade_openprice FLOAT NOT NULL,
        trade_cost FLOAT NOT NULL,
        trade_currentprice FLOAT NOT NULL,
        trade_closeprice FLOAT,
        trade_profit FLOAT
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS fraud (
        fraud_id INTEGER PRIMARY KEY,
        fraud_userid INTEGER NOT NULL REFERENCES user(id),
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
```

# scripts/deprecated/init_database.py

```py
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
```

# scripts/generate_data_fraud.py

```py
import sqlite3
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
fake = Faker()

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
countries = ['UK', 'Germany', 'Singapore', 'Australia', 'UAE', 'Hong Kong']
kyc_statuses = ['Verified', 'Pending', 'Failed']
currency_pairs = ['EUR', 'USD', 'GBP', 'JPY']

# Set random seed for reproducibility
np.random.seed(42)

# Generate user data with random values

num_users = 2
user_accounts = []
user_ids = range(2001, 2001 + num_users)
for user_id in user_ids:
    user_name = fake.name()
    user_email = f'{user_name.lower().replace(" ", ".")}{np.random.choice(["@gmail.com", "@yahoo.com", "@hotmail.com", "@outlook.com"])}'
    user_accounts.append({
        'user_id': user_id,
        'user_name': user_name,
        'user_email': user_email,
        'user_profession': np.random.choice(professions, num_users),
        'user_country': np.random.choice(countries, num_users),
        'user_createdat': datetime.now() - timedelta(days=np.random.randint(1, 365)),
        'user_lastlogin': datetime.now() - timedelta(hours=np.random.randint(1, 24)),
        'user_kycstatus': np.random.choice(kyc_statuses, num_users),
        'user_lockstatus': False
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
        minutes = np.random.randint(1, 15)
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
    days = np.random.randint(1, 7) # max 7 days ago
    hours = np.random.randint(1, 24)
    for _ in range(num_trades):
        open_price = np.random.uniform(10, 1000)
        is_closed = np.random.random() > 0.3
        close_price = np.random.uniform(10, 1000) if is_closed else None
        current_price = close_price if is_closed else np.random.uniform(10, 1000)
        profit = (current_price - open_price) if is_closed else None
        minutes = np.random.randint(1, 15)
        seconds = np.random.randint(1, 60)
        pair1, pair2 = np.random.choice(currency_pairs, size=2, replace=False)
        trade_id += 1
        trade_records.append({
            'trade_id': trade_id,
            'trade_userid': user_id,
            'trade_timestamp': datetime.now() - timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds),
            'trade_type': np.random.choice(['buy', 'sell']),
            'trade_market': np.random.choice(markets),
            'trade_pair': f"{pair1}/{pair2}",
            'trade_volume': np.random.uniform(0.1, 10.0),
            'trade_openprice': open_price,
            'trade_closeprice': close_price,
            'trade_currentprice': current_price,
            'trade_profit': profit,
            'trade_platform': np.random.choice(platforms)
        })
trades = pd.DataFrame(trade_records)

# Save to database
user.to_sql('user', conn, if_exists='append', index=False)
transactions.to_sql('user_transaction', conn, if_exists='append', index=False)
trades.to_sql('user_trading', conn, if_exists='append', index=False)

# # 2. Locked accounts with Deriv-specific reasons
# reasons = [
#     'Suspicious rapid deposits/withdrawals',
#     'Minimal trading activity',
#     'Pattern of small trades without profit/loss',
#     'Multiple accounts detected'
# ]
# locked_clients = pd.DataFrame({
#     'client_id': range(1001, 1008),
#     'lock_date': [datetime.now() - timedelta(hours=np.random.randint(1, 24)) 
#                   for _ in range(7)],
#     'reason': np.random.choice(reasons, 7)
# })
# locked_clients.to_sql('locked_accounts', conn, if_exists='replace', index=False)

conn.commit()
conn.close()

```

# scripts/generate_data_v3.py

```py
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
```

# scripts/init_database_v3.py

```py
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
```

# scripts/train_model.py

```py
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
```

# src/models/fraud_detector.py

```py
# src/models/fraud_detector.py
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
import joblib

class FraudDetector:
    def __init__(self, random_state=42):
        self.model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=random_state
        )
        
    def train(self, features, labels):
        """Train the fraud detection model"""
        self.model.fit(features, labels)
        
        # Get training predictions
        y_pred = self.model.predict(features)
        print("\nModel Performance:")
        print(classification_report(labels, y_pred))
        
        return self
    
    def predict(self, features):
        """Make predictions on new data"""
        return self.model.predict_proba(features)
    
    def save_model(self, filepath):
        """Save model to disk"""
        joblib.dump(self.model, filepath)
    
    @classmethod
    def load_model(cls, filepath):
        """Load model from disk"""
        instance = cls()
        instance.model = joblib.load(filepath)
        return instance
```

# src/utils/feature_engineering.py

```py
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
```

