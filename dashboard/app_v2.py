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