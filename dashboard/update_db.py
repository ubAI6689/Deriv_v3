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
    