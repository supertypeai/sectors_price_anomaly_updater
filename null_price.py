import logging
from imp import reload
from supabase import create_client
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime
import datetime as dt
import yfinance as yf
import numpy as np
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from dotenv import load_dotenv
load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)
LOG_FILENAME = 'daily_null_data.log'

def initiate_logging(LOG_FILENAME):
    reload(logging)

    formatLOG = '%(asctime)s - %(levelname)s: %(message)s'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO, format=formatLOG)
    logging.info('Program started')

def convert_numpy_int64(data):
    if isinstance(data, np.int64):
        return int(data)
    elif isinstance(data, dict):
        return {k: convert_numpy_int64(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_numpy_int64(i) for i in data]
    else:
        return data
    
def send_email(daily_data, active_stock,today_date):
    # Create email details
    message = Mail(
        from_email='gerald@supertype.ai',
        to_emails='geraldbryan9914@gmail.com',
        subject='Sending with SendGrid is Fun',
        html_content=f'<strong>The number of stock scrapped on {today_date} not equal with the number of active stocks, only {daily_data} from {active_stock} are not null, please check your script again!</strong>'
    )
    
    try:
        # Initialize SendGrid API client with your API key
        sg = SendGridAPIClient(os.environ.get("SENDGRID_API_KEY"))
        
        # Send the email
        response = sg.send(message)
        print(f"Email sent! Status code: {response.status_code}")
        print(response.body)  # Optional: Print the response body for debugging
        print(response.headers)  # Optional: Print response headers for debugging
    
    except Exception as e:
        print(f"Error sending email: {e}")

initiate_logging(LOG_FILENAME)

response = supabase.table('idx_daily_data').select('date').order('date', desc=True).execute()
today_date = pd.DataFrame(response.data).date.unique()[0]

id_data = supabase.table("idx_daily_data").select("*").eq("date",today_date).is_("close", "null").execute()

daily_data = supabase.table("idx_daily_data").select("symbol").eq("date",today_date).execute()
daily_data = pd.DataFrame(daily_data.data).shape[0]

active_stock = supabase.table("idx_active_company_profile").select("symbol").execute()
active_stock = pd.DataFrame(active_stock.data).shape[0]

if daily_data == active_stock:
    logging.info(f"{daily_data} tickers are scraped and already same with the number of active stock")
else:
    logging.error(f"{daily_data} tickers are scraped, not same with number of active stock, please checked the pipeline and revised it")
    send_email(daily_data,active_stock,today_date)
