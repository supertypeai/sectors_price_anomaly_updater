from supabase import create_client
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime,timedelta
import datetime as dt
import yfinance as yf
import numpy as np
import logging
from imp import reload

load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

LOG_FILENAME = 'daily_null_data.log'

def initiate_logging(LOG_FILENAME):
    reload(logging)

    formatLOG = '%(asctime)s - %(levelname)s: %(message)s'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO, format=formatLOG)
    logging.info('Program Filling Null Market Cap Started')

def convert_numpy_int64(data):
    if isinstance(data, np.int64):
        return int(data)
    elif isinstance(data, dict):
        return {k: convert_numpy_int64(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_numpy_int64(i) for i in data]
    else:
        return data

initiate_logging(LOG_FILENAME)

today_date = datetime.today().date()

data = supabase.table("idx_daily_data").select("*").eq("date",today_date.strftime("%Y-%m-%d")).is_("close", "null").execute()
data = pd.DataFrame(data.data)

if data.shape[0] > 0:
    response = supabase.table('idx_daily_data').select('date').order('date', desc=True).execute()
    mcap_data = supabase.table('idx_daily_data').select('symbol',"close","market_cap").eq("date",pd.DataFrame(response.data).date.unique()[1]).in_('symbol', data["symbol"]).execute()

    df_hist = pd.DataFrame(mcap_data.data)

    df_hist["outstanding_shares"] = df_hist["market_cap"]/df_hist['close']

    data = data.merge(df_hist[["symbol","outstanding_shares"]], on="symbol", how="left")
    data["market_cap"] = data["close"] * data["outstanding_shares"]

    data.market_cap = data.market_cap.astype("int")

    for i in range (0,data.shape[0]):
        supabase.table("idx_daily_data").update(
                    {"market_cap": convert_numpy_int64(data.iloc[i].market_cap)}
                ).eq("symbol", data.iloc[i].symbol).eq("date", data.iloc[i].date).execute() 
        
        logging.info(f"Update Market Cap Value for {data.iloc[i].symbol} on {data.iloc[i].date}")
elif data.shape == 0:
    logging.info(f"No Null Data Found, all market_cap in {today_date} is non-null data")