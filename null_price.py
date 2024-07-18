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

initiate_logging(LOG_FILENAME)

today_date = datetime.today().date()

id_data = supabase.table("idx_daily_data").select("*").eq("date",today_date.strftime("%Y-%m-%d")).is_("close", "null").execute()

daily_data = supabase.table("idx_daily_data").select("symbol").eq("date",today_date.strftime("%Y-%m-%d")).execute()
daily_data = pd.DataFrame(daily_data.data).shape[0]

active_stock = supabase.table("idx_active_company_profile").select("symbol").execute()
active_stock = pd.DataFrame(active_stock.data).shape[0]

if daily_data == active_stock:
    logging.info(f"{daily_data} tickers are scraped and already same with the number of active stock")
else:
    logging.error(f"{daily_data} tickers are scraped not same with number of active stock")

if pd.DataFrame(id_data.data).shape[0] == 0:
    logging.info(f"No null values at every stock on {today_date}")
else:
    null_data = pd.DataFrame(id_data.data)

    logging.error(f"There are some null values on {today_date} for stocks {null_data.symbol.unique()}")

    df_rerun = pd.DataFrame()

    for i in id_data.symbol.unique():
        a = yf.Ticker(i).history(start=today_date, end=today_date+dt.timedelta(1)).reset_index()[["Date","Close",'Volume']]
        a['symbol'] = i

        df_rerun = pd.concat([df_rerun,a])

    df_rerun.Date = pd.to_datetime(df_rerun.Date)
    df_rerun["Date"] = df_rerun["Date"].dt.strftime("%Y-%m-%d")
    
    logging.info(f"update data for {i} on {today_date}")

    # for i in df_rerun.symbol.unique():
    #     supabase.table("idx_daily_data").update(
    #                 {"close": convert_numpy_int64(df_rerun[df_rerun.symbol == i].iloc[0]["Close"]),
    #                 "volume": convert_numpy_int64(df_rerun[df_rerun.symbol == i].iloc[0]["Volume"])}
    #             ).eq("symbol", i).eq("date", df_rerun[df_rerun.symbol == i].iloc[0]["Date"]).execute()
    
    #     logging.info(f"update data for {i} on {today_date}")