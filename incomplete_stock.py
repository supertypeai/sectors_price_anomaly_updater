import logging
from imp import reload
from supabase import create_client
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import datetime as dt
import yfinance as yf
import numpy as np

load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)
LOG_FILENAME = 'daily_null_data.log'

def initiate_logging(LOG_FILENAME):
    reload(logging)

    formatLOG = '%(asctime)s - %(levelname)s: %(message)s'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO, format=formatLOG)
    logging.info('Program add incomplete stock started')

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

response = supabase.table('idx_daily_data').select('date').order('date', desc=True).execute()
date = pd.DataFrame(response.data).date.unique()[0]

daily_data = supabase.table("idx_daily_data").select("symbol").eq("date",date).execute()
daily_data = pd.DataFrame(daily_data.data)

active_stock = supabase.table("idx_active_company_profile").select("symbol").execute()
active_stock = pd.DataFrame(active_stock.data)

if daily_data.shape[0] != active_stock.shape[0]:
    
    symbol_df = active_stock[~active_stock.symbol.isin(daily_data.symbol)]
    end_date = pd.to_datetime(date) + timedelta(1)
    end_date = end_date.strftime("%Y-%m-%d")

    df_na = pd.DataFrame()
    for i in symbol_df.symbol.unique():
        ticker = yf.Ticker(i)
        a = ticker.history(start=date, end=end_date).reset_index()[["Date","Close",'Volume']]
        a['symbol'] = i
        try:
            a["market_cap"] = ticker.info["marketCap"]
        except:
            a["market_cap"] = np.nan

        df_na = pd.concat([df_na,a])

    df_na["Date"] = pd.to_datetime(df_na["Date"]
    df_na["Date"] = df_na["Date"].dt.strftime("%Y-%m-%d")
    df_na["update_on"] = pd.Timestamp.now(tz="GMT").strftime("%Y-%m-%d %H:%M:%S")
    df_na["mcap_method"] = 1

    df_na.columns = df_na.columns.str.lower()

    df_na.market_cap = df_na.market_cap.astype("Int64")
    df_na.close = df_na.close.astype('int')
    df_na.volume = df_na.volume.astype('int')

    df_na = df_na.replace({np.nan: None})

    for i in df_na.symbol.unique():
        supabase.table("idx_daily_data").insert(
                    {"symbol": convert_numpy_int64(df_na[df_na.symbol == i].iloc[0]["symbol"]),
                    "date": convert_numpy_int64(df_na[df_na.symbol == i].iloc[0]["date"]),
                        "close": convert_numpy_int64(df_na[df_na.symbol == i].iloc[0]["close"]),
                    "volume": convert_numpy_int64(df_na[df_na.symbol == i].iloc[0]["volume"]),
                    "market_cap": convert_numpy_int64(df_na[df_na.symbol == i].iloc[0]["market_cap"]),
                    "updated_on": df_na[df_na.symbol == i].iloc[0]["update_on"],
                    "mcap_method": convert_numpy_int64(df_na[df_na.symbol == i].iloc[0]["mcap_method"])}
                ).execute()
    
    logging.info(f"Finish add {df_na.shape[0]} missing stocks data for {date} in idx_daily_data_table")
