import logging
import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
from imp import reload
import yfinance as yf
from requests import Session
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, Limiter, RequestRate

load_dotenv()
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase_client = create_client(url, key)
LOG_FILENAME = 'update_daily_data.log'

def initiate_logging(LOG_FILENAME):
    reload(logging)

    formatLOG = '%(asctime)s - %(levelname)s: %(message)s'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO, format=formatLOG)
    logging.info('Program started')

def load_data(supabase_client):
    try:
        response = supabase_client.rpc("get_anomally", params=None).execute()
        logging.info("The data that has anomaly successfully fetched from the DB")
    except:
        logging.error("The data cannot be fetched from the DB function")

    retrieve_data = pd.DataFrame(response.data)

    today_df = retrieve_data[["symbol","date","close"]]
    yesterday_df = retrieve_data[["symbol","last_date","last_close"]].rename(columns={"last_date":"date","last_close":"close"})

    processed_df = pd.concat([today_df,yesterday_df]).drop_duplicates()

    return processed_df

def get_new_price(symbol, start):

    class LimiterSession(LimiterMixin, Session):
        def __init__(self):
            super().__init__(
                limiter=Limiter(
                    RequestRate(2, Duration.SECOND * 5)
                ),  # max 2 requests per 5 seconds
                bucket_class=MemoryQueueBucket,
            )

    session = LimiterSession()
    ticker = yf.Ticker(symbol, session=session)
    end = (pd.to_datetime(start) + pd.DateOffset(days=1)).strftime('%Y-%m-%d')
    data = ticker.history(start=start, end=end, auto_adjust=False)[
                                ["Close","Volume"]]
    return data

def data_change(processed_df):
    new_price = pd.DataFrame()
    
    for i in range(0, processed_df.shape[0]):
        df = get_new_price(processed_df.iloc[i,0],processed_df.iloc[i,1])
        df["symbol"] = processed_df.iloc[i,0]
        new_price = pd.concat([new_price, df], axis=0)
    
    logging.info("Finished Fetching New Data")

    return new_price

def comparison_proc(new_price, processed_df,supabase_client):
    new_price = new_price.reset_index().rename(columns={"Date":"date","Close":"new_price","Volume":"volume"})
    new_price["new_price"] = round(new_price["new_price"],0).astype("int")
    new_price["date"] = pd.to_datetime(new_price["date"]).dt.strftime("%Y-%m-%d")
    new_price = new_price.merge(processed_df, on=["date","symbol"])

    for i in range(0,new_price.shape[0]):
        if new_price.loc[i,"new_price"] != new_price.loc[i,"close"]:
            try:
                supabase_client.table("idx_daily_data").update(
                    {"close": int(new_price.loc[i,"new_price"]),
                     "volume": int(new_price.loc[i,"volume"]),
                     "updated_on": pd.Timestamp.now(tz="GMT").strftime("%Y-%m-%d %H:%M:%S")}
                ).eq("symbol",new_price.loc[i,"symbol"]).eq("date",str(new_price.loc[i,"date"])).execute()

                logging.info(f'The data for {new_price.loc[i,"symbol"]} on {new_price.loc[i,"date"]} is changed from {new_price.loc[i,"close"]} to {new_price.loc[i,"new_price"]}')
            except:
                logging.error(f'Failed to update data for {new_price.loc[i,"symbol"]} on {new_price.loc[i,"date"]}.')
        else:
            logging.info(f'The data for {new_price.loc[i,"symbol"]} on {new_price.loc[i,"date"]} is not changed based on the new fetching although it has anomally')
    
    return new_price

initiate_logging(LOG_FILENAME)
db_data = load_data(supabase_client)
new_data = data_change(db_data)
result = comparison_proc(new_data,db_data,supabase_client)

logging.info("Program has finished")

f = open(LOG_FILENAME, 'rt')
try:
    body = f.read()
finally:
    f.close()