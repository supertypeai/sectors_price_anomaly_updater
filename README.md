# sectors_price_anomaly_updater

This repositories is intended to re-fetch the daily price and volume data of some stocks that has an anomally based on the function that has been made in supabase. It will directly change the price in the database to the new fetched price if the new price is different from the old price. Moreover, it will also change the volume directly to the database. The script will be run once a week at 9am Jakarta time and the changes can be monitored through the update_daily_data.log.
