# TO query Gemini API and parse
import os
import csv
import requests
import json
import base64
import hmac
import hashlib
import datetime, time
import pandas as pd
from time import time, sleep
# TO write dictionaries to CSV
import csv
# TO upload csv to Cloud Storage
from google.cloud import storage
# TO fill dataframe column with NaN
import numpy as np
# TO have exact decimal precision https://realpython.com/python-rounding/#the-decimal-class, https://zetcode.com/python/decimal/
import decimal
from decimal import Decimal
# TO convert dictionary as string type to dictionary type https://www.kite.com/python/docs/ast.literal_eval
import ast

# TO query BigQuery
# import google.auth
# from google.cloud import bigquery
# from google.cloud import bigquery_storage

#-----GET PAST TRADES FROM GEMINI API-----#
# documentation at https://docs.gemini.com/rest-api/#get-past-trades

# PAYLOAD - nonce
nonce = int(round(time()*1000))

# PAYLOAD - timestamp
# check if trades.csv already exists
trades_exists = os.path.exists("trades.csv") # https://www.guru99.com/python-check-if-file-exists.html
if trades_exists == True: # if exists, get timestamp of latest trade, then query subsequent trades and append
    existing = pd.read_csv("trades.csv")
    timestamp = existing.loc[len(existing)-1,"timestamp"]
    timestamp = (datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f").timestamp()*1000) + 1 # multiply by 1000 as Gemini API accepts microseconds as whole number, then add one microsecond to timestamp to search for trades after and excluding last trade queried
else: # if does not exist, query from timestamp of first trade on Gemini https://docs.gemini.com/rest-api/#timestamps
    timestamp = 1444311607800
print("Querying trades from {} onwards.".format(datetime.datetime.fromtimestamp(timestamp/1000).strftime("%Y-%m-%d %H:%M:%S.%f"))) # fromtimestamp() can only take up to seconds as whole number, microseconds must be decimals

# PAYLOAD - build dict object
payload = {
	'request':'/v1/mytrades',
    'nonce': nonce,
	'symbol': ["btcusd", "ethusd"],
    'limit_trades': 500,
    'timestamp': timestamp # put datetime from which to search
}

# PAYLOAD - encode as json for hashing
payload = str.encode(json.dumps(payload))
# PAYLOAD - base64 encode the payload
b64 = base64.b64encode(payload)

# API KEY AND SECRET - read from gem.csv
gemini_csv = pd.read_csv("gem.csv")
key = gemini_csv.loc[1,"key"]
secret = gemini_csv.loc[1,"sec"]

# SIGNATURE - create using SECRET and PAYLOAD encoded as b64
signature = hmac.new(secret.encode(), b64, hashlib.sha384).hexdigest()

# HEADERS - as required for contacting api endpoint
headers = {
        'Content-Type':'text/plain',
        'Content-Length': "0",
        'X-GEMINI-APIKEY': key,
        'X-GEMINI-PAYLOAD': b64,
        'X-GEMINI-SIGNATURE': signature,
        'Cache-Control': "no-cache"
}

# POST - retrieve data as response
url = 'https://api.gemini.com/v1/mytrades' # gemini api endpoint
r = requests.request("POST", url, headers=headers)

trades_df = r.json()
print("Gemini API query success. Parsing {} trades.".format(len(trades_df)))

#-----PARSE JSON TO CREATE BUY / SELL RECORDS IN TRADES.CSV-----#

timestamps = []
buysell = []
symbols = []
fiats = []
qtys = []
fee_rates = []
pricesSGD = []
pricesUSD = []
valuesSGD = []
valuesUSD = []
exchanges = []

for trade in trades_df: # parse json
    timestamps.append(datetime.datetime.fromtimestamp(trade["timestampms"]/1000).strftime("%Y-%m-%d %H:%M:%S.%f")) # BigQuery needs timestamp in "%Y-%m-%d %H:%M:%S" format https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv
    buysell.append(trade["type"])
    symbols.append(trade["symbol"][:-3]) # e.g. LINKUSD, where before last 3 alphabets are symbol, last 3 alphabets are fiat 
    fiats.append(trade["symbol"][-3:])
    exchanges.append(trade["exchange"])
    qtys.append(float(trade["amount"]))
    
    # aggressor True = Taker = Fees 0.35%, aggressor False = Maker = Fees 0.25%
    if trade["aggressor"] == True:
        fee_rates.append(0.0035)
        # convert transaction fiat to both USD and SGD for easier reference
        if trade["symbol"][-3:] == "SGD":
            pricesSGD.append(float(trade["price"]))
            pricesUSD.append(float(trade["price"]) / 1.335)
            
            valueSGD = float(trade["price"]) * float(trade["amount"]) * 1.0035
            valuesSGD.append(valueSGD)
            valuesUSD.append(valueSGD / 1.335)
        elif trade["symbol"][-3:] == "USD": 
            pricesSGD.append(float(trade["price"]) * 1.335)
            pricesUSD.append(float(trade["price"]))
            
            valueUSD = float(trade["price"]) * float(trade["amount"]) * 1.0035
            valuesSGD.append(valueUSD * 1.335)
            valuesUSD.append(valueUSD)
    else:
        fee_rates.append(0.0025)
        # if transaction fiat not originally SGD, convert to SGD for easier reference
        if trade["symbol"][-3:] == "SGD":
            pricesSGD.append(float(trade["price"]))
            pricesUSD.append(float(trade["price"]) / 1.335)
            
            valueSGD = float(trade["price"]) * float(trade["amount"]) * 1.0025
            valuesSGD.append(valueSGD)
            valuesUSD.append(valueSGD / 1.335)
        elif trade["symbol"][-3:] == "USD": 
            pricesSGD.append(float(trade["price"]) * 1.335)
            pricesUSD.append(float(trade["price"]))
            
            valueUSD = float(trade["price"]) * float(trade["amount"]) * 1.0025
            valuesSGD.append(valueUSD * 1.335)
            valuesUSD.append(valueUSD)

trades_dict = {
    "timestamp": timestamps,
    "buysell": buysell,
    "symbol": symbols,
    "fiat": fiats,
    "qty": qtys,
    "fee_rate": fee_rates,
    "priceSGD": pricesSGD,
    "priceUSD": pricesUSD,
    "valueSGD": valuesSGD,
    "valueUSD": valuesUSD,
    "exchange": exchanges
}

trades_df = pd.DataFrame(trades_dict, columns = ["timestamp", "buysell", "symbol", "fiat", "qty", "fee_rate", "priceSGD", "priceUSD", "valueSGD", "valueUSD", "exchange"])
trades_df = trades_df.iloc[::-1].reset_index(drop=True) # reverse order in trades_df such that earliest trade is first
if len(trades_df) > 0: # if trades_df is not empty
    csv_file = trades_df.to_csv("trades.csv", index=False)
    print("Parsed {} trades from {} to {}. \ntrades.csv created.".format(len(trades_df), timestamps[len(trades_df)-1], timestamps[0]))

elif len(trades_df) == 0: # if trades_df empty
    print("No trades were parsed.")

# #-----UPLOAD TRADES.CSV TO GOOGLE CLOUD STORAGE-----#
# # run on Google Compute Engine VM instance with dependency installed using "pip3 install --upgrade google-cloud-storage"

storage_client = storage.Client()
bucket = storage_client.bucket("bullsheet")
blob = bucket.blob("trades.csv")
blob.upload_from_filename("/home/arthur95chionh/trades.csv")

print("Uploaded trades.csv containing {} records to Cloud Storage bucket 'bullsheet'.".format(len(trades_df)))

# # ********** PROCEED FROM TRADES TO BULL **********
wait = 3
while wait:
    print("Proceeding from trades to bull in {}.".format(wait), end="\r")
    sleep(1)
    wait -= 1
print("Processing trades_df with latest buys_left.csv.")

# # *************************************************

#----- COMBINE TRADES_DF WITH BUYS_LEFT.CSV (IF ANY) (USE TRADES_DF BECAUSE LATEST TRANSACTIONS ONLY, INSTEAD OF QUERYING ALL HISTORICAL TRADES IN BQ TABLE "TRADES") -----#

#-----PREPARE TRADES_DF, BUYS_LEFT-----#

# PREPARE TRADES_DF
trades_df = pd.read_csv("trades.csv")
#trades_df = trades_df.iloc[:15]

# trades_df contains both buy and sell transactions, has unnecessary columns like fiat/priceSGD etc., sorts transactions by descending timestamp, and has indices based on total transactions
# SO filter trades_df based on column "buysell", select certain columns only, sort by ascending timestamp, reset and drop index to iterate
buys_df = trades_df[trades_df["buysell"]=="Buy"][["timestamp","symbol","qty","priceUSD","valueUSD", "exchange"]].reset_index(drop=True) 
sells_df = trades_df[trades_df["buysell"]=="Sell"][["timestamp","symbol","qty","priceUSD","valueUSD", "exchange"]].reset_index(drop=True)

# PREPARE BUYS_LEFT_DF (CHECK IF BUYS_LEFT.CSV EXISTS)
# check if buys_left.csv already exists
buys_left_exists = os.path.exists("buys_left.csv")
if buys_left_exists == True: # if exists, # combine buys from buys_left_df with buys in trade_df (now in buys_df)
    buys_left_df = pd.read_csv("buys_left.csv")
    buys_df = buys_df.append(buys_left_df, ignore_index=True).sort_values(by="priceUSD",ascending=False).reset_index(drop=True) # sort by priceUSD descending, for display when choosing which buys to realize
else: # if buys_left_df is not appended to buys_df, buys_df will not have "qty_left" column...
    buys_df["qty_left"] = np.nan # ...so create a "qty_left" column of NaN values to indicate records in buys_df do not have existing realizations

# PREPARE SEPARATE DICTIONARIES OF RECORDS FOR BUY, SELL
# qtys from Gemini API are floats that could be fractions without exact decimal precision, so use Decimal class to have exact decimal precision for calculations
#decimal.getcontext().prec = 5 # set decimal class precision at 5 decimal places
#decimal.getcontext().rounding = decimal.ROUND_DOWN

buys = {}
for record in range(len(buys_df)):
    timestamp = buys_df.at[record,"timestamp"]
    buys[timestamp] = {}
    buys[timestamp]["symbol"] = buys_df.at[record,"symbol"]
    if pd.isna(buys_df.at[record,"qty_left"]): # if qty_left is NaN, it is a record from trades_df, so assign new qty_left and sells
        #if buys_df.at[record,"qty"] >= 0.00001: 
        buys[timestamp]["qty"] = Decimal(str(buys_df.at[record,"qty"])) # ...so just work at 5 decimals...
        buys[timestamp]["qty_left"] = Decimal(str(buys_df.at[record,"qty"])) # track qty to-be-realized, use Decimal class for decimal place precision
        buys[timestamp]["sells"] = {} # dictionary to record matched sells' timestamps and qtys realized
        #elif buys_df.at[record,"qty"] < 0.00001: # if qty is negligible...
            #buys[timestamp]["qty"] = buys_df.at[record,"qty"] # ...still record it so overall records still match up...
            #buys[timestamp]["qty_left"] = 0 # ...but don't bother realizing it...
            #buys[timestamp]["sells"] = {"negligible":buys_df.at[record,"qty"]} # ...and record as negligible
        buys[timestamp]["profit"] = 0 # to record profit
    else: # if qty_left is not NaN, it is a record from buys_left.csv, so use the existing qty_left and sells
        buys[timestamp]["qty"] = Decimal(str(buys_df.at[record,"qty"]))
        buys[timestamp]["qty_left"] = Decimal(str(buys_df.at[record,"qty_left"]))
        buys[timestamp]["sells"] = ast.literal_eval(buys_df.at[record,"sells"]) # must convert dictionary in string type (when saved as CSV) back into dictionary type
        buys[timestamp]["profit"] = buys_df.at[record,"profit"]
    buys[timestamp]["priceUSD"] = buys_df.at[record,"priceUSD"]
    buys[timestamp]["valueUSD"] = Decimal(str(buys_df.at[record,"valueUSD"]))
    buys[timestamp]["exchange"] = buys_df.at[record,"exchange"]
    
sells = {}
for record in range(len(sells_df)):
    timestamp = sells_df.at[record,"timestamp"]
    sells[timestamp] = {}
    sells[timestamp]["symbol"] = sells_df.at[record,"symbol"]
    #if sells_df.at[record,"qty"]  >= 0.00001: # logic explanation similar to that for buys dictionary
    sells[timestamp]["qty"] = Decimal(str(sells_df.at[record,"qty"]))
    sells[timestamp]["qty_left"] = Decimal(str(sells_df.at[record,"qty"]))
    sells[timestamp]["buys"] = {} # dictionary to record matched sells' timestamps and qtys realized
    #elif sells_df.at[record,"qty"]  < 0.00001: # if qty is negligible...
        #sells[timestamp]["qty"] = sells_df.at[record,"qty"] # ...still record it so overall records still match up...
        #sells[timestamp]["qty_left"] = 0 # ...but don't bother realizing it...
        #sells[timestamp]["buys"] = {"negligible":sells_df.at[record,"qty"]} # ...and record as negligible
    sells[timestamp]["profit"] = 0 # to record profit
    sells[timestamp]["priceUSD"] = sells_df.at[record,"priceUSD"]
    sells[timestamp]["valueUSD"] = Decimal(str(sells_df.at[record,"valueUSD"]))
    sells[timestamp]["exchange"] = sells_df.at[record,"exchange"]

#-----REALIZE SELL TRANSACTIONS-----#

for sell_timestamp, sell_info in sells.items(): # use Decimal class to have exact decimal place precision
    sell_valuePERqty = Decimal(str(sell_info["valueUSD"]))/Decimal(str(sell_info["qty"])) # to calculate profit with buy_valuePERqty (below)
    sell_profit = 0 # variable to record profit
    
    while Decimal(str(sell_info["qty_left"])) > 0: # generate "buylist" of eligible buy transactions to realize qty of sell transaction until all realized
        print("""\n{} SELL of {} @ USD {}
Qty Left: {}""".format(sell_timestamp, sell_info["symbol"], sell_info["priceUSD"], sell_info["qty_left"]))
        print("""\nBUYLIST:
ID  |          TIMESTAMP          | PRICEUSD  | PROFIT  | IF QTY REALIZED""") # header of buys "hitlist", "NET_QTY (S-B)" column indicates if sell or buy transaction has more qty left (i.e. which qty to choose)
        buylist = {} # create dictionary of buylist ID and timestamp for user to select ID, which is easier than selecting timestamp
        buylist_id = 0
        for buy_timestamp, buy_info in buys.items(): # print out hitlist of buy transactions that occurred before the sell transaction AND same symbol AND still has qty left
            buy_valuePERqty = Decimal(str(buys[buy_timestamp]["valueUSD"]))/Decimal(str(buys[buy_timestamp]["qty"])) # to calculate profit with sell_valuePERqty (above)
            
            if buy_timestamp < sell_timestamp and sell_info["symbol"] == buy_info["symbol"] and buy_info["qty_left"] > 0 and (sell_info["qty_left"]-buy_info["qty_left"]) >= 0: # if buy qty_left can be fully realized, let user choose buy qty_left:
                print("{:02d}  |  {} |  {:.2f}  |  {}   | {}".format(buylist_id, buy_timestamp, round(buy_info["priceUSD"],2), round((sell_valuePERqty-buy_valuePERqty)*Decimal(str(buy_info["qty_left"])),2), buy_info["qty_left"])) # profit calculated by (difference in value/qty) * qty realized
                buylist[buylist_id] = {}  # associate buy_timestamp and buy_info["qty_left"] with current buylist_id
                buylist[buylist_id]["timestamp"] = buy_timestamp
                buylist[buylist_id]["qty_left"] = Decimal(str(buy_info["qty_left"]))
                buylist_id += 1 # prepare next buylist_id
                
            elif buy_timestamp < sell_timestamp and sell_info["symbol"] == buy_info["symbol"] and buy_info["qty_left"] > 0 and (sell_info["qty_left"]-buy_info["qty_left"]) < 0: # elif buy qty_left cannot be fully realized, let user choose sell qty_left
                print("{:02d}  |  {} |  {:.2f}  |  {}   | {} (maximum sell qty_left)".format(buylist_id, buy_timestamp, round(buy_info["priceUSD"],2), round((sell_valuePERqty-buy_valuePERqty)*Decimal(str(buy_info["qty_left"])),2), sell_info["qty_left"])) # profit calculated by (difference in value/qty) * qty realized
                buylist[buylist_id] = {}  # associate buy_timestamp and sell_info["qty_left"] with current buylist_id
                buylist[buylist_id]["timestamp"] = buy_timestamp
                buylist[buylist_id]["qty_left"] = Decimal(str(sell_info["qty_left"]))
                buylist_id += 1 # prepare next buylist_id

        # let user choose which ID and what qty to realize, with input validation
        try:
            chosen_id = int(input("\nInput ID of buy transaction and qty to be realized: "))
            chosen_timestamp = buylist[chosen_id]["timestamp"]
            chosen_qty = Decimal(str(buylist[chosen_id]["qty_left"]))
        except ValueError: # let users reselect ID if they fail to input integer
            print("\nERROR! Please input integer. Try again (:")
            print("--------------------------------------------------------------")
            continue
        except KeyError: # let users reselect ID if they input wrong integer
            print("\nERROR! Please input a correct ID. Try again (:")
            print("--------------------------------------------------------------")
            continue
        if chosen_qty <= 0:
            print("ERROR! Qty must be > 0. Try again (:") # let users reselect qty if chosen_qty <= 0
            print("--------------------------------------------------------------")
            continue
        elif sell_info["qty_left"] - chosen_qty < 0: # let users reselect qty if it causes sell qty_left < 0
            print("ERROR! Sell qty_left < 0. Try again (:")
            print("--------------------------------------------------------------")
            continue
        elif buys[chosen_timestamp]["qty_left"] - chosen_qty < 0: # let users reselect qty if it causes sell qty_left < 0
            print("ERROR! Buy qty_left < 0. Try again (:")
            print("--------------------------------------------------------------")
            continue
        
        print("--------------------------------------------------------------")

        # realize qtys
        sell_info["qty_left"] = Decimal(str(sell_info["qty_left"])) - Decimal(str(chosen_qty))
        buys[chosen_timestamp]["qty_left"] = Decimal(str(buys[chosen_timestamp]["qty_left"])) - Decimal(str(chosen_qty))

        # record transactions used to realize
        # if timestamp already exists in buys/sells dictionary, add qty instead of overwriting previous realized qty(s)
        if chosen_timestamp not in sell_info["buys"]: # add qtys as floats so that dictionaries can be parsed using ast.literal_eval method
            sell_info["buys"][chosen_timestamp] = float(chosen_qty)
        else:
            sell_info["buys"][chosen_timestamp] += float(chosen_qty)
        
        if sell_timestamp not in buys[chosen_timestamp]["sells"]:
            buys[chosen_timestamp]["sells"][sell_timestamp] = float(chosen_qty)
        else:
            buys[chosen_timestamp]["sells"][sell_timestamp] += float(chosen_qty)

        # record profit
        # profit calculated by (difference in value/qty) * qty realized
        profit = (sell_valuePERqty - (buys[chosen_timestamp]["valueUSD"]/buys[chosen_timestamp]["qty"])) * chosen_qty
        buys[chosen_timestamp]["profit"] += float(profit)
        sell_profit += float(profit)
    
    sell_info["profit"] = sell_profit

#-----CREATE BUYS_REALIZED.CSV, BUYS_LEFT.CSV AND BULL_SELLS.CSV-----#

# write realized buys (i.e. qty_left = 0) into buys_realized.csv
with open("buys_realized.csv", "w", newline="") as csvfile:
    headers = ["timestamp","symbol", "qty", "qty_left", "priceUSD", "valueUSD", "exchange", "sells", "profit"] # get all headers per timestamp to iterate through
    writer = csv.DictWriter(csvfile, headers)
    writer.writeheader()
    for buy_timestamp,buy_info in sorted(buys.items()):
        if buy_info["qty_left"] == 0:
            record = {"timestamp":buy_timestamp}
            record.update(buy_info)
            writer.writerow(record)

# write unrealized buys (i.e. qty_left > 0) into buys_left.csv
with open("buys_left.csv", "w", newline="") as csvfile:
    headers = ["timestamp","symbol", "qty", "qty_left", "priceUSD", "valueUSD", "exchange", "sells", "profit"] # get all headers per timestamp to iterate through
    writer = csv.DictWriter(csvfile, headers)
    writer.writeheader()
    for buy_timestamp,buy_info in sorted(buys.items()):
        if buy_info["qty_left"] > 0:
            record = {"timestamp":buy_timestamp}
            record.update(buy_info)
            writer.writerow(record)

# write sells into sells.csv
with open("sells.csv", "w", newline="") as csvfile:
    headers = ["timestamp","symbol", "qty", "qty_left", "priceUSD", "valueUSD", "exchange", "buys", "profit"] # get all headers per timestamp to iterate through
    writer = csv.DictWriter(csvfile, headers)
    writer.writeheader()
    for sell_timestamp,sell_info in sorted(sells.items()):
        record = {"timestamp":sell_timestamp}
        record.update(sell_info)
        writer.writerow(record)

#-----UPLOAD BUYS_REALIZED.CSV, BUYS_LEFT.CSV AND SELLS.CSV TO GOOGLE CLOUD STORAGE-----#
# run on Google Compute Engine VM instance with dependency installed using "pip3 install --upgrade google-cloud-storage"

storage_client = storage.Client()
bucket = storage_client.bucket("bullsheet")
blob = bucket.blob("buys_realized.csv")
blob.upload_from_filename("/home/arthur95chionh/buys_realized.csv")

blob = bucket.blob("buys_left.csv")
blob.upload_from_filename("/home/arthur95chionh/buys_left.csv")

blob = bucket.blob("sells.csv")
blob.upload_from_filename("/home/arthur95chionh/sells.csv")
print("Uploaded buys_realized.csv, buys_left.csv and sells.csv to Cloud Storage bucket 'bullsheet'.")

#-----CREATE AND UPLOAD TRIGGER.TXT TO GOOGLE CLOUD STORAGE-----#
# triggers Cloud Function which uploads CSVs to BigQuery

trigger = open("trigger.txt", "w")
trigger.write("""This file is created and uploaded to Cloud Storage bucket bullsheet_trigger_cloudfunction.
Uploading this file triggers Cloud Function update_bigquery_bullsheet, 
which uploads CSVs in Cloud Storage bucket bullsheet to respective tables in BigQuery database Bullsheet.""")
trigger.close()

storage_client = storage.Client()
bucket = storage_client.bucket("bullsheet_trigger_cloudfunction")
blob = bucket.blob("trigger.txt")
blob.upload_from_filename("/home/arthur95chionh/trigger.txt")
print("Uploaded tigger.txt to Cloud Storage bucket 'bullsheet_trigger_cloudfunction'. Tables in BigQuery database 'Bullsheet' successfully updated.")

#-----REFERENCE: QUERY BIGQUERY-----#
# """# Explicitly create a credentials object. This allows you to use the same
# # credentials for both the BigQuery and BigQuery Storage clients, avoiding
# # unnecessary API calls to fetch duplicate authentication tokens.
# credentials, your_project_id = google.auth.default(
#     scopes=["https://www.googleapis.com/auth/cloud-platform"]
# )

# # Make clients.
# bqclient = bigquery.Client(credentials=credentials, project=your_project_id,)
# bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=credentials)"""

# # Download query results.
# #query_string = """
# #SELECT *
# #FROM `complete-axis-313516.Bullsheet.bull`
# #ORDER BY timestamp DESC
# #"""

# """bull = (
#     bqclient.query(query_string)
#     .result()
#     .to_dataframe(bqstorage_client=bqstorageclient)
# )"""

#-----REFERENCE: AGGREGATE TRADES-----#
# aggregate individual trades in same order i.e. sum qty, sum valueSGD, sum valueUSD, everything else same/first
# aggregations = {"timestamp":"first", "buysell": "first", "symbol":"first", "fiat":"first", "qty":"sum", "fee_rate":"first", "priceSGD":"first", "valueSGD": "sum", "valueUSD":"sum", "exchange":"first"}
# trades_df = trades_df.groupby("priceUSD", as_index=False).aggregate(aggregations) # as_index=False prevents priceUSD from becoming index
# print("Aggregated {} individual trades with same priceUSD.".format(len(timestamps)-len(trades_df)))
# print(trades_df)