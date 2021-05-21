# query Gemini API
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
# trades
from google.cloud import storage
# bull
import google.auth
from google.cloud import bigquery
from google.cloud import bigquery_storage

#-----GET PAST TRADES FROM GEMINI API-----#
# documentation at https://docs.gemini.com/rest-api/#get-past-trades

# PAYLOAD - nonce
nonce = int(round(time()*1000))

# PAYLOAD - timestamp
# check if trades.csv already exists
trades_exists = os.path.exists("trades.csv") # https://www.guru99.com/python-check-if-file-exists.html
if trades_exists == True: # if exists, get timestamp of latest trade, then query subsequent trades and append
    existing = pd.read_csv("trades.csv")
    timestamp = existing.loc[0,"timestamp"]
    timestamp = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").timestamp() + 1 # add one second to timestamp to search for trades after and excluding last trade queried
else: # if does not exist, query from timestamp when Gemini introduced in Singapore https://medium.com/@winklevoss/gemini-is-expanding-to-hong-kong-and-singapore-42d8b973c433
    timestamp = datetime.datetime.strptime("2016-10-02 00:00:00", "%Y-%m-%d %H:%M:%S").timestamp()
print("Querying trades from {} onwards.".format(datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")))

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

for trade in trades_df: # parse json
    timestamps.append(datetime.datetime.fromtimestamp(trade["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")) # BigQuery needs timestamp in "%Y-%m-%d %H:%M:%S" format https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv
    buysell.append(trade["type"])
    symbols.append(trade["symbol"][:-3]) # e.g. LINKUSD, where before last 3 alphabets are symbol, last 3 alphabets are fiat 
    fiats.append(trade["symbol"][-3:])
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
    "valueUSD": valuesUSD
}

trades_df = pd.DataFrame(trades_dict, columns = ["timestamp", "buysell", "symbol", "fiat", "qty", "fee_rate", "priceSGD", "priceUSD", "valueSGD", "valueUSD"])
# TRY AGGREGATION FUNCTIONS https://stackoverflow.com/questions/46826773/how-can-i-merge-rows-by-same-value-in-a-column-in-pandas-with-aggregation-func
csv_file = trades_df.to_csv("trades.csv", index=False)
print("Parsed {} trades from {} to {}. trades.csv created.".format(len(trades_df),timestamps[len(trades_df)-1],timestamps[0]))

#-----UPLOAD TRADES.CSV TO GOOGLE CLOUD STORAGE-----#
# run on Google Compute Engine VM instance with dependency installed using "pip3 install --upgrade google-cloud-storage"

storage_client = storage.Client()
bucket = storage_client.bucket("bullsheet")
blob = bucket.blob("trades.csv")
blob.upload_from_filename("/home/arthur95chionh/trades.csv")

print("Uploaded trades.csv to Cloud Storage bucket 'bullsheet'.")

# ********** PROCEED FROM TRADES TO BULL **********
wait = 5
while wait:
    print("Proceeding from trades to bull in {}.".format(wait), end="\r")
    sleep(1)
    wait -= 1
print("Combining existing bull.csv with trades_df to update bull.csv.")

# *************************************************

#-----COMBINE BULL.CSV (IF ANY) WITH TRADES_DF (LATEST TRANSACTIONS ONLY, INSTEAD OF BQ TABLE "TRADES") -----#

# create lists to track Buy and Sell transactions
B_timestamps = []
B_symbols = []
B_qtys = []
B_pricesUSD = []
B_valuesUSD = []

S_timestamps = []
S_symbols = []
S_qtys = []
S_pricesUSD = []
S_valuesUSD = []

profitUSD = 0 # variable to track Profit

# check if bull.csv exists
bull_exists = os.path.exists("bull.csv")

if bull_exists == True:  # if bull.csv exists then append its Buy transactions
    bull_df = pd.read_csv("bull.csv")
    for row in range(len(bull_df)):
            B_timestamps.append(bull_df.at[row,"timestamp"])
            B_symbols.append(bull_df.at[row,"symbol"])
            B_qtys.append(bull_df.at[row,"qty"])
            B_pricesUSD.append(bull_df.at[row,"priceUSD"])
            B_valuesUSD.append(bull_df.at[row,"valueUSD"])
else: pass

# sort trades_df based on Buy or Sell
for row in range(len(trades_df)):
    if trades_df.at[row, "buysell"] == "Buy":
        B_timestamps.append(trades_df.at[row,"timestamp"])
        B_symbols.append(trades_df.at[row,"symbol"])
        B_qtys.append(trades_df.at[row,"qty"])
        B_pricesUSD.append(trades_df.at[row,"priceUSD"])
        B_valuesUSD.append(trades_df.at[row,"valueUSD"])
    elif trades_df.at[row, "buysell"] == "Sell":
        S_timestamps.append(trades_df.at[row,"timestamp"])
        S_symbols.append(trades_df.at[row,"symbol"])
        S_qtys.append(trades_df.at[row,"qty"])
        S_pricesUSD.append(trades_df.at[row,"priceUSD"])
        S_valuesUSD.append(trades_df.at[row,"valueUSD"])

# sort all Sell lists based on lowest to highest S_pricesUSD, so that lowest S_pricesUSD is matched with buy transactions first
S_pricesUSD, S_timestamps, S_symbols, S_qtys, S_valuesUSD = (list(info) for info in zip(*sorted(zip(S_pricesUSD, S_timestamps, S_symbols, S_qtys, S_valuesUSD))))

#-----APPLY PROFIT-ONLY METHOD TO CREATE BULL.CSV-----#

for S_index in range(len(S_timestamps)):
    for B_index in range(len(B_timestamps)): # if same symbol AND sell price higher than 75% buy price i.e. not severe loss (FLAWED CONDITION!!) AND sell still has qty AND buy still has qty
        if S_symbols[S_index] == B_symbols[B_index] and S_pricesUSD[S_index] > (B_pricesUSD[B_index]*0.75) and S_qtys[S_index] != 0 and B_qtys[B_index] != 0:
            S_qtys[S_index],B_qtys[B_index] = max(0, S_qtys[S_index] - B_qtys[B_index]),max(0, B_qtys[B_index] - S_qtys[S_index])

#-----CHECK DATA VALIDITY > CREATE BULL RECORDS-----#
bull_timestamps = []
bull_symbols = []
bull_qtys = []
bull_pricesUSD = []
bull_valuesUSD = []

# ensure all sell quantities = 0
for index in range(len(S_timestamps)):
    if S_qtys[index] > 0:
        raise Exception("Sell quantity is not 0 for S_timestamp {}, qty {}.".format(S_timestamps[index],S_qtys[index]))

for index in range(len(B_timestamps)):
    if B_qtys[index] < 0: # ensure no negative Buy quantities
        raise Exception("Buy quantity is negative for B_timestamp {}.".format(B_timestamps[index]))
    elif B_qtys[index] > 0:
        bull_timestamps.append(B_timestamps[index])
        bull_symbols.append(B_symbols[index])
        bull_qtys.append(B_qtys[index])
        bull_pricesUSD.append(B_pricesUSD[index])
        bull_valuesUSD.append(B_valuesUSD[index])

#-----CREATE BULL_DICT, THEN CREATE BULL.CSV-----#
bull_dict = {
    "timestamp": bull_timestamps,
    "symbol": bull_symbols,
    "qty": bull_qtys,
    "priceUSD": bull_pricesUSD,
    "valueUSD": bull_valuesUSD
}

bull_df = pd.DataFrame(bull_dict, columns = ["timestamp", "symbol", "qty", "priceUSD", "valueUSD"])
csv_file = bull_df.to_csv("bull.csv", index=False)
print("Generated bull.csv.")

#-----UPLOAD BULL.CSV TO GOOGLE CLOUD STORAGE-----#
# run on Google Compute Engine VM instance with dependency installed using "pip3 install --upgrade google-cloud-storage"

storage_client = storage.Client()
bucket = storage_client.bucket("bullsheet")
blob = bucket.blob("bull.csv")
blob.upload_from_filename("/home/arthur95chionh/bull.csv")

print("Uploaded bull.csv to Cloud Storage bucket 'bullsheet'.")

"""# Explicitly create a credentials object. This allows you to use the same
# credentials for both the BigQuery and BigQuery Storage clients, avoiding
# unnecessary API calls to fetch duplicate authentication tokens.
credentials, your_project_id = google.auth.default(
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

# Make clients.
bqclient = bigquery.Client(credentials=credentials, project=your_project_id,)
bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=credentials)"""

# Download query results.
#query_string = """
#SELECT *
#FROM `complete-axis-313516.Bullsheet.bull`
#ORDER BY timestamp DESC
#"""

"""bull = (
    bqclient.query(query_string)
    .result()
    .to_dataframe(bqstorage_client=bqstorageclient)
)"""