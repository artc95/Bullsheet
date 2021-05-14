import requests
import json
import base64
import hmac
import hashlib
import datetime, time
import pandas as pd

#-----GET PAST TRADES FROM GEMINI API-----#
# documentation at https://docs.gemini.com/rest-api/#get-past-trades

nonce = int(round(time.time()*1000))

#sandbox api endpoint
url = 'https://api.gemini.com/v1/mytrades'

#build the dict payload object
payload = {
	'request':'/v1/mytrades',
    'nonce': nonce,
	'symbol': ["btcusd", "ethusd"],
    'limit_trades': 500,
    'timestamp': datetime.datetime.strptime("190421,000000", "%d%m%y,%H%M%S").timestamp() # put datetime from which to search
}

#endcode payload as a json object for hashing
payload = str.encode(json.dumps(payload))

#base64 encode the payload
b64 = base64.b64encode(payload)

#get api key and secret from local .csv file
gemini_csv = pd.read_csv("gem.csv")
key = gemini_csv.loc[1,"key"]
secret = gemini_csv.loc[1,"sec"]

#create the signature using sandbox secret and encoded payload in sha384 hash
signature = hmac.new(secret.encode(), b64, hashlib.sha384).hexdigest()

#build headers as required for contacting api endpoint
headers = {
        'Content-Type':'text/plain',
        'Content-Length': "0",
        'X-GEMINI-APIKEY': key,
        'X-GEMINI-PAYLOAD': b64,
        'X-GEMINI-SIGNATURE': signature,
        'Cache-Control': "no-cache"
}

#retrieve data from POST request as response
r = requests.request("POST", url, headers=headers)

trades = r.json()

#-----PARSE JSON TO CREATE BUY / SELL RECORDS-----#

B_timestamps = []
B_symbols = []
B_fiats = []
B_prices = []
B_qtys = []
B_taker35 = [] # aggressor True = Taker = Fees 0.35%, aggressor False = Maker = Fees 0.25%

S_timestamps = []
S_symbols = []
S_fiats = []
S_prices = [] 
S_qtys = []
S_taker35 = [] # aggressor True = Taker = Fees 0.35%, aggressor False = Maker = Fees 0.25%

for trade in trades: # parse json
    if trade["type"] == "Buy":
        timestamp_epoch = trade["timestamp"]
        B_timestamps.append(datetime.datetime.fromtimestamp(timestamp_epoch).strftime("%d%m%y %H%M%S"))
        B_symbols.append(trade["symbol"][:-3])
        B_fiats.append(trade["symbol"][-3:])
        B_prices.append(float(trade["price"]))
        B_qtys.append(float(trade["amount"]))
        B_taker35.append(trade["aggressor"])
    elif trade["type"] == "Sell":
        timestamp_epoch = trade["timestamp"]
        S_timestamps.append(datetime.datetime.fromtimestamp(timestamp_epoch).strftime("%d%m%y %H%M%S"))
        S_symbols.append(trade["symbol"][:-3])
        S_fiats.append(trade["symbol"][-3:])
        S_prices.append(float(trade["price"]))
        S_qtys.append(float(trade["amount"]))
        S_taker35.append(trade["aggressor"])

B_timestamps = [datetime.datetime.strptime(i, "%d%m%y %H%M%S") for i in B_timestamps] # convert timestamps from string to datetime format for comparison
S_timestamps = [datetime.datetime.strptime(i, "%d%m%y %H%M%S") for i in S_timestamps] # convert timestamps from string to datetime format for comparison

B_pricesUSD = [] # convert prices to USD for easier comparison
for index in range(len(B_fiats)):
    if B_fiats[index] == "SGD": # convert SGD to USD by dividing by 1.335, average exchange rate
        B_pricesUSD.append(B_prices[index]/1.335)
    else:
        B_pricesUSD.append(B_prices[index])

S_pricesUSD = [] # convert prices to USD for easier comparison
for index in range(len(S_fiats)):
    if S_fiats[index] == "SGD": # convert SGD to USD by dividing by 1.335, average exchange rate
        S_pricesUSD.append(S_prices[index]/1.335)
    else:
        S_pricesUSD.append(S_prices[index])

#-----MATCH SELL TO BUY RECORDS-----#
for S_index in range(len(S_timestamps)):
    #while S_qtys[S_index] > 0: # CHECKS VALIDITY of Buy records i.e. if eternal loop, a Sell record does not have a matching Buy record!
    for B_index in range(len(B_timestamps)):
        if S_timestamps[S_index] > B_timestamps[B_index] and S_symbols[S_index] == B_symbols[B_index] and S_pricesUSD[S_index] > B_pricesUSD[B_index]:
            S_qtys[S_index],B_qtys[B_index] = max(0, S_qtys[S_index] - B_qtys[B_index]),max(0, B_qtys[B_index] - S_qtys[S_index])


#-----CHECK DATA VALIDITY > CREATE NET RECORDS-----#
net_timestamps = []
net_symbols = []
net_fiats = []
net_pricesUSD = []
net_qtys = []
net_valuesUSD = []

for index in range(len(B_timestamps)):
    if B_qtys[index] < 0: # ensure no negative Buy quantities
        raise Exception("Buy quantity is negative for B_timestamp {}.".format(B_timestamps[index]))
    elif B_qtys[index] > 0:
        net_timestamps.append(B_timestamps[index].strftime("%d%m%y %H%M%S"))
        net_symbols.append(B_symbols[index])
        net_fiats.append(B_fiats[index])
        net_pricesUSD.append(B_pricesUSD[index])
        net_qtys.append(B_qtys[index])
        net_valuesUSD.append(B_qtys[index]*B_pricesUSD[index])

print(net_symbols)
print(net_qtys)

#-----CREATE NET_DICT, THEN NET.CSV-----#
net_dict = {
    "timestamp_ddmmyy HHMMSS": net_timestamps,
    "symbol": net_symbols,
    "fiat": net_fiats,
    "priceUSD": net_pricesUSD,
    "qty": net_qtys,
    "valueUSD": net_valuesUSD
}

net_df = pd.DataFrame(net_dict, columns = ["timestamp_ddmmyy HHMMSS", "symbol", "fiat", "priceUSD", "qty", "valueUSD"])

# save df as csv
csv_file = net_df.to_csv("net.csv", index=False)