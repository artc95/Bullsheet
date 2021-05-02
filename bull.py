import requests
import json
import base64
import hmac
import hashlib
import datetime, time
import pandas as pd

nonce = int(round(time.time()*1000))

#sandbox api endpoint
url = 'https://api.sandbox.gemini.com/v1/mytrades'

#build the dict payload object
payload = {
	'request':'/v1/mytrades',
    'nonce': nonce,
	'symbol': ["btcusd", "ethusd"]
}

#endcode payload as a json object for hashing
payload = str.encode(json.dumps(payload))

#base64 encode the payload
b64 = base64.b64encode(payload)

#create the signature using sandbox secret and encoded payload in sha384 hash
signature = hmac.new(str.encode("4L3ufQ8oXcUy5k1wGGssS7RH59WX"), b64, hashlib.sha384).hexdigest()

#build headers as required for contacting api endpoint
headers = {
        'Content-Type':'text/plain',
        'X-GEMINI-APIKEY': "account-MzGIlSFJxj6eg3DKpBUm",
        'X-GEMINI-PAYLOAD': b64,
        'X-GEMINI-SIGNATURE': signature
}

#retrieve data from POST request as response
r = requests.request("POST", url, headers=headers)

trades = r.json()

timestampms = []
buysell = []
symbol = []
currency = []
price = []
qty = []
value = []

for trade in trades:
    timestamp_epoch = trade["timestampms"]/1000
    timestampms.append(datetime.datetime.fromtimestamp(timestamp_epoch).strftime("%d%m%y %H%M%S.%f"))
    
    buysell.append(trade["type"])
    symbol.append(trade["symbol"])
    currency.append(trade["fee_currency"])
    price.append(trade["price"])
    qty.append(trade["amount"])
    value.append(float(trade["fee_amount"])/0.01)

trades_dict = {
    "timestamp_ddmmyy HHMMSS.f": timestampms,
    "buysell": buysell,
    "symbol": symbol,
    "currency": currency,
    "price": price,
    "qty": qty,
    "value": value
}

trades_df = pd.DataFrame(trades_dict, columns = ["timestamp_ddmmyy HHMMSS.f", "buysell", "symbol", "currency", "price", "qty", "value"])

# save df as csv
csv_name = "trades.csv"
csv_file = trades_df.to_csv(csv_name, index=False)