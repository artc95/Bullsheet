import requests
import json
import base64
import hmac
import hashlib
import datetime, time
import pandas as pd

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px

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
print(trades)

timestampms = []
buysell = []
symbol = []
currency = []
price = []
qty = []
value = []

for trade in trades:
    timestampms.append(trade["timestampms"])
    buysell.append(trade["type"])
    symbol.append(trade["symbol"])
    currency.append(trade["fee_currency"])
    price.append(trade["price"])
    qty.append(trade["amount"])
    value.append(float(trade["fee_amount"])/0.01)

trades_dict = {
    "timestamp_ms": timestampms,
    "buysell": buysell,
    "symbol": symbol,
    "currency": currency,
    "price": price,
    "qty": qty,
    "value": value
}

trades_df = pd.DataFrame(trades_dict, columns = ["timestamp_ms", "buysell", "symbol", "currency", "price", "qty", "value"])

print(trades_df)

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# assume you have a "long-form" data frame
# see https://plotly.com/python/px-arguments/ for more options
df = pd.DataFrame({
    "Fruit": ["Apples", "Oranges", "Bananas", "Apples", "Oranges", "Bananas"],
    "Amount": [4, 1, 2, 2, 4, 5],
    "City": ["SF", "SF", "SF", "Montreal", "Montreal", "Montreal"]
})

fig = px.bar(df, x="Fruit", y="Amount", color="City", barmode="group")

app.layout = html.Div(children=[
    html.H1(children='Hello Dash'),

    html.Div(children='''
        Dash: A web application framework for Python.
    '''),

    dcc.Graph(
        id='example-graph',
        figure=fig
    )
])

if __name__ == '__main__':
    app.run_server(debug=True)

