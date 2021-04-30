import requests
import json
import base64
import hmac
import hashlib
import datetime, time

nonce = int(round(time.time()*1000))

#sandbox api endpoint
url = 'https://api.sandbox.gemini.com/v1/mytrades'

#build the dict payload object
payload = {
	'request':'/v1/mytrades',
        'nonce': nonce,
	'symbol': "btcusd"
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
response = requests.request("POST", url, headers=headers)

print(response.text)