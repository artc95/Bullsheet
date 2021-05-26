***TLDR*** (summary of ***PROBLEM***): Approaching cryptocurrency investment **per transaction** may be more lucrative than using **average**. **Bullsheet** seeks to automate and simplify **per transaction** management of cryptocurrency investment. 

***PROBLEM:***<br>
Traditionally, investments are calculated using **average** prices and are not managed/realized **per transaction**.<br>
However, an **average** approach might not be as effective for cryptocurrency investment. Because cryptocurrency is highly volatile, the **average** approach may present psychological barriers. A scenario - you buy 1 Bitcoin (BTC) at USD 50000. It drops to USD 40000, and you buy 1 more. Your average buy price is between USD 45000, but BTC drops further to USD 30000. Psychological barriers include:
1. **BIG DIP** - The current price is way below your **average** price, so you may be psychologically paralyzed from "buying the dip" or psychologically pressured to sell at a loss, only to regret/FOMO when it bounces back up. HOWEVER, a **per transaction** approach may help you temporarily ignore your previous purchases, and perceive the current dip as new territory to invest in.
2. **BOUNCING BEAR** - You may "buy the dip" and buy 1 BTC at USD 30000. Now your average price is USD 40000. But BTC moves sideways over time, hovering between USD 30000 and USD 35000, never reaching USD 40000. During this sideway movement, you may be psychologically paralyzed from selling at USD 35000 and buying back at USD 30000 because it's all below your average price of USD 40000. HOWEVER, a **per transaction** approach may help you focus on "riding the waves" of BTC's bounce. Selling at USD 35000 and buying back at USD 30000 may not produce great profits, but is USD 0.01 profit insignificant?

***SOLUTION:***

<h1>BULLSHEET</h1><br/>
Python, SQL on Google Cloud Platform (Compute Engine, Cloud Storage, Cloud Functions, BigQuery) and Google Data Studio
- Live at https://datastudio.google.com/reporting/43c0182a-33b4-42ab-823d-3515366f6e90<br/>
- With reference to "How to automate financial data collection with Python using APIs and Google Cloud https://towardsdatascience.com/how-to-automate-financial-data-collection-with-python-using-tiingo-api-and-google-cloud-platform-b11d8c9afaa1"<br/>
- DAG: <br/>
<img src="https://github.com/artc95/Bullsheet/blob/master/Bullsheet_DAG.PNG?raw=true" width="50%" height="50%"><br/>

<h2>Create VM Instance in Compute Engine</h2>
- Create VM instance, for Free Tier (https://cloud.google.com/free/docs/gcp-free-tier/#compute) use:<br/>
&nbsp; - Machine Type = f1-micro<br/>
&nbsp; - Zone = us-central1-a<br/>
- SSH into VM instance, upload Python script (using Options button at top-right-hand corner), run "sudo apt-get install python3-pip" to install pip (see 4th answer https://stackoverflow.com/questions/45188725/how-do-i-install-pip-modules-on-google-compute-engine), run "pip3 install DEPENDENCY" to install dependencies for Python script e.g. "pip3 install requests", "pip3 install pandas", "pip3 install --upgrade google-cloud-storage"<br/>
  - dependencies to query BigQuery (https://stackoverflow.com/questions/56357794/unable-to-install-grpcio-using-pip-install-grpcio): "pip3 install --upgrade pip", "python3 -m pip install --upgrade setuptools", "pip3 install --no-cache-dir  --force-reinstall -Iv grpcio==<version_number>" (get version number from https://pypi.org/project/grpcio/), "pip3 install google-cloud-bigquery", "pip3 install google-cloud-bigquery-storage", "pip3 install pyarrow"<br/>
  - query BigQuery using Python https://cloud.google.com/bigquery/docs/bigquery-storage-python-pandas#pip

<h2>Create Bucket in Cloud Storage</h2>
- Create Bucket in us-central1 for Free Tier (https://cloud.google.com/free/docs/gcp-free-tier/#storage)

<h2>Create Dataset and Load Table in BigQuery</h2>
- Create Dataset and Table in data location US to access Cloud Storage Bucket in us-central1<br/>

<h2>Create Cloud Function to update BigQuery table with new file written in Cloud Storage Bucket</h2>
- Loading csv data from Cloud Storage into BigQuery https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv<br/>
- Add requirements.txt file with necessary dependencies e.g. google-cloud-bigquery==2.16.1<br/>
  ------------------------------------------

<h1>Python Dash App on Heroku<h1>
<h2>Write simple Dash app</h2>
- tutorial at https://dash.plotly.com/installation

<h2>Deploy Dash app using Heroku</h2>

- tutorial at https://devcenter.heroku.com/articles/getting-started-with-python  
- samples of necessary files (e.g. Procfile, requirements.txt, runtime.txt) https://github.com/austinlasseter/flying-dog-beers (tutorial at https://austinlasseter.medium.com/deploy-a-plotly-dash-app-on-heroku-4d2c3224230)  
- explanation of process https://towardsdatascience.com/deploying-your-dash-app-to-heroku-the-magical-guide-39bd6a0c586c
- app.py must have the line "server = app.server" !!!!!

<h2>TBC: Deploy Dash app using Google App Engine</h2>
https://realpython.com/python-web-applications/
