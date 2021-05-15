<h1>Python on Google Cloud Platform (Compute Engine, Cloud Storage, BigQuery), Google Data Studio</h1>
- How to automate financial data collection with Python using APIs and Google Cloud https://towardsdatascience.com/how-to-automate-financial-data-collection-with-python-using-tiingo-api-and-google-cloud-platform-b11d8c9afaa1

<h2>Create VM Instance on Compute Engine</h2>
- Create VM instance, for Free Tier (https://cloud.google.com/free/docs/gcp-free-tier/#compute) use:<br/>
&nbsp; - Machine Type = f1-micro<br/>
  - Zone = us-central1-a<br/>
- SSH into VM instance, upload Python script (using Options button at top-right-hand corner), run "sudo apt-get install python3-pip" to install Python dependencies (see 4th answer https://stackoverflow.com/questions/45188725/how-do-i-install-pip-modules-on-google-compute-engine)
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
