import dash
from dash.dependencies import Input, Output
import dash_table
import dash_core_components as dcc
import dash_html_components as html

import pandas as pd

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

# assume you have a "long-form" data frame
# see https://plotly.com/python/px-arguments/ for more options
df = pd.read_csv("https://raw.githubusercontent.com/artc95/Bullsheet/master/trades.csv")

app.layout = html.Div([
    dash_table.DataTable(
        id="trades_table",
        columns=[{"name":i, "id":i} for i in df.columns],
        data=df.to_dict("records"),
        filter_action="native",
        sort_action="native",
    ),
    html.Div(id="trades_table_container")
])

if __name__ == '__main__':
    app.run_server(debug=True)
