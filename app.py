import pandas as pd
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

# assume you have a "long-form" data frame
# see https://plotly.com/python/px-arguments/ for more options
df = pd.read_csv("https://raw.githubusercontent.com/artc95/Bullsheet/master/trades.csv")

def generate_table(dataframe):
    return html.Table([
        html.Thead(
            html.Tr([html.Th(col) for col in dataframe.columns])
        ),
        html.Tbody([
            html.Tr([
                html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
            ]) for i in range(len(dataframe))
        ])
    ])

app.layout = html.Div(children=[
    html.H1(children='Trades'),
    generate_table(df)
])

if __name__ == '__main__':
    app.run_server(debug=True)
