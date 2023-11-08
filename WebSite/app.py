from flask import Flask, render_template, url_for
from dash import Dash, dcc, html, Input, Output
import yfinance as yf
import pandas as pd
import plotly.graph_objects as go
import os

PEOPLE_FOLDER = os.path.join('static', 'images')
app = Flask(__name__)


app.config['UPLOAD_FOLDER'] = PEOPLE_FOLDER
# Define the route for your resume page
@app.route('/')
@app.route('/index')
def resume():
    full_filename = os.path.join(app.config['UPLOAD_FOLDER'], 'headshot.jpg')
    return render_template('index.html',user_image = full_filename)

@app.route('/about')
def about():
    full_filename = os.path.join(app.config['UPLOAD_FOLDER'], 'mandala.jpg')
    return render_template('about.html', man_image = full_filename)

@app.route('/contact')
def contact():
    return render_template('contact.html')

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
with open('templates/projects.html','r') as f:
    html_string = f.read()
dash = Dash(__name__, external_stylesheets=external_stylesheets, server=app, index_string=html_string)

dash.layout = html.Div([
    html.H4('Stock price analysis, Below you will see the yearly stock prices based on your selection!!'),
    dcc.Graph(id="time-series-chart"),
    
    html.P("Select stock:"),
    dcc.Dropdown(
        id="ticker",
        style={'color': 'Black', 'font-size': 20},
        options=['AMZN', 'META', "F",'DISH','LI','BRK-B','XRP-USD','SNOW','U','COST','SPCE','NTR.TO','CREV','OGI','CDIO','DOGE-USD','ALB','LLY','EGRNQ','FCEL','TRKA','FRPT','CANO','NTR','BMBL','MLTX','SOFI','BVH','SATS','RILY','VRTX','LEV.TO'],
        value="AMZN",
        clearable=False,
        
    ),
])
@dash.callback(
    Output("time-series-chart", "figure"), 
    Input("ticker", "value"))
def display_time_series(ticker):
    df = pd.DataFrame(yf.Ticker(ticker).history('12mo'))
    df['Date'] = df.index
    fig = go.Figure(data=[go.Candlestick(x=df['Date'],
                    open=df['Open'],
                    high=df['High'],
                    low=df['Low'],
                    close=df['Close'],
                    increasing_line_color= 'cyan', decreasing_line_color= 'gray')])
    fig.update_layout(template='plotly_dark')
    return fig

if __name__ == '__main__':
    app.run(debug=True)