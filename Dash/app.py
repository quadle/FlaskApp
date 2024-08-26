from flask import Flask, render_template
from dash import Dash, dcc, html, callback, Output, Input
import pandas as pd

import os
from datetime import date
from dateutil.relativedelta import relativedelta
import numpy as np
PEOPLE_FOLDER = os.path.join('static', 'images')

import plotly.express as px






def is_within_next_12_months(target_date):
    current_date = date.today()
    current_date = pd.Timestamp(current_date)
    future_date = current_date + relativedelta(months=+12)
    return current_date <= target_date <= future_date


def get_df():
    df = pd.read_excel('FlaskApp/Dash/templates/data.xlsx')
    df['totFLOORsqrft'] = df.groupby('FLOORNO')['SUITSQFT'].transform("sum")
    df['PercentageWithSquareFeet'] = df[df['SUITSQFT'] > 0].groupby('FLOORNO')['SUITSQFT'].transform('count')
    df['date'] = pd.to_datetime(df['EXPIR']).dt.date
    daTEXT = []
    for f,o,s,e,r in zip(df['SUITEID'] ,df['OCCUPANTNAME'],df['SUITSQFT'],df['date'],df['GENCODE']):
        try :

            s = int(s)
            text = str(f) + '<b>' + o.strip() + '</b>' + ' ' + r + '<br>' + '<b>Sqft</b> - ' + str(s)  + '<br>' + '<b>Vacate\Expire -</b>' + str(e)
            daTEXT.append(text)
        except :
            daTEXT.append(o)


    df['barTEXT'] = daTEXT


    data_color = []
    for ex, va in zip(df['EXPIR'],df['VACATE']):
        if isinstance(ex,float) == False:

            #e = pd.to_datetime(ex)
            e = pd.Timestamp(ex)
            d = is_within_next_12_months(e)
            if d == True:
                data_color.append('Expiration < 12 Months')
            else:
                ee = ex.year

                if ex.year >= 2027:

                    data_color.append('2027+')
                elif ex.year  <= 2022:

                    data_color.append(np.nan)
                elif isinstance(ee,float) == False:
                    data_color.append(ee)
                else:
                    data_color.append(np.nan)
        else:
            data_color.append('Vacant')
    df['Lease Status'] = data_color


    df['SUITSQFT'] = pd.to_numeric(df['SUITSQFT'])
    df['FLOORNO'] = df['FLOORNO'].astype(str)
    df['FLOORNO'] = [i.replace(" ","") for i in df['FLOORNO']]
    df['BEGINDATE'] = pd.to_datetime(df['BEGINDATE']).dt.date
    df = df.rename(columns = {'OCCUPANTNAME':'Occupant Name','BEGINDATE':'Begin Date','GENERATION':'Generation','PRIMARYCHGS':'Primary Changes'})

    return df

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
with open('FlaskApp/Dash/templates/index.html','r') as f:
    html_string = f.read()
app = Flask(__name__)
dash = Dash(__name__,  server=app, index_string=html_string)
df = get_df()
dash.layout = html.Div([

    dcc.Dropdown(sorted(df.BldgName.unique()),'Central Bank Building',id = 'drop',multi=False,
    style={'width':'60%','background-color':'rgba(0, 0, 0, 200)', 'color': 'white'}),
    html.Br(),
    dcc.Graph(id='display-selected-values',config={"toImageButtonOptions": {"width": 800, "height": 600}},style={'margin':'0'}),

    ])


app.config['UPLOAD_FOLDER'] = PEOPLE_FOLDER
# Define the route for your resume pageS
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


colors = {
    'background': 'rgba(0, 0, 0, 250)',
    'text': '#ffffff'
}
@callback(
    Output('display-selected-values', 'figure'),
    Input('drop', 'value'),



)

def update_output(value):

    df = get_df()
    data = df[df.BldgName == value]

    num = []
    non_floor = []

    def convertible(v):
        try:
            int(v)
            num.append(v)
        except (TypeError, ValueError):
            non_floor.append(v)


    dg =sorted(data['FLOORNO'].unique(),reverse=True)
    c = [i for i in dg if convertible(i)]

    color_descret = {
    'Expiration < 12 Months':"#fe2b37",
    "Vacant":"#fffeff",
    "2027+":"#fcdfa9",
    "2023":"#fed5fc",
    "2026":"#caf0b1",
    "2024":"#c9f5f9",
    "2025":"#ebd0fc",
    'Vacant New Lease':'fffeff'}

    categoty_order_l = [i for i in dg if i not in non_floor]

    categoty_order_list = categoty_order_l + non_floor
    data['totFLOORsqrft'] = data.groupby('FLOORNO')['SUITSQFT'].transform("sum")
    data['PercentageWithSquareFeet'] = data[data['SUITSQFT'] > 0].groupby('FLOORNO')['SUITSQFT'].transform('count')
    data['EXPIR'] = pd.to_datetime(data['EXPIR'])
    data['Percentage'] = (data['SUITSQFT'] / data['totFLOORsqrft']) * 100
    data['ESXP'] = data['EXPIR'].dt.year
    data['Percentage'] = (1 / data['PercentageWithSquareFeet']) * 100
    data['Lease Status'] = data['Lease Status'].astype(str)
    data = data.sort_values(by='Lease Status')


    report_name = str(value)

    # data['Percentage'] = (1 / data['PercentageWithSquareFeet']) * 100

    height = int(len(set(data['FLOORNO']))) * 100




    fig = px.bar(data, x="Percentage", y="FLOORNO",category_orders={'FLOORNO': categoty_order_list},color ='Lease Status',
    height=height,
    # width=1000,
    text='barTEXT', title=report_name,color_discrete_map=color_descret,hover_data={
                                                                                                                                                    'Occupant Name':True,
                                                                                                                                                    'SUITSQFT':False,
                                                                                                                                                    'EXPIR':False,
                                                                                                                                                    'barTEXT':False,
                                                                                                                                                    'FLOORNO':False,
                                                                                                                                                    'Begin Date':True,
                                                                                                                                                    'Generation':True,
                                                                                                                                                    'Primary Changes': True})

    fig.update_traces(insidetextanchor='middle')
    fig.update_xaxes(range=[0, 100])
    fig.update_yaxes(visible=False)
    fig.update_layout(

    bargap=0.01,bargroupgap=0.02,
    yaxis_title=None,
    xaxis_title=None,
    plot_bgcolor=colors['background'],
    paper_bgcolor=colors['background'],
    font_color=colors['text'],

    title_font=dict(size=25),

    legend=dict(

                orientation="h",
                y=-0.03,
                font=dict(size=15,color="white"),
                xanchor="center",
                bgcolor='rgba(0, 0, 0, 250)',
                x = .5),# x=0),
        hoverlabel=dict(
        font_size=16,
        font_family="Rockwell"),
    margin=dict(l=50, r=50, t=80, b=75),),

    return fig

if __name__ == '__main__':
    app.run()