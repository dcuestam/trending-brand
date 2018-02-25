import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State, Event
import plotly.plotly as py
from plotly.graph_objs import *
from scipy.stats import rayleigh
from flask import Flask
import numpy as np
import pandas as pd
import os
import sqlite3
import datetime as dt
import dash
import pandas as pd
import plotly.graph_objs as go
import dash.dependencies as ddp
import dash_core_components as dcc
import dash_html_components as html
from kafka import KafkaConsumer
import datetime
import time
import json
from random import randint
from pydrill.client import PyDrill
"""
drill = PyDrill(host='localhost', port=8047)


yelp_reviews = drill.query('''
SELECT SUM(tweet) as s, fecha as f FROM dfs.`spark/sentiments/` WHERE fecha > '2018-02-21 21:54' GROUP BY fecha''')
for a in yelp_reviews:
    print(a)
    """
def kafka_consumer():
    """
    consumer = KafkaConsumer('beer')
    for msg in consumer:
        msg = (msg.value).decode("utf-8")

        '''
        if(file_name!=datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H:%M')):
            file.close()
            #convert_json(file_name, file_name[:len(file_name) - 5] + ".parquet",schema)
            file_name=datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H:%M')+ ".json"
            file= open(file_name,"w")
        print()
        '''
        tweet_info = json.loads(msg)
        print(tweet_info)
        tweet_info_fileterd = {"timestamp": datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H:%M'),
                               "created_at": tweet_info["created_at"],
                               "text": tweet_info["text"]}
        print("---tweet_info" + str(tweet_info_fileterd))
        return "@" +  str(tweet_info['user']['screen_name']) + ": " + str(tweet_info['text']) + ". " +  str(tweet_info_fileterd["timestamp"])
"""
    return "no tweet"


server = Flask('trending-brand')
server.secret_key = os.environ.get('secret_key', 'secret')

app = dash.Dash('trending-brand', server=server,
                url_base_pathname='/NBA/',
                csrf_protect=False)

app.layout = html.Div([
    html.Div([
        html.H2("TRENDING-BRAND"),
        html.Img(src="https://raw.githubusercontent.com/davidcuestam/trending-brand/master/modules/trending-brand-ui/trending-brand.png"),
    ], className='banner'),
    html.Div([
        html.Div([
            html.H3("NUMBER OF TWEETS")
        ], className='Title'),
        html.Div([
            dcc.Graph(id='wind-speed'),
        ], className='twelve columns wind-speed'),
        dcc.Interval(id='wind-speed-update', interval=10000),
    ], className='row wind-speed-row'),
    html.Div([
        html.Div([
            html.Div([
                html.H3("SENTIMENT ANALYSIS")
            ], className='Title'),
            dcc.Graph(id='wind-histogram'),
        ], className='seven columns wind-histogram'),

        html.Div([
            html.Div([
                html.H3("LAST TWEET")
            ], className='Title'),
            dcc.Markdown(id='plot', children=str(kafka_consumer()))
        ], className='five columns wind-polar')
    ], className='row wind-histo-polar'),
], style={'padding': '0px 10px 15px 10px',
          'marginLeft': 'auto', 'marginRight': 'auto', "width": "900px",
          'boxShadow': '0px 0px 5px 5px rgba(204,204,204,0.4)'})



@app.callback(output=dash.dependencies.Output('plot', 'children'),events=
                 [dash.dependencies.Event('wind-speed-update', 'interval')])
def update():
    return str(kafka_consumer())

server = app.server
@server.after_request
def add_header(r):
    """
    Add headers to both force latest IE rendering engine or Chrome Frame,
    and also to cache the rendered page for 10 minutes.
    """
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    r.headers['Cache-Control'] = 'public, max-age=0'
    return r



@app.callback(Output('wind-speed', 'figure'), [],
              [],
              [Event('wind-speed-update', 'interval')])
def gen_wind_speed():
    now = dt.datetime.now()
    sec = now.second
    minute = now.minute
    hour = now.hour

    total_time = (hour * 3600) + (minute * 60) + (sec)

    #con = sqlite3.connect("./Data/wind-data.db")
    #df = pd.read_sql_query('SELECT Speed, SpeedError, Direction from Wind where\
     #                       rowid > "{}" AND rowid <= "{}";'
     #                       .format(total_time-200, total_time), con)

    trace1 = {
        "x": ["12/30/11 23:00", "12/31/11 0:00", "12/31/11 1:00", "12/31/11 2:00", "12/31/11 3:00", "12/31/11 4:00",
              "12/31/11 5:00", "12/31/11 6:00", "12/31/11 7:00", "12/31/11 8:00", "12/31/11 9:00", "12/31/11 10:00",
              "12/31/11 11:00", "12/31/11 12:00", "12/31/11 13:00", "12/31/11 14:00", "12/31/11 15:00",
              "12/31/11 16:00", "12/31/11 17:00", "12/31/11 18:00", "12/31/11 19:00", "12/31/11 20:00",
              "12/31/11 21:00", "12/31/11 22:00", "12/31/11 23:00", "1/1/12 0:00", "1/1/12 1:00", "1/1/12 2:00",
              "1/1/12 3:00", "1/1/12 4:00", "1/1/12 5:00", "1/1/12 6:00", "1/1/12 7:00", "1/1/12 8:00", "1/1/12 9:00",
              "1/1/12 10:00", "1/1/12 11:00", "1/1/12 12:00", "1/1/12 13:00", "1/1/12 14:00", "1/1/12 15:00"],
        "y": [8, 40327, 39938, 45133, 47009, 45705, 45841, 44520, 44430, 50774, 66330, 93642, 137345, 167380, 237606,
              278185, 242886, 510205, 954878, 612498, 416047, 354282, 383924, 462609, 698534, 807354, 541200, 593989,
              510280, 501744, 1062100, 648903, 352523, 363534, 214594, 194403, 187932, 194569, 208644, 254083, 259707],
        "marker": {"color": "rgb(31, 119, 180)"},
        "name": "Col3",
        "type": "bar"
    }
    data = Data([trace1])
    layout = {
        "autosize": False,
        "bargap": 0.2,
        "bargroupgap": 0,
        "barmode": "stack",
        "boxgap": 0.3,
        "boxgroupgap": 0.3,
        "boxmode": "overlay",
        "dragmode": "zoom",
        "font": {
            "color": "#000",
            "family": "\"PT Sans Narrow\", sans-serif",
            "size": 12
        },
        "height": 600,
        "hovermode": "x",
        "legend": {
            "bgcolor": "#fff",
            "bordercolor": "#000",
            "borderwidth": 1,
            "font": {
                "color": "",
                "family": "",
                "size": 0
            },
            "traceorder": "normal"
        },
        "margin": {
            "r": 80,
            "t": 100,
            "b": 80,
            "l": 80,
            "pad": 0
        },
        "paper_bgcolor": "#fff",
        "plot_bgcolor": "#fff",
        "separators": ".,",
        "title": "\"New Year\" Related Tweets Worldwide",
        "titlefont": {
            "color": "",
            "family": "",
            "size": 22
        },
        "width": 900,
        "xaxis": {
            "anchor": "y",
            "autorange": True,
            "autotick": False,
            "domain": [0, 1],
            "dtick": 4,
            "exponentformat": "e",
            "gridcolor": "#ddd",
            "gridwidth": 1,
            "linecolor": "#000",
            "linewidth": 1,
            "mirror": False,
            "nticks": 0,
            "overlaying": False,
            "position": 0,
            "range": [-0.5, 47.5],
            "rangemode": "normal",
            "showexponent": "all",
            "showgrid": False,
            "showline": True,
            "showticklabels": True,
            "tick0": 0,
            "tickangle": "auto",
            "tickcolor": "rgba(0, 0, 0, 0)",
            "tickfont": {
                "color": "",
                "family": "",
                "size": 0
            },
            "ticklen": 5,
            "ticks": "outside",
            "tickwidth": 1,
            "title": "GMT",
            "titlefont": {
                "color": "",
                "family": "",
                "size": 0
            },
            "type": "category",
            "zeroline": False,
            "zerolinecolor": "#000",
            "zerolinewidth": 1
        },
        "yaxis": {
            "anchor": "x",
            "autorange": True,
            "autotick": True,
            "domain": [0, 1],
            "dtick": 200000,
            "exponentformat": "e",
            "gridcolor": "#ddd",
            "gridwidth": 1,
            "linecolor": "#000",
            "linewidth": 1,
            "mirror": False,
            "nticks": 0,
            "overlaying": False,
            "position": 0,
            "range": [0, 1118000],
            "rangemode": "normal",
            "showexponent": "all",
            "showgrid": True,
            "showline": True,
            "showticklabels": True,
            "tick0": 0,
            "tickangle": "auto",
            "tickcolor": "rgba(0, 0, 0, 0)",
            "tickfont": {
                "color": "",
                "family": "",
                "size": 0
            },
            "ticklen": 5,
            "ticks": "outside",
            "tickwidth": 1,
            "title": "tweets/ hr",
            "titlefont": {
                "color": "",
                "family": "",
                "size": 0
            },
            "type": "linear",
            "zeroline": False,
            "zerolinecolor": "#000",
            "zerolinewidth": 1
        }
    }
    return Figure(data=data, layout=layout)

"""
@app.callback(Output('plot', 'children'), [],
              [],
              [Event('wind-speed-update', 'interval')])
def gen_wind_direction():
    return "Tweet test"
"""

@app.callback(Output('wind-histogram', 'figure'),
              [],[],
              [Event('wind-speed-update', 'interval')])
def gen_wind_histogram():
    wind_val = []

    trace1 = {
        "x": ["12/30/11 23:00", "12/31/11 0:00", "12/31/11 1:00", "12/31/11 2:00", "12/31/11 3:00", "12/31/11 4:00",
              "12/31/11 5:00", "12/31/11 6:00", "12/31/11 7:00", "12/31/11 8:00", "12/31/11 9:00", "12/31/11 10:00",
              "12/31/11 11:00", "12/31/11 12:00", "12/31/11 13:00", "12/31/11 14:00", "12/31/11 15:00",
              "12/31/11 16:00", "12/31/11 17:00", "12/31/11 18:00", "12/31/11 19:00", "12/31/11 20:00",
              "12/31/11 21:00", "12/31/11 22:00", "12/31/11 23:00", "1/1/12 0:00", "1/1/12 1:00", "1/1/12 2:00",
              "1/1/12 3:00", "1/1/12 4:00", "1/1/12 5:00", "1/1/12 6:00", "1/1/12 7:00", "1/1/12 8:00", "1/1/12 9:00",
              "1/1/12 10:00", "1/1/12 11:00", "1/1/12 12:00", "1/1/12 13:00", "1/1/12 14:00", "1/1/12 15:00",
              "1/1/12 16:00", "1/1/12 17:00", "1/1/12 18:00", "1/1/12 19:00", "1/1/12 20:00", "1/1/12 21:00",
              "1/1/12 22:00"],
        "y": [8, 40327, 39938, 45133, 47009, 45705, 45841, 44520, 44430, 50774, 66330, 93642, 137345, 167380, 237606,
              278185, 242886, 510205, 954878, 612498, 416047, 354282, 383924, 462609, 698534, 807354, 541200, 593989,
              510280, 501744, 1062100, 648903, 352523, 363534, 214594, 194403, 187932, 194569, 208644, 254083, 259707,
              248718, 234990, 218121, 193565, 163479, 151588, 133513],
        "marker": {"color": "rgb(31, 119, 180)"},
        "name": "Col3",
        "type": "bar"
    }
    data = Data([trace1])
    layout = {
        "autosize": False,
        "bargap": 0.2,
        "bargroupgap": 0,
        "barmode": "stack",
        "boxgap": 0.3,
        "boxgroupgap": 0.3,
        "boxmode": "overlay",
        "dragmode": "zoom",
        "font": {
            "color": "#000",
            "family": "\"PT Sans Narrow\", sans-serif",
            "size": 12
        },
        "height": 500,
        "hovermode": "x",
        "legend": {
            "bgcolor": "#fff",
            "bordercolor": "#000",
            "borderwidth": 1,
            "font": {
                "color": "",
                "family": "",
                "size": 0
            },
            "traceorder": "normal"
        },
        "margin": {
            "r": 80,
            "t": 100,
            "b": 80,
            "l": 80,
            "pad": 0
        },
        "paper_bgcolor": "#fff",
        "plot_bgcolor": "#fff",
        "separators": ".,",
        "title": "\"New Year\" Related Tweets Worldwide",
        "titlefont": {
            "color": "",
            "family": "",
            "size": 22
        },
        "width": 600,
        "xaxis": {
            "anchor": "y",
            "autorange": True,
            "autotick": False,
            "domain": [0, 1],
            "dtick": 4,
            "exponentformat": "e",
            "gridcolor": "#ddd",
            "gridwidth": 1,
            "linecolor": "#000",
            "linewidth": 1,
            "mirror": False,
            "nticks": 0,
            "overlaying": False,
            "position": 0,
            "range": [-0.5, 47.5],
            "rangemode": "normal",
            "showexponent": "all",
            "showgrid": False,
            "showline": True,
            "showticklabels": True,
            "tick0": 0,
            "tickangle": "auto",
            "tickcolor": "rgba(0, 0, 0, 0)",
            "tickfont": {
                "color": "",
                "family": "",
                "size": 0
            },
            "ticklen": 5,
            "ticks": "outside",
            "tickwidth": 1,
            "title": "GMT",
            "titlefont": {
                "color": "",
                "family": "",
                "size": 0
            },
            "type": "category",
            "zeroline": False,
            "zerolinecolor": "#000",
            "zerolinewidth": 1
        },
        "yaxis": {
            "anchor": "x",
            "autorange": True,
            "autotick": True,
            "domain": [0, 1],
            "dtick": 200000,
            "exponentformat": "e",
            "gridcolor": "#ddd",
            "gridwidth": 1,
            "linecolor": "#000",
            "linewidth": 1,
            "mirror": False,
            "nticks": 0,
            "overlaying": False,
            "position": 0,
            "range": [0, 1118000],
            "rangemode": "normal",
            "showexponent": "all",
            "showgrid": True,
            "showline": True,
            "showticklabels": True,
            "tick0": 0,
            "tickangle": "auto",
            "tickcolor": "rgba(0, 0, 0, 0)",
            "tickfont": {
                "color": "",
                "family": "",
                "size": 0
            },
            "ticklen": 5,
            "ticks": "outside",
            "tickwidth": 1,
            "title": "tweets/ hr",
            "titlefont": {
                "color": "",
                "family": "",
                "size": 0
            },
            "type": "linear",
            "zeroline": False,
            "zerolinecolor": "#000",
            "zerolinewidth": 1
        }
    }
    return Figure(data=data, layout=layout)


external_css = ["https://cdnjs.cloudflare.com/ajax/libs/skeleton/2.0.4/skeleton.min.css",
                "https://cdn.rawgit.com/plotly/dash-app-stylesheets/737dc4ab11f7a1a8d6b5645d26f69133d97062ae/dash-wind-streaming.css",
                "https://fonts.googleapis.com/css?family=Raleway:400,400i,700,700i",
                "https://fonts.googleapis.com/css?family=Product+Sans:400,400i,700,700i"]

for css in external_css:
    app.css.append_css({"external_url": css})

if 'DYNO' in os.environ:
    app.scripts.append_script({
        'external_url': 'https://cdn.rawgit.com/chriddyp/ca0d8f02a1659981a0ea7f013a378bbd/raw/e79f3f789517deec58f41251f7dbb6bee72c44ab/plotly_ga.js'
    })

if __name__ == '__main__':
    app.run_server()
