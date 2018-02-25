'''

    File name: ui.py
    Author: David Cuesta
    Python Version: 3.6
'''
########################################################################################################################
# IMPORTS
########################################################################################################################
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Event
from flask import Flask
import os
from pydrill.client import PyDrill
from tb.modules.trending_brand_query_engine.query_engine import  tweets_per_minute,sentiment_query_engine, last_message_form_kafka
from kafka import KafkaConsumer
########################################################################################################################


consumer = KafkaConsumer('beer', group_id='ui2')
drill1 = PyDrill(host='localhost', port=8047)
drill2 = PyDrill(host='localhost', port=8047)

server = Flask('my app')
server.secret_key = os.environ.get('secret_key', 'secret')

app = dash.Dash('streaming-wind-app', server=server,
                url_base_pathname='/BEER/',
                csrf_protect=False)

app.layout = html.Div([
    html.Div([
        html.H2("Trending-Brand: Beer consumption"),
    ], className='banner'),
    html.Div([
        html.Div([
            html.H3("TWEETS/MINUTE")
        ], className='Title'),
        html.Div([
            dcc.Graph(id='tweets-per-minute', figure = tweets_per_minute(drill1)),
        ], className='twelve columns tweets-per-minute'),
        dcc.Interval(id='wind-speed-update', interval=30000),
    ], className='row wind-speed-row'),
    html.Div([
        html.Div([
            html.H3("SENTIMENT ANALYSIS")
        ], className='Title'),
        html.Div([
            dcc.Graph(id='sentiment-analysis', figure = sentiment_query_engine(drill2)),
        ], className='twelve columns wind-speed'),
        dcc.Interval(id='wind-speed-update2', interval=30000),

    ], className='row wind-speed-row'),
html.Div([
        html.Div([
            html.H3("LAST TWEEET")
        ], className='Title'),
        html.Div([
            dcc.Markdown(id='last-tweet', children=str(last_message_form_kafka(consumer))),
        ], className='twelve columns wind-speed'),
    ], className='row wind-speed-row'),
    dcc.Interval(id='wind-speed-update3', interval=1000),

], style={'padding': '0px 10px 15px 10px',
          'marginLeft': 'auto', 'marginRight': 'auto', "width": "900px",'maginTop':'auto','marginBottom':'auto',
          'boxShadow': '0px 0px 5px 5px rgba(204,204,204,0.4)'})


@app.callback(Output('tweets-per-minute', 'figure'), [],
              [],
              [Event('wind-speed-update', 'interval')])
def gen_wind_speed():
    return tweets_per_minute(drill1)

@app.callback(output=dash.dependencies.Output('last-tweet', 'children'),events=
                 [dash.dependencies.Event('wind-speed-update3', 'interval')])
def update():
    return str(last_message_form_kafka(consumer))

@app.callback(Output('sentiment-analysis', 'figure'), [],
              [],
              [Event('wind-speed-update2', 'interval')])
def sentiment_analysis():
    return  sentiment_query_engine(drill2)


external_css = ["https://cdnjs.cloudflare.com/ajax/libs/skeleton/2.0.4/skeleton.min.css",
                "https://cdn.rawgit.com/plotly/dash-app-stylesheets/737dc4ab11f7a1a8d6b5645d26f69133d97062ae/dash-wind-streaming.css",
                "https://fonts.googleapis.com/css?family=Raleway:400,400i,700,700i",
                "https://fonts.googleapis.com/css?family=Product+Sans:400,400i,700,700i"]


for css in external_css:
    app.css.append_css({"external_url": css})
"""
if 'DYNO' in os.environ:
    app.scripts.append_script({
        'external_url': 'https://cdn.rawgit.com/chriddyp/ca0d8f02a1659981a0ea7f013a378bbd/raw/e79f3f789517deec58f41251f7dbb6bee72c44ab/plotly_ga.js'
    })
"""
if __name__ == '__main__':
    app.run_server()
