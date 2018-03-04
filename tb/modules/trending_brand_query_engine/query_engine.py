'''

    File name: query_engine.py
    Author: David Cuesta
    Python Version: 3.6
'''
########################################################################################################################
# IMPORTS
########################################################################################################################
import plotly.graph_objs as go
import datetime
import json
import time
########################################################################################################################
def last_message_form_kafka(consumer):

    for msg in consumer:
        msg = (msg.value).decode("utf-8")
        tweet_info = json.loads(msg)
        tweet_info_fileterd = {"timestamp": datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H:%M'),
                               "created_at": tweet_info["created_at"],
                               "text": tweet_info["text"]}
        return "@" +  str(tweet_info['user']['screen_name']) + ": " + str(tweet_info['text']) + \
               ". " +  str(tweet_info_fileterd["timestamp"])

    return "no tweet"

def sentiment_query_engine(drill2):

    query_info = drill2.query('''
        SELECT SUM(tweet) as s, dtt as f FROM dfs.`spark/sentiments/` 
        GROUP BY dtt ORDER BY dtt DESC LIMIT 10''', timeout= 120)
    count = []
    dates = []
    for a in query_info:
        count.append(int(a['s']))
        dates.append(str(a['f']).replace("-", "/"))
    dates=dates[::-1]
    count=count[::-1]

    return {
        'data': [go.Scatter(
            x=dates,
            y=count,
            mode='lines+markers'
        )],
        'layout': {
            'height': 500,
            'margin': {'l': 50, 'b': 100, 'r': 80, 't': 50},

            'annotations': [{
                'x': 0, 'y': 0.70, 'xanchor': 'left', 'yanchor': 'bottom',
                'xref': 'paper', 'yref': 'paper', 'showarrow': False,
                'align': 'left', 'bgcolor': 'rgba(255, 255, 255, 0.5)',
                'text': "Sentiment index"
            }],
            'yaxis': {'type': 'linear',
            'xaxis': {'showgrid': False}
        }
    }
    }
def tweets_per_minute(drill1):

    yelp_reviews = drill1.query('''
        SELECT SUM(tweet) as s, dtt as f FROM dfs.`spark/count/` 
         GROUP BY dtt ORDER BY dtt DESC LIMIT 50''', timeout= 120)
    dates=[]
    count=[]
    for a in yelp_reviews:
        count.append(int(a['s']))
        dates.append(str(a['f']).replace("-", "/"))
    dates=dates[::-1]
    count=count[::-1]

    return {
        'data': [go.Scatter(
            x=dates,
            y=count,
            mode='lines+markers'
        )],
        'layout': {
            'height': 500,
            'margin': {'l': 50, 'b': 150, 'r': 80, 't': 50},
            'annotations': [{
                'x': 0, 'y': 0.70, 'xanchor': 'left', 'yanchor': 'bottom',
                'xref': 'paper', 'yref': 'paper', 'showarrow': False,
                'align': 'left', 'bgcolor': 'rgba(255, 255, 255, 0.5)',
                'text': "Tweets"
            }],
            'yaxis': {'type': 'linear',
            'xaxis': {'showgrid': False}
        }
    }
    }
