'''

    File name: twitter_publisher.py
    Author: David Cuesta
    Python Version: 3.6
'''
########################################################################################################################
# IMPORTS
########################################################################################################################
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, SimpleClient
import json
import os
########################################################################################################################


with open(os.path.expanduser('~/.twitter_credentials.json')) as credentials:
    twitter_credentials = json.load(credentials)

access_token = twitter_credentials["access_token"]
access_token_secret =  twitter_credentials["access_token_secret"]
consumer_key =  twitter_credentials["consumer_key"]
consumer_secret =  twitter_credentials["consumer_secret"]

print(consumer_secret)
class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send_messages("beer", data.encode('utf-8'))
        print(data)
        return True
    def on_error(self, status):
        print (status)


kafka = SimpleClient("kafka_1:9092")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track=["beer"])