import sys
import socket
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import pykafka


api_key = "your_api_key"
api_secret = "your_api_secret"

access_token = "your_access_token"
access_token_secret = "your_access_token_secret"


class StdOutListener(StreamListener):

    def __init__(self, kafkaProducer):

        print ("initialize")
        self.kafkaproducer = kafkaProducer

    def on_data(self, data):

        try:
            json_message = json.loads(data)
            message = json_message["text"]
            print (message)
            self.kafkaproducer.produce(bytes(json.dumps(message), "ascii"))

        except BaseException as error:
            print(str(error))

        return True

    def on_error(self, status):

        print (status)
        return True
    

if __name__ == "__main__":

    topic = 'iot'

    kafka_client = pykafka.KafkaClient("localhost:9092")

    kafka_producer = kafka_client.topics[bytes(topic,"utf-8")].get_producer()

    l = StdOutListener(kafka_producer)
    
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(track=['basketball'], languages=["en"])


















































































































































