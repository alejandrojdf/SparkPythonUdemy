import tweepy
import pandas
from tweepy import OAuthHandler,Stream
from tweepy.streaming import StreamListener
import json
import socket

consumer_key = 'I7nkFvS3AeOk0dkfA6AGsuLdQ'
consumer_secret = 'wea6sFfpYYkZmIwKb4xXXRNJwnH26FWbNXmO11rA5LCIUpETur'
access_token = '318371728-ZJtICpQgDw14IyrU1gLrN8MWnOFMf9j5vruxMdAw'
access_secret = 'T0NrvyLbEsnze8uuIVZn9vhGCllVKTui4vbhdurHM5Gj8'

class TweetListener(StreamListener):

    def __init__(self,csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            msg = json.loads(data)
            print(msg('text').encode('utf-8'))
            self.client_socket.send(msg['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print('ERROR: ', e)
        return True

    def on_error(self, status_code):
        print(status_code)
        return True

def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    twitter_stream = Stream(auth, TweetListener(c_socket))
    twitter_stream.filter(track=['batman'])

if __name__ == '__main__':
    s = socket.socket()
    host = '127.0.0.1'
    port = 9999
    s.bind((host, port))

    print(f'listening to port {port}')

    s.listen((5))
    c,addr = s.accept()

    sendData(c)

