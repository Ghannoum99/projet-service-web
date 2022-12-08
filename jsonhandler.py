#!/usr/bin/python3
# Authors : Ulysse Feillet - Jihad GHANNOUM - IATIC 5

import json
from suds.cache import NoCache
from suds.client import Client

dbHandlerService = Client('http://localhost:8000/DbHandlerService?wsdl', cache=NoCache())


class JsonHandler:
    def read_json(self, file):
        with open(file, 'r', encoding='utf8') as f:
            tweets = json.load(f)
            for tweet in tweets:
                tweet_string = json.dumps(tweet)
                print(tweet_string)
                dbHandlerService.service.save_tweet_db(tweet_string)


if __name__ == '__main__':
    jsonhandler = JsonHandler()
    jsonhandler.read_json("versailles_tweets_100.json")




