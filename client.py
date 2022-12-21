#!/usr/bin/python3
# Authors : Ulysse Feillet - Jihad GHANNOUM - IATIC 5

import json
from suds.client import Client


file = "versailles_tweets_100.json"
dbHandlerService = Client('http://localhost:8000/DbHandlerService?wsdl')
authorIdentifierService = Client('http://localhost:8000/AuthorIdentifierService?wsdl')

with open(file, 'r', encoding='utf8') as f:
    tweets = json.load(f)
    for tweet in tweets:
        tweet_string = json.dumps(tweet)
        print(tweet_string)
        dbHandlerService.service.save_tweet_db(tweet_string)
        authorIdentifierService.service.identify_author(tweet_string)

