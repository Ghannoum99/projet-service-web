#!/usr/bin/python3
# Authors : Ulysse Feillet - Jihad GHANNOUM - IATIC 5

import json

import pymongo
from suds.cache import NoCache
from suds.client import Client


mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongodb_client["td2_services_web"]
collection = db["Tweets"]
dbHandlerService = Client('http://localhost:8000/DbHandlerService?wsdl', cache=NoCache())
authorIdentifierService = Client('http://localhost:8000/AuthorIdentifierService?wsdl', cache=NoCache())
hashtagExtractorService = Client('http://localhost:8000/HashtagExtractorService?wsdl', cache=NoCache())
sentimentAnalyzerService = Client('http://localhost:8000/SentimentAnalyzerService?wsdl', cache=NoCache())
topicIdentifierService = Client('http://localhost:8000/TopicIdentifierService?wsdl', cache=NoCache())


class JsonHandler:
    def read_json(self, file):
        with open(file, 'r', encoding='utf8') as f:
            tweets = json.load(f)
            for tweet in tweets:
                print(tweet)
                tweet_string = json.dumps(tweet)
                dbHandlerService.service.save_tweet_db(tweet_string)


if __name__ == '__main__':
    #jsonhandler = JsonHandler()
    #jsonhandler.read_json("versailles_tweets_100.json")
    for tweet in collection.find():
        tweet_str = json.dumps(tweet)
        #print(authorIdentifierService.service.identify_author(tweet_str))
        #print(hashtagExtractorService.service.extract_hashtag(tweet_str))
        #print(sentimentAnalyzerService.service.sentiment_analysis(tweet_str))
        print(topicIdentifierService.service.identify_topics(tweet_str))





