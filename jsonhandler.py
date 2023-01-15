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
postNumberByUserService = Client('http://localhost:8000/PostNumberByUserService?wsdl', cache=NoCache())
postNumberByHashtagService = Client('http://localhost:8000/PostNumberByHashtagService?wsdl', cache=NoCache())
postNumberByTopicService = Client('http://localhost:8000/PostNumberByTopicService?wsdl', cache=NoCache())
treatment_services = Client('http://localhost:8000/TreatmentServices?wsdl', cache=NoCache())
top_k_users = Client('http://localhost:8000/TopKUsersService?wsdl', cache=NoCache())
top_k_hashtags = Client('http://localhost:8000/TopKHashtagsService?wsdl', cache=NoCache())
top_k_topics = Client('http://localhost:8000/TopKTopicsService?wsdl', cache=NoCache())


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

    k = 3
    author_id = ""
    hashtag = ""
    topic = ""

    for tweet in collection.find():
        tweet_str = json.dumps(tweet)
        treatment_services.service.processing_data(tweet_str)

    print("les " + str(k) + " top utilisateurs : " + top_k_users.service.get_top_k_users(k) + "\n")
    print("les " + str(k) + " top hashtags : "  + top_k_hashtags.service.get_top_k_hashtags(k) + "\n")
    print("les " + str(k) + " top topics : " + top_k_topics.service.get_top_k_topics(k) + "\n")
    """
    print("le nombre de post avec un hashtag 'jira' : " + postNumberByHashtagService.service.get_post_number_by_hashtag("jira"))
    print("le nombre de post avec un hashtag 'jira' : " + postNumberByHashtagService.service.get_post_number_by_hashtag("jira"))
    print("le nombre de post avec un topic 'Person' : " + postNumberByTopicService.service.get_post_number_by_topic("Person"))
    """





