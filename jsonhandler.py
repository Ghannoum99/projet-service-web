#!/usr/bin/python3
# Authors : Ulysse Feillet - Jihad GHANNOUM - Rim Sliti - IATIC 5
# Projet SOA : Traitement et analyse de données de réseaux sociaux à base de Services Web

import json
import pymongo
from pymongo.errors import DuplicateKeyError
from suds import WebFault
from suds.cache import NoCache
from suds.client import Client

mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongodb_client["td2_services_web"]
collection = db["Tweets"]

dbHandlerService = Client('http://localhost:8000/DbHandlerService?wsdl', cache=NoCache())
treatment_services = Client('http://localhost:8000/TreatmentServices?wsdl', cache=NoCache())
analysis_services = Client('http://localhost:8000/AnalysisServices?wsdl', cache=NoCache())


class JsonHandler:
    def read_json(self, file):
        with open(file, 'r', encoding='utf8') as f:
            tweets = json.load(f)
            try:
                for tweet in tweets:
                    tweet_string = json.dumps(tweet)
                    dbHandlerService.service.save_tweet_db(tweet_string)

                print("\n" + "Done : tweets have been stored" + "\n")

            except WebFault:
                print("\n" + "The tweets have already been stored" + "\n")


if __name__ == '__main__':
    tweets_file = "versailles_tweets_100.json"
    k = 3
    author_id = "1339914264522461187"
    hashtag = "CIV"
    topic = "Max Gradel"

    jsonhandler = JsonHandler()
    jsonhandler.read_json(tweets_file)

    for tweet in collection.find():
        tweet_str = json.dumps(tweet)
        treatment_result = treatment_services.service.processing_data(tweet_str)

    # print(treatment_result)

    print(analysis_services.service.analyze_services(k, author_id, hashtag, topic))
