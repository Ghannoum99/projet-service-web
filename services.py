#!/usr/bin/python3
# Authors : Ulysse Feillet - Jihad GHANNOUM - IATIC 5

import json
import logging
import pymongo

logging.basicConfig(level=logging.DEBUG)
import sys
from spyne import Application, rpc, ServiceBase, Unicode, Integer
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication
from spyne.util.wsgi_wrapper import run_twisted
from textblob import TextBlob


class DbHandlerService(ServiceBase):
    @rpc(str)
    def save_tweet_db(ctx, tweet):
        tweet_dict = json.loads(tweet)
        mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongodb_client["td2_services_web"]
        collection = db["Tweets"]
        collection.insert_one(tweet_dict)


application1 = Application([DbHandlerService],
                           tns='spyne.examples.DbHandler',
                           in_protocol=Soap11(validator='lxml'),
                           out_protocol=Soap11()
                           )


class AuthorIdentifierService(ServiceBase):
    @rpc(str, _returns=str)
    def identify_author(ctx, tweet):
        tweet_dict = json.loads(tweet)
        return tweet_dict['author_id']


application2 = Application([AuthorIdentifierService],
                           tns='spyne.examples.AuthorIdentifier',
                           in_protocol=Soap11(validator='lxml'),
                           out_protocol=Soap11()
                           )


class HashtagExtractorService(ServiceBase):
    @rpc(str, _returns=str)
    def get_hashtags(ctx, tweet):
        hashtag_list = []
        tweet_dict = json.loads(tweet)
        if "entities" in tweet_dict:
            if "hashtags" in tweet_dict['entities']:
                for tag in tweet_dict['entities']['hashtags']:
                    hashtag_list.append(tag.get('tag'))
                    # print(tag.get('tag'))
                    # print('-----------')

        return str(hashtag_list)


application3 = Application([HashtagExtractorService],
                           tns='spyne.examples.HashtagExtractor',
                           in_protocol=Soap11(validator='lxml'),
                           out_protocol=Soap11()
                           )


class SentimentAnalyzerService(ServiceBase):
    @rpc(str, _returns=str)
    def sentiment_analysis(ctx, tweet):
        tweet_dict = json.loads(tweet)
        text = tweet_dict.get("text")
        res = TextBlob(str(text))
        if res.sentiment.polarity >= 0:
            return "ce tweet est positif"
        else:
            return "ce tweet est négatif"


application4 = Application([SentimentAnalyzerService],
                           tns='spyne.examples.SentimentAnalyzer',
                           in_protocol=Soap11(validator='lxml'),
                           out_protocol=Soap11()
                           )


class TopicIdentifierService(ServiceBase):
    @rpc(str, _returns=str)
    def identify_topics(ctx, tweet):
        topics_list = []
        tweet_dict = json.loads(tweet)
        if "context_annotations" in tweet_dict:
            for domain in tweet_dict['context_annotations']:
                topics_list.append(domain.get('domain').get('name'))
                # print(domain.get('domain').get('name'))
                # print('-----------')

        return str(topics_list)


application5 = Application([TopicIdentifierService],
                           tns='spyne.examples.TopicIdentifier',
                           in_protocol=Soap11(validator='lxml'),
                           out_protocol=Soap11()
                           )


class TreatmentServices(ServiceBase):
    @rpc(str)
    def processing_data(ctx, tweet):
        result = {"author_id": "",
                  "hashtags": "",
                  "sentiment": "",
                  "topics": ""}
        author = AuthorIdentifierService.identify_author(ctx, tweet)
        hashtags = HashtagExtractorService.get_hashtags(ctx, tweet)
        sentiment = SentimentAnalyzerService.sentiment_analysis(ctx, tweet)
        topics = TopicIdentifierService.identify_topics(ctx, tweet)

        result['author_id'] = author
        result['hashtags'] = hashtags
        result['sentiment'] = sentiment
        result['topics'] = topics

        mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongodb_client["td2_services_web"]
        collection = db["treatment_results"]
        collection.insert_one(result)


application6 = Application([TreatmentServices],
                           tns='spyne.examples.treatmentServices',
                           in_protocol=Soap11(validator='lxml'),
                           out_protocol=Soap11()
                           )


class TopKHashtagsService(ServiceBase):
    @rpc(str, _returns=str)
    def get_top_k_hashtags(ctx, k):
        mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongodb_client["td2_services_web"]
        collection = db["treatment_results"]
        results = list(collection.aggregate([
            {"$group": {"_id": "$author_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]))


class TopKUsersService(ServiceBase):
    @rpc(Integer, _returns=str)
    def get_top_k_users(ctx, k):
        top_k_users = list()
        mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongodb_client["td2_services_web"]
        collection = db["treatment_results"]

        results = list(collection.aggregate([
            {"$group": {"_id": "$author_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]))

        if k <= len(results):
            for i in range(k):
                top_k_users.append(results[i]["_id"])

        return str(top_k_users)


application8 = Application([TopKUsersService],
                           tns='spyne.examples.topKUsers',
                           in_protocol=Soap11(validator='lxml'),
                           out_protocol=Soap11()
                           )


class TopKTopicsService(ServiceBase):
    @rpc(str, _returns=str)
    def get_top_k_topics(ctx, k):
        pass


class PostNumberByUserService(ServiceBase):
    @rpc(str, _returns=str)
    def get_post_number_by_user(ctx, userId):
        mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongodb_client["td2_services_web"]
        collection = db["treatment_results"]
        result = list(collection.aggregate([{ "$match" : { "author_id" : userId } } ] ))
        return str(len(result))


application10 = Application([PostNumberByUserService],
                            tns='spyne.examples.PostNumberByUser',
                            in_protocol=Soap11(validator='lxml'),
                            out_protocol=Soap11()
                            )


class PostNumberByHashtagService(ServiceBase):
    @rpc(str, _returns=str)
    def get_post_number_by_hashtag(ctx, hashtag):
        mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongodb_client["td2_services_web"]
        collection = db["treatment_results"]
        hashtags = list(collection.find({"hashtags": hashtag}))

        return str(len(hashtags))


application11 = Application([PostNumberByHashtagService],
                            tns='spyne.examples.PostNumberByHashtag',
                            in_protocol=Soap11(validator='lxml'),
                            out_protocol=Soap11()
                            )


if __name__ == '__main__':
    wsgi_app1 = WsgiApplication(application1)
    wsgi_app2 = WsgiApplication(application2)
    wsgi_app3 = WsgiApplication(application3)
    wsgi_app4 = WsgiApplication(application4)
    wsgi_app5 = WsgiApplication(application5)
    wsgi_app6 = WsgiApplication(application6)
    wsgi_app8 = WsgiApplication(application8)
    wsgi_app10 = WsgiApplication(application10)
    wsgi_app11 = WsgiApplication(application11)
    twisted_apps = [
        (wsgi_app1, b'DbHandlerService'),
        (wsgi_app2, b'AuthorIdentifierService'),
        (wsgi_app3, b'HashtagExtractorService'),
        (wsgi_app4, b'SentimentAnalyzerService'),
        (wsgi_app5, b'TopicIdentifierService'),
        (wsgi_app6, b'TreatmentServices'),
        (wsgi_app8, b'TopKUsersService'),
        (wsgi_app10, b'PostNumberByUserService'),
        (wsgi_app11, b'PostNumberByHashtagService'),
    ]
    sys.exit(run_twisted(twisted_apps, 8000))
