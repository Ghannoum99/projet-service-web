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
        return "Lidentifiant de l'auteur de ce tweet est : " + tweet_dict['author_id']


application2 = Application([AuthorIdentifierService],
                          tns='spyne.examples.AuthorIdentifier',
                          in_protocol=Soap11(validator='lxml'),
                          out_protocol=Soap11()
                          )


class HashtagExtractorService(ServiceBase):
    @rpc(str, _returns=str)
    def extract_hashtag(ctx, tweet):
        hashtag_list = []
        tweet_dict = json.loads(tweet)
        if "entities" in tweet_dict:
            if "hashtags" in tweet_dict['entities']:
                for tag in tweet_dict['entities']['hashtags']:
                    hashtag_list.append(tag.get('tag'))
                    print(tag.get('tag'))
                    print('-----------')

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
                print(domain.get('domain').get('name'))
                print('-----------')

        return str(topics_list)


application5 = Application([TopicIdentifierService],
                           tns='spyne.examples.TopicIdentifier',
                           in_protocol=Soap11(validator='lxml'),
                           out_protocol=Soap11()
                           )


class PostNumberByUserService(ServiceBase):
    @rpc(str, _returns=str)
    def getPostNumber(ctx, userId):
        mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongodb_client["td2_services_web"]
        collection = db["Tweets"]
        post_number = collection.count_documents({"author_id": userId})
        return str(post_number+1)


application6 = Application([PostNumberByUserService],
                           tns='spyne.examples.PostNumberByUser',
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
    twisted_apps = [
        (wsgi_app1, b'DbHandlerService'),
        (wsgi_app2, b'AuthorIdentifierService'),
        (wsgi_app3, b'HashtagExtractorService'),
        (wsgi_app4, b'SentimentAnalyzerService'),
        (wsgi_app5, b'TopicIdentifierService'),
        (wsgi_app6, b'PostNumberByUserService'),
    ]
    sys.exit(run_twisted(twisted_apps, 8000))
