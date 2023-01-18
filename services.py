#!/usr/bin/python3
# Authors : Ulysse Feillet - Jihad GHANNOUM - Rim Sliti - IATIC 5
# Projet SOA : Traitement et analyse de données de réseaux sociaux à base de Services Web

import json
import logging
import pymongo
import sys
from spyne import Application, rpc, ServiceBase, Integer
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication
from spyne.util.wsgi_wrapper import run_twisted
from textblob import TextBlob
logging.basicConfig(level=logging.DEBUG)


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
            for entity in tweet_dict['context_annotations']:
                topics_list.append(entity.get('entity').get('name'))

        return str(topics_list)


application5 = Application([TopicIdentifierService],
                           tns='spyne.examples.TopicIdentifier',
                           in_protocol=Soap11(validator='lxml'),
                           out_protocol=Soap11()
                           )


class TreatmentServices(ServiceBase):
    @rpc(str, _returns=str)
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

        return str(result)


application6 = Application([TreatmentServices],
                           tns='spyne.examples.treatmentServices',
                           in_protocol=Soap11(validator='lxml'),
                           out_protocol=Soap11()
                           )


class TopKHashtagsService(ServiceBase):
    @rpc(Integer, _returns=str)
    def get_top_k_hashtags(ctx, k):
        hashtags = list()
        tags = list()
        top_k_hashtags = list()
        mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongodb_client["td2_services_web"]
        collection = db["treatment_results"]
        result = list(collection.aggregate([
            {"$group": {"_id": "$hashtags", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]))

        for i in range(len(result)):
            hashtags.append(str(result[i]["_id"]).replace("['", "").replace("']", "").replace("'", "").replace(",", ""))
            x = hashtags[i].split()
            tags += x

        tags_list = [x for x in tags if x != '[]']

        if k <= len(tags_list):
            for i in range(k):
                top_k_hashtags.append(max(set(tags_list), key=tags_list.count))
                tags_list = list(filter((top_k_hashtags[i]).__ne__, tags_list))

        else:
            return "K is greater than the length of the hashtags list"

        return str(top_k_hashtags)


application7 = Application([TopKHashtagsService],
                           tns='spyne.examples.topKHashtags',
                           in_protocol=Soap11(validator='lxml'),
                           out_protocol=Soap11()
                           )


class TopKUsersService(ServiceBase):
    @rpc(Integer, _returns=str)
    def get_top_k_users(ctx, k):
        top_k_users = list()
        mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongodb_client["td2_services_web"]
        collection = db["treatment_results"]

        result = list(collection.aggregate([
            {"$group": {"_id": "$author_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]))

        if k <= len(result):
            for i in range(k):
                top_k_users.append(result[i]["_id"])
        else:
            return "K is greater than the length of the users list"

        return str(top_k_users)


application8 = Application([TopKUsersService],
                           tns='spyne.examples.topKUsers',
                           in_protocol=Soap11(validator='lxml'),
                           out_protocol=Soap11()
                           )


class TopKTopicsService(ServiceBase):
    @rpc(Integer, _returns=str)
    def get_top_k_topics(ctx, k):
        topics = list()
        topics_list = list()
        top_k_topics = list()
        mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongodb_client["td2_services_web"]
        collection = db["treatment_results"]
        result = list(collection.aggregate([
            {"$group": {"_id": "$topics", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]))

        for i in range(len(result)):
            topics.append(str(result[i]["_id"]).replace("['", "").replace("']", "").replace("'", "").replace(",", ""))
            x = topics[i].split()
            topics_list += x

        list_of_topics = [x for x in topics_list if x != '[]']

        if k <= len(list_of_topics):
            for i in range(k):
                top_k_topics.append(max(set(list_of_topics), key=list_of_topics.count))
                list_of_topics = list(filter((top_k_topics[i]).__ne__, list_of_topics))

        else:
            return "K is greater than the length of the topics list"

        return str(top_k_topics)


application9 = Application([TopKTopicsService],
                           tns='spyne.examples.topKTopics',
                           in_protocol=Soap11(validator='lxml'),
                           out_protocol=Soap11()
                           )


class PostNumberByUserService(ServiceBase):
    @rpc(str, _returns=str)
    def get_post_number_by_user(ctx, userId):
        mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongodb_client["td2_services_web"]
        collection = db["treatment_results"]
        result = list(collection.aggregate([{"$match": {"author_id": userId}}]))
        return str(len(result))


application10 = Application([PostNumberByUserService],
                            tns='spyne.examples.PostNumberByUser',
                            in_protocol=Soap11(validator='lxml'),
                            out_protocol=Soap11()
                            )


class PostNumberByHashtagService(ServiceBase):
    @rpc(str, _returns=str)
    def get_post_number_by_hashtag(ctx, hashtag):
        hashtags = list()
        tags = list()
        cpt = 0
        mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongodb_client["td2_services_web"]
        collection = db["treatment_results"]
        result = collection.distinct("hashtags")

        for i in result:
            x = json.loads(i.replace("'", '"'))
            hashtags = list(dict.fromkeys(list(x)))
            if hashtag in hashtags:
                cpt += 1

        return str(cpt)


application11 = Application([PostNumberByHashtagService],
                            tns='spyne.examples.PostNumberByHashtag',
                            in_protocol=Soap11(validator='lxml'),
                            out_protocol=Soap11()
                            )


class PostNumberByTopicService(ServiceBase):
    @rpc(str, _returns=str)
    def get_post_number_by_topic(ctx, topic):
        topics = list()
        cpt = 0
        mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongodb_client["td2_services_web"]
        collection = db["treatment_results"]
        cursor = list(collection.distinct("topics"))

        for i in cursor:
            x = json.loads(i.replace("'", '"'))
            topics = list(dict.fromkeys(list(x)))
            if topic in topics:
                cpt += 1

        return str(cpt)


application12 = Application([PostNumberByTopicService],
                            tns='spyne.examples.PostNumberByTopic',
                            in_protocol=Soap11(validator='lxml'),
                            out_protocol=Soap11()
                            )


class AnalysisServices(ServiceBase):
    @rpc(Integer, str, str, str, _returns=str)
    def analyze_services(ctx, k, author_id, hashtag, topic):
        top_k_hashtags = TopKHashtagsService.get_top_k_hashtags(ctx, k)
        top_k_users = TopKUsersService.get_top_k_users(ctx, k)
        top_k_topics = TopKTopicsService.get_top_k_topics(ctx, k)
        post_number_by_user = PostNumberByUserService.get_post_number_by_user(ctx, author_id)
        post_number_by_hashtag = PostNumberByHashtagService.get_post_number_by_hashtag(ctx, hashtag)
        post_number_by_topic = PostNumberByTopicService.get_post_number_by_topic(ctx, topic)
        return "The top " + str(k) + " users : " + top_k_users + "\n" + \
               "The top " + str(k) + " hashtags : " + top_k_hashtags + "\n" + \
               "The top " + str(k) + " topics : " + top_k_topics + "\n" + \
               "The number of post by user " + "\"" + author_id + "\"" + " : " + post_number_by_user + "\n" \
               "The number of post by hashtag " + "\"" + hashtag + "\"" + " : " + post_number_by_hashtag + "\n" + \
               "The number of post by topic " + "\"" + topic + "\"" + " : " + post_number_by_topic + "\n"


application13 = Application([AnalysisServices],
                            tns='spyne.examples.analysisServices',
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
    wsgi_app7 = WsgiApplication(application7)
    wsgi_app8 = WsgiApplication(application8)
    wsgi_app9 = WsgiApplication(application9)
    wsgi_app10 = WsgiApplication(application10)
    wsgi_app11 = WsgiApplication(application11)
    wsgi_app12 = WsgiApplication(application12)
    wsgi_app13 = WsgiApplication(application13)
    twisted_apps = [
        (wsgi_app1, b'DbHandlerService'),
        (wsgi_app2, b'AuthorIdentifierService'),
        (wsgi_app3, b'HashtagExtractorService'),
        (wsgi_app4, b'SentimentAnalyzerService'),
        (wsgi_app5, b'TopicIdentifierService'),
        (wsgi_app6, b'TreatmentServices'),
        (wsgi_app7, b'TopKHashtagsService'),
        (wsgi_app8, b'TopKUsersService'),
        (wsgi_app9, b'TopKTopicsService'),
        (wsgi_app10, b'PostNumberByUserService'),
        (wsgi_app11, b'PostNumberByHashtagService'),
        (wsgi_app12, b'PostNumberByTopicService'),
        (wsgi_app13, b'AnalysisServices'),
    ]
    sys.exit(run_twisted(twisted_apps, 8000))
