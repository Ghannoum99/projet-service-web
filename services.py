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


class DbHandlerService(ServiceBase):
    @rpc(str)
    def save_tweet_db(ctx, tweet):
        tweet_dict = json.loads(tweet)
        mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongodb_client["td2_services_web"]
        collection = db["Tweets"]
        collection.insert_one(tweet_dict)


application = Application([DbHandlerService],
                          tns='spyne.examples.DbHandler',
                          in_protocol=Soap11(validator='lxml'),
                          out_protocol=Soap11()
                          )

if __name__ == '__main__':
    wsgi_app = WsgiApplication(application)
    twisted_apps = [
        (wsgi_app, b'DbHandlerService'),
    ]
    sys.exit(run_twisted(twisted_apps, 8000))
