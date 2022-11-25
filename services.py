#!/usr/bin/python3

# Authors : Jihad GHANNOUM - IATIC 5

import logging
import pymongo
logging.basicConfig( level=logging.DEBUG)
import sys
from spyne import Application, rpc, ServiceBase, Integer, Unicode
from spyne import Iterable
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication
from spyne.util.wsgi_wrapper import run_twisted


myclient = pymongo.MongoClient("mongodb://localhost:27017/")


dblist = myclient.list_database_names()
if "td2_services_web" in dblist:
  mydb = myclient["td2_services_web"]
else :
    print("Base de données n'existe pas")

mycol = mydb["Tweets"]
mydict = { "_id" : "222E3", "name": "John", "address": "Highway 37" }
x = mycol.insert_one(mydict)
print(x.inserted_id)
