import os

from pymongo import MongoClient


class MongoLoader():

    def __init__(self):

        mg_client = self.connectMongo()
        
        return None
    
    def connectMongo(self):

        client = MongoClient('mongodb://localhost:27017/')
        
        return client

    def load_idealista(self):

        return None
    
    def load_lookup(self):

        return None