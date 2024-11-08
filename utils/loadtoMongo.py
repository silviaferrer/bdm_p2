from pymongo import MongoClient
import tempfile
import os
from bson.binary import Binary

class MongoDBLoader:
    def __init__(self, vm_host, mongodb_port, database_name, mongo_cluster, logger):
        self.vm_host = vm_host
        self.mongodb_port = int(mongodb_port)
        self.database_name = database_name
        self.logger = logger
        self.mongo_uri = mongo_cluster
        self.client = MongoClient(self.mongo_uri, self.mongodb_port)
        self.db = self.client[self.database_name]
        

    def create_collection(self, collection_name):
        try:
            if collection_name not in self.db.list_collection_names():
                self.db.create_collection(collection_name)
                self.logger.info(f"Collection '{collection_name}' created in database '{self.database_name}'")
            else:
                self.logger.info(f"Collection '{collection_name}' already exists in database '{self.database_name}'")
        except Exception as e:
            self.logger.error(f"Failed to create collection '{collection_name}' in database '{self.database_name}': {e}")

    def drop_collection(self, collection_name):
        try:
            self.db[collection_name].drop()
            self.logger.info(f"Collection '{collection_name}' dropped from database '{self.database_name}'")
        except Exception as e:
            self.logger.error(f"Failed to drop collection '{collection_name}' from database '{self.database_name}': {e}")

    def read_collection(self, spark, collection_name):
        try:
            uri = f"mongodb://{self.vm_host}:{self.mongodb_port}/{self.database_name}.{collection_name}"
            df = spark.read.format("mongo").option('uri', uri).option('encoding', 'utf-8-sig').load()
            return df
        except Exception as e:
            self.logger.error(f"Failed to read collection '{collection_name}' from database '{self.database_name}': {e}")

    def write_to_collection(self, collection_name, dataframe, append=True):
        try:
            # uri = f"mongodb+srv://airdac:1234@cluster0.brrlvo1.mongodb.net/{self.database_name}?retryWrites=true&w=majority&appName=Cluster0"
            if not append:
                self.drop_collection(collection_name)
                self.create_collection(collection_name)
            dataframe.write.format("mongo").option("uri", self.mongo_uri).option('collection', collection_name).option("encoding", "utf-8-sig").mode("append").save()
            self.logger.info(f"Data written to collection '{collection_name}' in database '{self.database_name}'")
        except Exception as e:
            self.logger.error(f"Failed to write to collection '{collection_name}' in database '{self.database_name}': {e}")

    def save_model_to_collection(self, model, collection_name):
        try:
            # Define the MongoDB URI
            # uri = f"mongodb+srv://airdac:1234@cluster0.brrlvo1.mongodb.net/{self.database_name}?retryWrites=true&w=majority&appName=Cluster0"
            
            # Use a temporary directory to save the serialized model
            with tempfile.TemporaryDirectory() as temp_dir:
                model_path = os.path.join(temp_dir, "pyspark_model")
                model.save(model_path)
                
                # Read the model files into a byte array
                model_bytes = bytearray()
                for root, _, files in os.walk(model_path):
                    for file in files:
                        with open(os.path.join(root, file), 'rb') as f:
                            model_bytes.extend(f.read())
                
                # Connect to MongoDB
                # client = MongoClient(self.mongo_uri)
                # db = self.client[self.database_name]
                collection = self.db[collection_name]
                
                # Insert the model byte array into MongoDB
                model_binary = Binary(model_bytes)
                model_document = {'model': model_binary}
                collection.insert_one(model_document)
                
                print(f"Model saved to MongoDB collection '{collection_name}' in database '{self.database_name}'.")
        except Exception as e:
            print(f"Failed to save model to collection '{collection_name}' in database '{self.database_name}': {e}")