from pymongo import MongoClient

class MongoDBLoader:
    def __init__(self, vm_host, mongodb_port, database_name):
        self.vm_host = vm_host
        self.mongodb_port = int(mongodb_port)
        self.database_name = database_name
        #self.client = MongoClient(self.vm_host, self.mongodb_port)
        self.client = MongoClient(
            "mongodb+srv://airdac:1234@cluster0.brrlvo1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0", self.mongodb_port)
        self.db = self.client[self.database_name]

    def create_collection(self, collection_name):
        try:
            if collection_name not in self.db.list_collection_names():
                self.db.create_collection(collection_name)
                print(f"Collection '{collection_name}' created in database '{self.database_name}'")
            else:
                print(f"Collection '{collection_name}' already exists in database '{self.database_name}'")
        except Exception as e:
            print(f"Failed to create collection '{collection_name}' in database '{self.database_name}': {e}")

    def drop_collection(self, collection_name):
        try:
            self.db[collection_name].drop()
            print(f"Collection '{collection_name}' dropped from database '{self.database_name}'")
        except Exception as e:
            print(f"Failed to drop collection '{collection_name}' from database '{self.database_name}': {e}")

    def read_collection(self, spark, collection_name):
        try:
            uri = f"mongodb://{self.vm_host}:{self.mongodb_port}/{self.database_name}.{collection_name}"
            df = spark.read.format("mongo").option('uri', uri).option('encoding', 'utf-8-sig').load()
            return df
        except Exception as e:
            print(f"Failed to read collection '{collection_name}' from database '{self.database_name}': {e}")

    def write_to_collection(self, collection_name, dataframe, append=True):
        try:
            #uri = f"mongodb://{self.vm_host}:{self.mongodb_port}/{self.database_name}.{collection_name}"
            uri = f"mongodb+srv://airdac:1234@cluster0.brrlvo1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0/{
                self.database_name}.{collection_name}"
            if not append:
                self.drop_collection(collection_name)
                self.create_collection(collection_name)
            dataframe.write.format("mongo").option("uri", uri).option("encoding", "utf-8-sig").mode("append").save()
            print(f"Data written to collection '{collection_name}' in database '{self.database_name}'")
        except Exception as e:
            print(f"Failed to write to collection '{collection_name}' in database '{self.database_name}': {e}")

    def initialize_database(self):
        # Create a dummy collection and insert a document to initialize the database
        try:
            dummy_collection_name = "dummy_collection"
            self.db[dummy_collection_name].insert_one({"initialization": "This is to initialize the database"})
            print(f"Database '{self.database_name}' initialized with dummy collection '{dummy_collection_name}'")
        except Exception as e:
            print(f"Failed to initialize database '{self.database_name}': {e}")
