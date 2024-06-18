import duckdb
import pymongo
import pandas as pd
import pickle
import warnings

from utils.loadtoMongo import MongoDBLoader
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression

VM_HOST = '10.192.36.59'
MONGODB_PORT = '27017'
DB_NAME = 'test'

class PredictiveAnalysis():

    def __init__(self,spark):

        db_con = self.connectDuckDB('bdm_p2')

        mongoLoader = MongoDBLoader(VM_HOST, MONGODB_PORT, DB_NAME)
        
        collections = ['airqual','idealista','income']

        dfs = self.loadFromSpark(spark,mongoLoader.database_name,collections,mongoLoader)

        print(dfs)

        # Join all tables
        #df_joined = self.joinDf(spark, df_income)

        # Define feature list and select the features from the source
        feature_list = []
        df_joined = self.selectFeatures(spark, df_joined, feature_list)

        model = self.buildModel(spark, df_joined)

        # Save model to DuckDB
        self.saveModel(model, db_con, 'Model1')

        # Close the DuckDB connection
        db_con.close()

        return None
    
    def connectDuckDB(self, db_path):
        try:
            # Create a DuckDB connection
            con = duckdb.connect(db_path)
            return con
        except Exception as e:
            print("Error: ", e)
    
    def loadFromSpark(self, spark, db_name, collections, mongoLoader):

        uri = f"mongodb+srv://airdac:1234@cluster0.brrlvo1.mongodb.net/{db_name}?retryWrites=true&w=majority&appName=Cluster0"
        # Dictionary to hold the DataFrames
        dfs = {}

        # Load each collection into a DataFrame
        for collection in collections:
            print(f"Loading collection '{collection}' into DataFrame")
            #df = spark.read.format("mongo").option("uri", f"mongodb+srv://airdac:1234@cluster0.brrlvo1.mongodb.net/{db_name}.{collection}?retryWrites=true&w=majority&appName=Cluster0").load()
            df = spark.read.format("mongo").option("uri", uri).option('collection', collection).option("encoding", "utf-8-sig").load()
            #df = mongoLoader.read_collection(spark,collection)
            dfs[collection] = df
            print(f"Loaded collection '{collection}' into DataFrame")

        # Example: Show the schema and first few rows of each DataFrame
        for collection, df in dfs.items():
            print(f"Schema for collection '{collection}':")
            df.printSchema()
            print(f"First few rows of collection '{collection}':")
            df.show()

        return dfs
    
    def joinDf(self, df_left, df_right, join_col):
    
        # FIRST ensure join-col in both!!!

        joined_df = df_left.join(df_right, df_left[join_col] == df_right[join_col], 'inner')
        return joined_df
    
    def selectFeatures(spark, df, feature_list):
        # Get the current columns in the DataFrame
        current_columns = df.columns
        
        # Filter feature_list to only include features that exist in df's columns
        valid_features = []
        missing_features = []
        
        for feature in feature_list:
            if feature in current_columns:
                valid_features.append(feature)
            else:
                missing_features.append(feature)
        
        # Print warning for missing features
        if missing_features:
            warnings.warn(f"The following features are not present in the DataFrame: {', '.join(missing_features)}")
            # Alternatively, you can print a message:
            # print(f"Warning: The following features are not present in the DataFrame: {', '.join(missing_features)}")
        
        # Select only the valid features in the DataFrame
        selected_df = df.select(*[col(feature) for feature in valid_features])
        
        return selected_df
    
    def buildModel(self, spark, df):
        # Assume the data has the following columns: 'feature1', 'feature2', 'label'
        indexer = StringIndexer(inputCol="feature1", outputCol="feature1_indexed")
        assembler = VectorAssembler(inputCols=["feature1_indexed", "feature2"], outputCol="features")
        pipeline = Pipeline(stages=[indexer, assembler])
        preprocessed_df = pipeline.fit(df).transform(df)
        data = preprocessed_df.select(col("features"), col("label"))
        train_data, test_data = data.randomSplit([0.8, 0.2], seed=1234)

        # Step 4: Create a predictive model
        lr = LinearRegression(featuresCol="features", labelCol="label")
        lr_model = lr.fit(train_data)

        # Step 5: Evaluate the model
        predictions = lr_model.transform(test_data)
        evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="label", metricName="rmse")
        rmse = evaluator.evaluate(predictions)
        print(f"Root Mean Squared Error (RMSE): {rmse}")
        evaluator_r2 = RegressionEvaluator(predictionCol="prediction", labelCol="label", metricName="r2")
        r2 = evaluator_r2.evaluate(predictions)
        print(f"R2: {r2}")

        return lr_model

    # Function to serialize and save the model to DuckDB
    def saveModel(model, db_con, table_name):
        # Serialize the model to a byte array
        model_bytes = pickle.dumps(model)
        
        # Create a DataFrame with the serialized model
        df = pd.DataFrame({'model': [model_bytes]})
        
        # Save the DataFrame to a DuckDB table
        db_con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (model BLOB)")
        db_con.execute(f"INSERT INTO {table_name} VALUES (?)", [model_bytes])
        
        # Verify the model was saved
        result_df = db_con.execute(f"SELECT * FROM {table_name}").fetchdf()
        print("Model saved to DuckDB:")
        print(result_df.head())
        
        