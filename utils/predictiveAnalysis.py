import duckdb
import pymongo
import pandas as pd
import pickle
import warnings
import os

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

        #dfs = self.loadFromSpark(spark,mongoLoader.database_name,collections,mongoLoader)

        # Join all tables
        #df_joined = self.joinDf(dfs['idealista'], dfs['income'], 'district')

        #===============================
        # TEMP: store dfs in csv's
        #===============================
        predictive_output_path = os.path.join('data', 'output', 'predictive_analysis')
        '''for key, df in dfs.items():
            output_file_path = os.path.join(
                predictive_output_path, f'{key}.csv')
            df.toPandas().to_csv(output_file_path, index=False)'''
        

        df_income = spark.read.csv(os.path.join(predictive_output_path,'income.csv'), header=True, inferSchema=True)
        df_idealista = spark.read.csv(os.path.join(predictive_output_path,'idealista.csv'), header=True, inferSchema=True)
        df_airqual = spark.read.csv(os.path.join(predictive_output_path,'airqual.csv'), header=True, inferSchema=True)

        # Join the dataframes in a single dataframe
        df_joined = self.joinDf(df_income,df_idealista,'district')
        df_joined = self.joinDf(df_joined,df_airqual,'district')

        # Output the schema to check if the join was correct
        df_joined.printSchema()

        exclude_cols = ["_id", "priceByArea", "price"]
        feature_list = [col for col in df_joined.columns if col not in exclude_cols]
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
    
        # Check if join_col exists in both DataFrames
        if join_col not in df_left.columns or join_col not in df_right.columns:
            raise ValueError(f"Column '{join_col}' must be present in both DataFrames.")
        
        # Rename join column in right DataFrame to avoid clashes
        df_right_renamed = df_right.withColumnRenamed(join_col, join_col + "_right")
        
        # Perform the join
        joined_df = df_left.join(df_right_renamed, df_left[join_col] == df_right_renamed[join_col + "_right"], 'inner')
        
        # Drop the duplicate join column
        joined_df = joined_df.drop(df_right_renamed[join_col + "_right"])

        # Output the number of columns in the DataFrame
        num_columns = len(joined_df.columns)
        print(f"The number of columns in the joined DataFrame: {num_columns}")

        return joined_df
    
    def selectFeatures(self, spark, df, feature_list):
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
        # Identify the label column and feature columns
        label_col = "price"
        feature_cols = [col for col in df.columns if col != label_col]
        
        # Handle string columns by indexing them
        indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_indexed").fit(df) for col in feature_cols if dict(df.dtypes)[col] == 'string']
        indexed_cols = [f"{col}_indexed" if dict(df.dtypes)[col] == 'string' else col for col in feature_cols]

        # Assemble feature columns into a single vector column
        assembler = VectorAssembler(inputCols=indexed_cols, outputCol="features")
        
        # Create a pipeline with indexers and the assembler
        pipeline = Pipeline(stages=indexers + [assembler])
        preprocessed_df = pipeline.fit(df).transform(df)
        data = preprocessed_df.select(col("features"), col(label_col).alias("label"))
        
        # Split the data into training and testing sets
        train_data, test_data = data.randomSplit([0.8, 0.2], seed=1234)
        
        # Create and train the linear regression model
        lr = LinearRegression(featuresCol="features", labelCol="label")
        lr_model = lr.fit(train_data)
        
        # Evaluate the model
        predictions = lr_model.transform(test_data)
        evaluator_rmse = RegressionEvaluator(predictionCol="prediction", labelCol="label", metricName="rmse")
        rmse = evaluator_rmse.evaluate(predictions)
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
        
        