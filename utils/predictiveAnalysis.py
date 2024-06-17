import duckdb as db
import mongod 
import pandas as pd
import pickle
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression


class PredictiveAnalysis():

    def __init__(self,spark):



    def loadFromSpark(self, spark, df_name):

        df = spark.read.format("mongo").load()
        return df
    
    def buildModel(self, spark, ):
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

    # Function to serialize and save the model to DuckDB
    def save_model_to_duckdb(model, db_path, table_name):
        # Serialize the model to a byte array
        model_bytes = pickle.dumps(model)
        
        # Create a DuckDB connection
        con = duckdb.connect(db_path)
        
        # Create a DataFrame with the serialized model
        df = pd.DataFrame({'model': [model_bytes]})
        
        # Save the DataFrame to a DuckDB table
        con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (model BLOB)")
        con.execute(f"INSERT INTO {table_name} VALUES (?)", [model_bytes])
        
        # Verify the model was saved
        result_df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
        print("Model saved to DuckDB:")
        print(result_df.head())
        
        # Close the DuckDB connection
        con.close()