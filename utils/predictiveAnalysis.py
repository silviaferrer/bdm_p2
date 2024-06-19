import duckdb
import pandas as pd
import os
import tempfile
from functools import reduce

from pyspark.sql.functions import col, year, unix_timestamp
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression

from utils.utils import loadFromSpark

class PredictiveAnalysis():

    def __init__(self, spark, mongoLoader, logger):

        self.db_con = self._connectDuckDB('BDM_P2_ExploitationZone')
        self.spark = spark
        self.mongoLoader = mongoLoader
        self.logger = logger
        
        self.collections = ['airqual','idealista','income']
    
    def _connectDuckDB(self, db_path):
        try:
            # Create a DuckDB connection
            con = duckdb.connect(db_path)
            return con
        except Exception as e:
            self.logger.error("Error: ", e)
    
    def _joinDf(self, df_left, df_right, join_col):
    
        # Check if join_col exists in both DataFrames
        if not set(join_col).issubset(set(df_left.columns).intersection(set(df_right.columns))):
            raise ValueError(f"Column '{join_col}' must be present in both DataFrames.")
        
        # Rename join column in right DataFrame to avoid clashes
        df_right_renamed = df_right
        for col in join_col:
            df_right_renamed = df_right_renamed.withColumnRenamed(col, col + "_right")

        # Create a list of column equality conditions
        conditions = [df_left[col] == df_right_renamed[col + "_right"]
                      for col in join_col]
        # Combine the conditions using the & operator
        join_condition = reduce(lambda a, b: a & b, conditions)
        
        # Perform the join
        joined_df = df_left.join(df_right_renamed, join_condition, 'inner')
        
        # Drop the duplicate join column
        for col in join_col:
            joined_df = joined_df.drop(df_right_renamed[col + "_right"])

        # Identify columns that are the same in both DataFrames (excluding the join column)
        common_columns = set(df_left.columns).intersection(set(df_right.columns)) - set(join_col)
            
        # Drop one of the common columns
        for col in common_columns:
            joined_df = joined_df.drop(df_right_renamed[col])

        # Output the number of columns in the DataFrame
        num_columns = len(joined_df.columns)
        self.logger.info(f"The number of columns in the joined DataFrame: {num_columns}")

        return joined_df
    
    def _selectFeatures(self, df, feature_list):
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
        
        # self.logger.info warning for missing features
        if missing_features:
            self.logger.warning(f"The following features are not present in the DataFrame: {', '.join(missing_features)}")
            # Alternatively, you can self.logger.info a message:
            # self.logger.info(f"Warning: The following features are not present in the DataFrame: {', '.join(missing_features)}")
        
        # Select only the valid features in the DataFrame
        selected_df = df.select(*[col(feature) for feature in valid_features])
        
        return selected_df
    
    def _buildModel(self, df):

        if df.isEmpty():
            self.logger.info("Data is empty!")

        # Identify the label column and feature columns
        label_col = "price"
        feature_cols = [col for col in df.columns if col != label_col]
        
        # Handle string columns by indexing them
        self.logger.info("Indexing categorical features...")
        indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_indexed").fit(df) for col in feature_cols if dict(df.dtypes)[col] == 'string']
        indexed_cols = [f"{col}_indexed" if dict(df.dtypes)[col] == 'string' else col for col in feature_cols]

        self.logger.info("Assembeling vectors...")
        # Assemble feature columns into a single vector column
        assembler = VectorAssembler(inputCols=indexed_cols, outputCol="features")
        
        self.logger.info("Creating pipeline...")
        # Create a pipeline with indexers and the assembler
        pipeline = Pipeline(stages=indexers + [assembler])
        # Change date type for a type supported by the pipeline
        df = df.withColumn('date', unix_timestamp(df['date']))
        preprocessed_df = pipeline.fit(df).transform(df)
        data = preprocessed_df.select("features", df[label_col].alias("label"))

        
        # Split the data into training and testing sets
        train_data, test_data = data.randomSplit([0.8, 0.2], seed=1234)

        # Check if the split yielded non-empty datasets
        if train_data.isEmpty() or test_data.isEmpty():
            raise ValueError("The split yielded an empty training or testing set. Please check your data or try a different seed.")

        self.logger.info("Fitting model...")
        # Create and train the linear regression model
        lr = LinearRegression(featuresCol="features", labelCol="label")
        lr_model = lr.fit(train_data)
        
        # Evaluate the model
        predictions = lr_model.transform(test_data)
        evaluator_rmse = RegressionEvaluator(predictionCol="prediction", labelCol="label", metricName="rmse")
        rmse = evaluator_rmse.evaluate(predictions)
        self.logger.info(f"Root Mean Squared Error (RMSE): {rmse}")
        
        evaluator_r2 = RegressionEvaluator(predictionCol="prediction", labelCol="label", metricName="r2")
        r2 = evaluator_r2.evaluate(predictions)
        self.logger.info(f"R2: {r2}")
        
        return lr_model

    # Function to serialize and save the model to DuckDB
    def _saveModel(self, model, table_name):
        # Use a temporary directory to save the serialized model
        with tempfile.TemporaryDirectory() as temp_dir:
            model_path = os.path.join(temp_dir, "lr_model")
            model.save(model_path)
            
            # Read the model file(s) into a byte array
            model_bytes = bytearray()
            for root, _, files in os.walk(model_path):
                for file in files:
                    with open(os.path.join(root, file), 'rb') as f:
                        model_bytes.extend(f.read())
            
            # Create a DataFrame with the serialized model
            df = pd.DataFrame({'model': [model_bytes]})
            
            # Save the DataFrame to a DuckDB table
            self.db_con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (model BLOB)")
            self.db_con.execute(f"INSERT INTO {table_name} VALUES (?)", [model_bytes])
            
            # Verify the model was saved
            result_df = self.db_con.execute(f"SELECT * FROM {table_name}").fetchdf()
            self.logger.info("Model saved to DuckDB:")
            self.logger.info(result_df.head())
        
    def _cleanDF(self, df):

        # Identify columns with any null values
        cols_to_drop = [col for col in df.columns if df.filter(df[col].isNull()).count() > 0]

        # self.logger.info the columns to be dropped
        self.logger.info(f"Columns with NA values to be dropped: {cols_to_drop}")
        
        # Drop the identified columns
        cleaned_df = df.drop(*cols_to_drop)
        
        return cleaned_df
        
    def main(self):
        dfs = loadFromSpark(self.spark, self.mongoLoader
                            , self.logger, self.collections)

        # Join all tables
        #df_joined = self._joinDf(dfs['idealista'], dfs['income'], 'district')

        #===============================
        # TEMP: store dfs in csv's
        #===============================
        '''predictive_output_path = os.path.join('data', 'output', 'predictive_analysis')
        for key, df in dfs.items():
            output_file_path = os.path.join(
                predictive_output_path, f'{key}.csv')
            df.toPandas().to_csv(output_file_path, index=False)'''
        

        '''df_income = spark.read.csv(os.path.join(predictive_output_path,'income.csv'), header=True, inferSchema=True)
        df_idealista = spark.read.csv(os.path.join(predictive_output_path,'idealista.csv'), header=True, inferSchema=True)
        df_airqual = spark.read.csv(os.path.join(predictive_output_path,'airqual.csv'), header=True, inferSchema=True)'''

        self.logger.info("Joining tables...")
        # Join the dataframes in a single dataframe
        df_idealista = dfs['idealista']
        dfs['idealista'] = df_idealista.withColumn('year', year(df_idealista['date']))

        df_joined = self._joinDf(dfs['airqual'], dfs['idealista'], ['neighborhood', 'year'])
        df_joined = self._joinDf(df_joined, dfs['income'], ['neighborhood'])

        num_rows = df_joined.count()
        num_cols = len(df_joined.columns)
        self.logger.info(f"Shape of DataFrame before indexing: ({num_rows}, {num_cols})")

        self.logger.info("Tables joined!")
        # Output the schema to check if the join was correct
        #df_joined.self.logger.infoSchema()

        self.logger.info("Selecting features...")
        exclude_cols = ["_id", "priceByArea"]
        feature_list = [col for col in df_joined.columns if col not in exclude_cols]
        df_joined = self._selectFeatures(df_joined, feature_list)
        self.logger.info("Features selected!")

        #df_joined.self.logger.infoSchema()
        # Output the shape of the DataFrame before indexing
        '''num_rows = df_joined.count()
        num_cols = len(df_joined.columns)
        self.logger.info(f"Shape of DataFrame before indexing: ({num_rows}, {num_cols})")'''

        self.logger.info("Cleaning data...")
        df_joined = self._cleanDF(df_joined)
        num_rows = df_joined.count()
        num_cols = len(df_joined.columns)
        self.logger.info(f"Shape of DataFrame before indexing: ({num_rows}, {num_cols})")
        self.logger.info("Data cleaned!")

        #df_joined.self.logger.infoSchema()

        self.logger.info("Building model...")
        model = self._buildModel(df_joined)

        self.logger.info("Saving model...")
        # Save model to DuckDB
        self._saveModel(model, 'Model1')

        # Save df and model to Exploitation Zone in MongoDB
        self.logger.info("Saving model to MongoDB...")
        self.mongoLoader.write_to_collection('ExploitationZone', df_joined)
        self.mongoLoader.save_model_to_collection(model, 'ExploitationZone')

        # Close the DuckDB connection
        self.db_con.close()

        return None