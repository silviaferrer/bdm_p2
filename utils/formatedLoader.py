import os
from pyspark.sql import SparkSession

dir_path = os.getcwd()
data_path = os.path.join(dir_path,'data')
idealista_path = os.path.join(data_path,'idealista')
income_path = os.path.join(data_path,'income_opendata')
lookuptables_path = os.path.join(data_path,'lookup_tables')
airquality_path = os.path.join(data_path,'airquality_data')

class LoadtoFormatted:

    def __init__(self):

        # Create spark session
        spark = self.connectPySpark()
        
        # Load data to PySpark
        print('Loading airquality data to PySpark...')
        df_airqual = self.loadToSpark(spark, airquality_path)
        if df_airqual is not None: print('Airquality data loaded!')

        print('Loading income data to PySpark...')
        df_income = self.loadToSpark(spark, income_path)
        if df_income is not None: print('Income data loaded!')

        print('Loading lookup tables data to PySpark...')
        df_lookup = self.loadToSpark(spark, lookuptables_path)
        if df_lookup is not None: print('Lookup tables data loaded!')

        print('Loading idealista data to PySpark...')
        dfs_idealista = []
        folder_names = os.listdir(idealista_path)
        print(folder_names)
        for folder in folder_names:
            file_path = os.path.join(idealista_path, folder)
            if os.path.isdir(file_path):
                df = self.loadToSpark(spark, file_path)[0]
                if df is not None:
                    dfs_idealista.append(df)
        if dfs_idealista: print('Idealista data loaded!')

        spark.stop()

        return None
    
    
    def connectPySpark(self): 
        try:
            spark = SparkSession.builder.appName("P2FormatedZone").getOrCreate()
            print("Spark connection is successful!")
            return spark
        
        except Exception as e:
            print("Error: ", e)

    def loadToSpark(self, spark, path):

        file_names = os.listdir(path)

        dfs = []
        combined_df = None
        for file_name in file_names:
            file_path = os.path.join(path, file_name)
            if file_name.split('.')[-1] == 'json':
                df = spark.read.json(file_path)
            elif file_name.split('.')[-1] == 'csv':
                df = spark.read.csv(file_path)
            elif file_name.split('.')[-1] == 'parquet':
                df = spark.read.parquet(file_path)
            else:
                df = None
                print("Uningestible file format: ", file_name)
                
            if df is not None:
                dfs.append(df)
                '''if combined_df is None and df is not None:
                    combined_df = df
                else:
                    combined_df = combined_df.union(df)'''

        return dfs