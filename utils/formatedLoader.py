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

        sprkSession = self.connectPySpark()
        
        return None
    

    
    def connectPySpark(self): 

        spark = SparkSession.builder \
        .appName("PySpark Example") \
        .getOrCreate()
        
        return spark