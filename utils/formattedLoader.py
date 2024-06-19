import os
from pyspark.sql.functions import to_date, lit

dir_path = os.getcwd()
data_path = os.path.join(dir_path, 'data')
idealista_path = os.path.join(data_path, 'idealista')
income_path = os.path.join(data_path, 'income_opendata')
lookuptables_path = os.path.join(data_path, 'lookup_tables')
airquality_path = os.path.join(data_path, 'airquality_data')


class LoadtoFormatted:

    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger        

    def _loadToSpark(self, path):

        file_names = os.listdir(path)

        dfs = {}
        for file_name in file_names:
            file_path = os.path.join(path, file_name)
            if file_name.split('.')[-1] == 'json':
                df = self.spark.read.option('header', True).json(file_path)
            elif file_name.split('.')[-1] == 'csv':
                df = self.spark.read.option('header', True).csv(file_path)
            elif file_name.split('.')[-1] == 'parquet':
                df = self.spark.read.option('header', True).parquet(file_path)
            else:
                df = None
                # self.logger.info("Uningestible file format: ", file_name)

            if df is not None:
                dfs[file_name] = df
                
        return dfs

    def main(self):
        try:
            # Load data to PySpark
            self.logger.info('Loading airquality data to PySpark...')
            df_airqual = self._loadToSpark(airquality_path)
            df_airqual = {file_name: df.withColumn('year', lit(file_name[:4]))
              for file_name, df in df_airqual.items()}
            if df_airqual is not None:
                self.logger.info('Airquality data loaded!')

            self.logger.info('Loading income data to PySpark...')
            df_income = self._loadToSpark(income_path)
            if df_income is not None:
                self.logger.info('Income data loaded!')

            self.logger.info('Loading lookup tables data to PySpark...')
            df_lookup = self._loadToSpark(lookuptables_path)
            if df_lookup is not None:
                self.logger.info('Lookup tables data loaded!')

            self.logger.info('Loading idealista data to PySpark...')
            dfs_idealista = {}
            folder_names = os.listdir(idealista_path)
            for folder in folder_names:
                file_path = os.path.join(idealista_path, folder)
                if os.path.isdir(file_path):
                    df = next(iter(self._loadToSpark(file_path).values()))
                    if df is not None:
                        df = df.withColumn('date', to_date(lit(folder[:10]), 'yyyy_MM_dd'))
                        dfs_idealista[folder] = df

            if dfs_idealista:
                self.logger.info('Idealista data loaded!')

            self.dfs = {'airqual': df_airqual, 'income': df_income,
                        'lookup': df_lookup, 'idealista': dfs_idealista}
            
            self.logger.info('Finished loading data to PySpark with success!')

            return self.dfs
            
        except Exception:
            self.logger.error("Error loading data to PySpark", exc_info=True)
    