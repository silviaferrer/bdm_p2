import os

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
        try:
            # Load data to PySpark
            logger.info('Loading airquality data to PySpark...')
            df_airqual = self.loadToSpark(airquality_path)
            if df_airqual is not None:
                logger.info('Airquality data loaded!')

            logger.info('Loading income data to PySpark...')
            df_income = self.loadToSpark(income_path)
            if df_income is not None:
                logger.info('Income data loaded!')

            logger.info('Loading lookup tables data to PySpark...')
            df_lookup = self.loadToSpark(lookuptables_path)
            if df_lookup is not None:
                logger.info('Lookup tables data loaded!')

            logger.info('Loading idealista data to PySpark...')
            dfs_idealista = {}
            folder_names = os.listdir(idealista_path)
            for folder in folder_names:
                file_path = os.path.join(idealista_path, folder)
                if os.path.isdir(file_path):
                    df = next(iter(self.loadToSpark(file_path).values()))
                    if df is not None:
                        dfs_idealista[folder] = df
            if dfs_idealista:
                logger.info('Idealista data loaded!')

            self.dfs = {'airqual': df_airqual, 'income': df_income,
                        'lookup': df_lookup, 'idealista': dfs_idealista}
            
            logger.info('Finished loading data to PySpark with success!')
            
        except Exception:
            self.logger.error("Error loading data to PySpark", exc_info=True)
        

        return None

    def loadToSpark(self, path):

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
                # logger.info("Uningestible file format: ", file_name)

            if df is not None:
                dfs[file_name] = df

        return dfs
