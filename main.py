from pyspark.sql import SparkSession
import argparse
import logging
from logging.handlers import RotatingFileHandler

from utils.loadtoMongo import MongoDBLoader
from utils.dataFormatter import DataFormatter
from utils.predictiveAnalysis import PredictiveAnalysis

VM_HOST = '10.192.36.59'
MONGODB_PORT = '27017'
DB_NAME = 'test'

# import os
# os.environ['PYSPARK_PYTHON'] = 'C:/Users/Silvia/AppData/Local/Programs/Python/Python312/python.exe'

# Create logger
logger = logging.getLogger('data_management')
logger.setLevel(logging.DEBUG)

# Create rotating file handler
handler = RotatingFileHandler('main.log', maxBytes=5*1024*1024, backupCount=5)
handler.setLevel(logging.DEBUG)
# This configuration ensures that logs are rotated when they reach 5 MB, with up to 5 backup files retained. 
# The logs include timestamps, logger names, log levels, and messages, providing a comprehensive logging solution.

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add handler to logger
logger.addHandler(handler)

def main():
    parser = argparse.ArgumentParser(description='Data Management')

    parser.add_argument('exec_mode', type=str, choices=['data-formatting', 'data-visualization', 'data-prediction', 'data-streaming'], help='Execution mode')

    args = parser.parse_args()
    exec_mode = args.exec_mode

    # Create spark session
    try:
        spark = SparkSession \
            .builder \
            .appName("myApp") \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .config("spark.mongodb.input.uri", f"mongodb+srv://airdac:1234@cluster0.brrlvo1.mongodb.net/{DB_NAME}?retryWrites=true&w=majority&appName=Cluster0") \
            .config("spark.mongodb.output.uri", f"mongodb+srv://airdac:1234@cluster0.brrlvo1.mongodb.net/{DB_NAME}?retryWrites=true&w=majority&appName=Cluster0") \
            .getOrCreate()
        logger.info("Spark connection is successful!")

    except Exception as e:
        logger.error("Building Spark Session error: ", e)

    mongoLoader = MongoDBLoader(VM_HOST, MONGODB_PORT, DB_NAME, logger)

    if exec_mode == 'data-formatting':

        try:
            logger.info('Starting data formatting process')

            dataFormatter = DataFormatter(spark, mongoLoader, logger)
            dataFormatter.main()

            logger.info('Finished succesfully data formatting process')

        except Exception as e:
            logger.error(f'Error occurred during data formatting: {e}')

    elif exec_mode == 'data-visualization':

        try:
            logger.info('Starting data visualization process')


            logger.info('Finished succesfully data visualization process')

        except Exception as e:
            logger.error(f'Error occurred during data visualization: {e}')

    elif exec_mode == 'data-prediction':

        try:
            logger.info('Starting data prediction process')

            predictiveAnalysis = PredictiveAnalysis(spark, mongoLoader, logger)
            predictiveAnalysis.main()

            logger.info('Finished succesfully data prediction process')

        except Exception as e:
            logger.error(f'Error occurred during data prediction: {e}')

    # 'data-streaming'


    spark.stop()

    return None


if __name__ == '__main__':
    main()
