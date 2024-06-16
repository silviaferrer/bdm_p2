from pyspark.sql import SparkSession
import argparse

from utils.loadtoMongo import MongoDBLoader
from utils.dataFormatter import DataFormatter

VM_HOST = '10.4.41.45'
MONGODB_PORT = '27017'
DB_NAME = 'test'

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
            .getOrCreate()
        print("Spark connection is successful!")

    except Exception as e:
        print("Error: ", e)

    if exec_mode == 'data-formatting':

        try:
            print('Starting data formatting process')

            dataFormatter = DataFormatter(spark)
            mongoLoader = MongoDBLoader(VM_HOST, MONGODB_PORT, DB_NAME)

            # Write each DataFrame to MongoDB
            # Ensure you have the final DataFrame in the dataFormatter
            if dataFormatter.dfs:
                for key, df in dataFormatter.dfs.items():
                    # Write to MongoDB
                    mongoLoader.write_to_collection(key, df, append=True)
                    print(f"Data written to collection '{key}' in database '{DB_NAME}'")
                
            else:
                print("No final DataFrame found in dataFormatter")

    

            print('Finished succesfully data formatting process')

        except Exception as e:
            print(f'Error occurred during data formatting: {e}')

    elif exec_mode == 'data-visualization':

        try:
            print('Starting data visualization process')


            print('Finished succesfully data visualization process')

        except Exception as e:
            print(f'Error occurred during data visualization: {e}')

    elif exec_mode == 'data-prediction':

        try:
            print('Starting data prediction process')


            print('Finished succesfully data prediction process')

        except Exception as e:
            print(f'Error occurred during data prediction: {e}')

    # 'data-streaming'


    spark.stop()

    return None


if __name__ == '__main__':
    main()
