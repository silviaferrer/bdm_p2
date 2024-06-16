from pyspark.sql import SparkSession

from utils.loadtoMongo import MongoDBLoader
from utils.dataFormatter import DataFormatter


def main():
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

    dataFormatter = DataFormatter(spark)

    vm_host = '10.4.41.45'
    mongodb_port = '27017'
    db_name = 'test'

    mongoLoader = MongoDBLoader(vm_host, mongodb_port, db_name)
    

    # Write each DataFrame to MongoDB
    try:
        # Ensure you have the final DataFrame in the dataFormatter
        if dataFormatter.dfs:
            for key, df in dataFormatter.dfs.items():
                # Write to MongoDB
                mongoLoader.write_to_collection(key, df, append=True)
                print(f"Data written to collection '{key}' in database '{db_name}'")
            
        else:
            print("No final DataFrame found in dataFormatter")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        spark.stop()

    return None


if __name__ == '__main__':
    main()
