from pyspark.sql import SparkSession


class DataFormatter():

    def __init__(self, dfs):

        # Test with single df
        df = dfs
        df.show(5, 0)
        print('Columns:', df.columns)
        print('N_rows:', df.count())
        df = df.dropDuplicates()
        print('N_rows after dropping duplicates:', df.count())

        ########## Remove duplicates ############
        # Simply use .dropDuplicates()
        ##########################################################

        ########## Append, join and reconciliate data ############
        # Reconciliate = join using the lookup tables
        ##########################################################

        ########## Clean data ##########
        '''General cleaning steps:
            0. Remove columns with > 30% null values
            1. Remove rows with null values
            2. Find columns with wrong values (like negative height)
            3. Transform wrong columns
            4. Remove rows with wrong data that cannot be transformed
        '''
        ################################

        return None


if __name__ == '__main__':

    # Initialize Spark connection
    try:
        spark = SparkSession.builder.appName(
            "P2FormatedZone").getOrCreate()
        print("Spark connection is successful!")
    except Exception as e:
        print("Error: ", e)

    # Read example csv file
    import os
    dir_path = os.getcwd()
    data_path = os.path.join(dir_path, 'data')
    folder_path = os.path.join(data_path, 'airquality_data')
    file_path = os.path.join(folder_path, '2019_qualitat_aire_estacions.csv')

    df = spark.read.option('header', True).csv(file_path)
    DataFormatter(df)

    spark.stop()
