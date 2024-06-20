from pyspark.sql.types import StringType
import os
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf

from utils.formattedLoader import LoadtoFormatted
from utils.utils import *

dir_path = os.getcwd()
data_path = os.path.join(dir_path, 'data')
out_path = os.path.join(data_path, 'output')
idealista_path = os.path.join(data_path, 'idealista')
income_path = os.path.join(data_path, 'income_opendata')
lookuptables_path = os.path.join(data_path, 'lookup_tables')
airquality_path = os.path.join(data_path, 'airquality_data')


class DataFormatter:
    def __init__(self, spark, mongoLoader, logger):
        self.dfs = {}
        self.spark = spark
        self.mongoLoader = mongoLoader
        self.logger = logger

    def _reconciliate_data(self):
        self.logger.info('Starting reconciliate process...')
        merged_dfs = {}
        # Process df_income
        df_income_list = list(self.dfs.get('income').values())
        if df_income_list is not None and isinstance(df_income_list, list):
            # Drop not useful columns
            df_income_list = [df.drop('_id') for df in df_income_list]

            # Reconcile district
            df_lookup = self.dfs['lookup']['income_lookup_district.json'].select('district', 'district_reconciled')
            df_income_merged = join_and_union(df_income_list, df_lookup, 'district_name', 'district')
            # Keep only reconciled column
            df_income_merged = df_income_merged.drop('district', 'district_name', 'district_id').withColumnRenamed('district_reconciled', 'district')

            # Reconcile neigborhood
            df_lookup = self.dfs['lookup']['income_lookup_neighborhood.json'].select(
                'neighborhood', 'neighborhood_reconciled')
            df_income_merged = join_and_union([df_income_merged], df_lookup, 'neigh_name ', 'neighborhood')
            df_income_merged = df_income_merged.drop('neigh_name ', 'neighborhood').withColumnRenamed('neighborhood_reconciled', 'neighborhood')

            # Explode info column into year, pop, RFD
            df_income_merged = df_income_merged.withColumn('RFD', 
                col('info')[0]['RFD']).withColumn('pop', col('info')[0]['pop']).withColumn('year', col('info')[0]['year']).drop('info')

            self.logger.info(f"Income after merge columns: {df_income_merged.columns}")
            merged_dfs['income'] = df_income_merged

        # Process df_idealista
        df_idealista_list = list(self.dfs.get('idealista').values())
        if df_idealista_list is not None and isinstance(df_idealista_list, list):
            # Drop not useful columns
            columns_to_drop = ['country', 'municipality', 'province', 'url', 'thumbnail'] #operation
            df_idealista_list = [df.drop(*columns_to_drop) for df in df_idealista_list]

            # Explode columns parkingSpace, detailedType, suggestedTexts
            df_idealista_list = explode_column(df_idealista_list, 'parkingSpace')
            df_idealista_list = explode_column(df_idealista_list, 'detailedType')
            df_idealista_list = explode_column(df_idealista_list, 'suggestedTexts')

            # Reconcile district
            df_lookup = self.dfs['lookup']['rent_lookup_district.json'].select('di', 'di_re')
            df_idealista_merged = join_and_union(df_idealista_list, df_lookup, 'district', 'di')
            df_idealista_merged = df_idealista_merged.drop('di', 'district').withColumnRenamed('di_re', 'district')

            # Reconcile neighborhood
            df_lookup = self.dfs['lookup']['rent_lookup_neighborhood.json'].select('ne', 'ne_re')
            df_idealista_merged = join_and_union([df_idealista_merged], df_lookup, 'neighborhood', 'ne')
            df_idealista_merged = df_idealista_merged.drop('neighborhood', 'ne').withColumnRenamed('ne_re', 'neighborhood')

            self.logger.info(f"Idealista after merge columns: {df_idealista_merged.columns}")
            merged_dfs['idealista'] = df_idealista_merged

        # Process df_airqual
        df_airqual_list = list(self.dfs.get('airqual').values())
        if df_airqual_list is not None and isinstance(df_airqual_list, list):
            # Drop not useful columns
            df_airqual_list = [df.drop('_id') for df in df_airqual_list]

            # Transform district and neighborhood names to join them with the
            # lowercase names in the lookup tables            
            lowercase_lookup_udf = udf(lowercase_lookup, StringType())

            # Apply the UDF to each row of the 'Nom_districte' column
            df_airqual_list = [df.withColumn('Nom_districte', lowercase_lookup_udf(df['Nom_districte'])).withColumn('Nom_barri', lowercase_lookup_udf(df['Nom_barri'])) for df in df_airqual_list]

            # df_lookup for df_airqual
            df_lookup = self.dfs['lookup']['income_lookup_district.json'].select('district_name', 'district_reconciled')
            df_airqual_merged = join_and_union(
                df_airqual_list, df_lookup, 'Nom_districte', 'district_name')
            df_airqual_merged = df_airqual_merged.drop(
                'district_name', 'Nom_districte', 'Codi_districte').withColumnRenamed('district_reconciled', 'district')

            # Reconcile neigborhood
            df_lookup = self.dfs['lookup']['income_lookup_neighborhood.json'].select(
                'neighborhood_name', 'neighborhood_reconciled')
            df_airqual_merged = join_and_union(
                [df_airqual_merged], df_lookup, 'Nom_barri', 'neighborhood_name')
            df_airqual_merged = df_airqual_merged.drop('Nom_barri', 'neighborhood_name', 'Codi_barri').withColumnRenamed(
                'neighborhood_reconciled', 'neighborhood')

            self.logger.info(f"Airqual after merge columns: {df_airqual_merged.columns}")
            merged_dfs['airqual'] = df_airqual_merged

        self.logger.info("Data reconciliated")

        return merged_dfs

    def _clean_data(self, df):
        self.logger.info("Starting cleaning data...")

        # Remove duplicates
        df = df.dropDuplicates()

        # Find columns with wrong values (e.g., negative height)
        if 'height' in df.columns:
            df = df.filter(df['height'] >= 0)

        # Transform wrong columns (if necessary)
        if 'age' in df.columns:
            df = df.withColumn('age', F.when(df['age'] < 0, None).otherwise(df['age']))

        self.logger.info("Data cleaned!")

        return df

    def _load_to_mongo(self):
        self.logger.info("Starting to load formatting data to MongoDB...")
        try:
            # Ensure you have the final DataFrame in the dataFormatter
            if self.dfs:
                for key, df in self.dfs.items():
                    # Write to MongoDB
                    self.mongoLoader.write_to_collection(key, df, append=False)
                    self.logger.info(f"Data written to collection '{key}'")
                
            else:
                self.logger.info("No final DataFrame found in dataFormatter")
        except Exception:
            self.logger.error("Error loading formatting data to MongoDB", exc_info=True)

    def main(self):
        try:
            formattedLoader = LoadtoFormatted(self.spark, self.logger)
            self.dfs = formattedLoader.main()
            # Reconciliate data, unification column names
            merged_dfs = self._reconciliate_data()

            # Clean data
            self.dfs = {key: self._clean_data(df) for key, df in merged_dfs.items()}

            # Write each DataFrame to MongoDB
            self._load_to_mongo()  
            self.logger.info("Finished Formatting Data and loading it with success!")

        except Exception:
            self.logger.error("Error formatting data", exc_info=True)