from pyspark.sql.types import StringType
import os
import unicodedata
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StructType

from utils.formattedLoader import LoadtoFormatted

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
            df_idealista_merged = join_and_union(df_idealista_list, df_lookup, 'district', 'di', ensure_same_schema=True)
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

    def _join_dfs(self):
        self.logger.info('Starting final join...')
        dfs_dict = self.dfs
        # Merge df_income, df_airqual, and df_idealista into a single DataFrame
        final_df = dfs_dict['income'].join(dfs_dict['idealista'], ['district', 'neighborhood'], 'outer') \
                .join(dfs_dict['airqual'], ['district', 'neighborhood'], 'outer')

        # Store the final DataFrame in the dictionary
        self.dfs['final'] = final_df
        self.logger.info("Data joined!")

    def _clean_data(self, df):
        self.logger.info("Starting cleaning data...")

        # Step 0: Remove duplicates
        df = df.dropDuplicates()

        # # Step 1: Remove columns with > 30% null values
        # threshold = 0.3 * df.count()
        # for col in df.columns:
        #     if df.filter(df[col].isNull()).count() > threshold:
        #         df = df.drop(col)

        # Step 2: Remove rows with null values
        #df = df.dropna()

        # Step 3: Find columns with wrong values (e.g., negative height)
        if 'height' in df.columns:
            df = df.filter(df['height'] >= 0)

        # Step 4: Transform wrong columns (if necessary)
        if 'age' in df.columns:
            df = df.withColumn('age', F.when(df['age'] < 0, None).otherwise(df['age']))

        # Step 5: Remove rows with wrong data that cannot be transformed
        #df = df.dropna()
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
            self.dfs = {key: self.clean_data(df) for key, df in merged_dfs.items()}

            # Join dfs
            self._join_dfs()
            
            self.logger.info(f"Columns of the final join are: {self.dfs['final'].columns}")

            # Write each DataFrame to MongoDB
            self._load_to_mongo()  
            self.logger.info("Finished Formatting Data and loading it with success!")

        except Exception:
            self.logger.error("Error formatting data", exc_info=True)

def get_all_columns(dfs):
    all_columns = set()
    for df in dfs:
        all_columns.update(df.columns)
    return list(all_columns) if all_columns else []

def explode_column(dfs, column_name):
    output_dfs = []
    for df in dfs:
        if column_name in df.columns:
            for subcol in df.select(column_name + '.*').columns:
                df = df.withColumn(subcol, col(column_name + '.' + subcol))
            output_dfs.append(df.drop(column_name))
    return output_dfs

def ensure_all_columns(df, all_columns):
    # Add missing columns with null values
    for col_name in all_columns:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None))

    # Reorder columns to match the order in all_columns
    ordered_columns = [col_name for col_name in all_columns if col_name in df.columns]
    df = df.select(*ordered_columns)

    return df

def join_and_union(dfs, df_lookup, join_column, lookup_column, ensure_same_schema=False):
    joined_list = []
    all_columns = get_all_columns(dfs)
    all_columns += get_all_columns([df_lookup])

    for df in dfs:
        # This makes the district column empty
        '''if ensure_same_schema:
            df = ensure_all_columns(df, all_columns)'''

        # Ensure compatible column types on join columns
        lookup_data_type = df_lookup.schema[lookup_column].dataType
        if df.schema[join_column].dataType != lookup_data_type:
            # Handle mismatched column types
            if isinstance(lookup_data_type, StructType):
                # If the lookup column type is a struct, cast the DataFrame column to match
                df = df.withColumn(
                    join_column, df[join_column].cast(lookup_data_type))
            else:
                # Otherwise, add or replace the column with null values
                df = df.withColumn(join_column, lit(None).cast(lookup_data_type))

        # logger.info_shape_info(df, f"{df_name} before join")
        joined_df = df.join(df_lookup, df[join_column] == df_lookup[lookup_column], 'left')
        # logger.info_shape_info(joined_df, f"{df_name} after join")
        
        # Alias columns to avoid ambiguous references
        rdd = joined_df.rdd
        # We use RDDs because withColumnRenamed
        # changes the name of all columns with the old name, so if we
        # have two columns named equally, after renaming them,
        # they will keep having the same name

        aliased_columns = []
        for col_name in joined_df.columns:
            aliased_name = col_name
            count = 1
            while aliased_name in aliased_columns:
                aliased_name = f"{col_name}_{count}"
                count += 1
            aliased_columns.append(aliased_name)
        
        schema = joined_df.schema
        for idx, aliased_column in enumerate(aliased_columns):
            schema[idx].name = aliased_column
        joined_df = rdd.toDF(schema)
        
        # Reorder columns
        joined_df = ensure_all_columns(joined_df, all_columns)

        joined_list.append(joined_df)

    # Union
    if joined_list:
        merged_df = joined_list[0]
        for df in joined_list[1:]:
            merged_df = merged_df.union(df)
        return merged_df
    return None

def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    only_ascii = nfkd_form.encode('ASCII', 'ignore')
    return only_ascii.decode()

def lowercase_lookup(s):
    # Replace this with your actual string function
    return remove_accents(s.lower().replace('-', ' '))
