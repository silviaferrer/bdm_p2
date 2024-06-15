from pyspark.sql.types import StringType
import os
import re
import unicodedata
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StructType


dir_path = os.getcwd()
data_path = os.path.join(dir_path, 'data')
out_path = os.path.join(data_path, 'output')
idealista_path = os.path.join(data_path, 'idealista')
income_path = os.path.join(data_path, 'income_opendata')
lookuptables_path = os.path.join(data_path, 'lookup_tables')
airquality_path = os.path.join(data_path, 'airquality_data')


class DataFormatter:
    def __init__(self, dfs):
        self.dfs = dfs

        # Remove duplicates from each DataFrame in the dictionary
        print("Removing duplicates...")
        self.remove_duplicates()
        print("Duplicates removed!")

        #self.output_first_5_rows_to_csv(out_path)

        # Append, join, and reconciliate data
        print("Reconciliating data...")
        merged_df = self.reconciliate_data()
        print("Data reconciliated")

        # Clean data
        #self.clean_data()


    '''def output_first_5_rows_to_csv(self, output_dir):
        for key, df in self.dfs.items():
            if key == 'lookup' or key=='idealista':
                if isinstance(df, list) and len(df) > 0:
                    for i, sub_df in enumerate(df):
                        # Convert to Pandas DataFrame
                        pandas_df = sub_df.toPandas()
                        # Output to CSV file
                        pandas_df.head().to_csv(f"{output_dir}/{key}_{i}_first_5_rows.csv", index=False)
                        if i == 10:
                            break
            else:
                if isinstance(df, list) and len(df) > 0:
                    # Convert to Pandas DataFrame
                    pandas_df = df[0].toPandas()
                    # Output to CSV file
                    pandas_df.head().to_csv(f"{output_dir}/{key}_first_5_rows.csv", index=False)
                else:
                    # Convert to Pandas DataFrame
                    pandas_df = df.toPandas()
                    # Output to CSV file
                    pandas_df.head().to_csv(f"{output_dir}/{key}_first_5_rows.csv", index=False)'''


    def remove_duplicates(self):
        # Iterate over each DataFrame in the dictionary and drop duplicates
        for df_family_name, df_family in self.dfs.items():
            for file_name, df in df_family.items():
                if isinstance(df, list):
                    # Handle list of DataFrames
                    self.dfs[df_family_name][file_name] = df.dropDuplicates()


    def reconciliate_data(self):
        '''df_lookup_list = self.dfs.get('lookup')
        # Find the lookup DataFrame that contains the 'district' column
        df_lookup = None
        if isinstance(df_lookup_list, list):
            for df in df_lookup_list:
                if 'district' in df.columns:
                    df_lookup = df
                    break'''

        '''if df_lookup is not None:'''
        print('Starting join process...')
        def print_shape_info(df, df_name):
            if df is not None:
                print(f"{df_name} shape: {df.count()} rows, {len(df.columns)} columns")
            else:
                print(f"{df_name} is None")

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

        def join_and_union(dfs, df_lookup, join_column, lookup_column, df_name, ensure_same_schema=False):
            joined_list = []
            all_columns = get_all_columns(dfs)
            all_columns += get_all_columns([df_lookup])

            for df in dfs:
                if ensure_same_schema:
                    df = ensure_all_columns(df, all_columns)

                # Ensure compatible column types
                '''for col_name in df.columns:
                    if col_name in df_lookup.columns:
                        lookup_data_type = df_lookup.schema[col_name].dataType
                        if df.schema[col_name].dataType != lookup_data_type:
                            # Handle mismatched column types
                            if isinstance(lookup_data_type, StructType):
                                # If the lookup column type is a struct, cast the DataFrame column to match
                                df = df.withColumn(col_name, df[col_name].cast(lookup_data_type))
                            else:
                                # Otherwise, add or replace the column with null values
                                df = df.withColumn(col_name, lit(None).cast(lookup_data_type))'''
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

                print_shape_info(df, f"{df_name} before join")
                joined_df = df.join(df_lookup, df[join_column] == df_lookup[lookup_column], 'left')
                print_shape_info(joined_df, f"{df_name} after join")
                
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
                #joined_df = rdd.toDF(Row(*aliased_columns))
                
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


        # Process df_income
        df_income_list = list(self.dfs.get('income').values())
        if df_income_list is not None and isinstance(df_income_list, list):
            # Reconcile district
            df_income_join_column = 'district_name'  # Replace with the actual join column in df_income
            df_lookup_income_column = 'district'  # Replace with the actual join column in df_lookup for df_income
            df_lookup = self.dfs['lookup']['income_lookup_district.json'].select('district', 'district_reconciled')
            df_income_merged = join_and_union(df_income_list, df_lookup, df_income_join_column, df_lookup_income_column, "df_income")
            # Keep only reconciled column
            df_income_merged = df_income_merged.drop('district', 'district_name', 'district_id').withColumnRenamed('district_reconciled', 'district')

            # Reconcile neigborhood
            df_lookup = self.dfs['lookup']['income_lookup_neighborhood.json'].select(
                'neighborhood', 'neighborhood_reconciled')
            df_income_merged = join_and_union([df_income_merged]
                                              , df_lookup
                                              , 'neigh_name '
                                              , 'neighborhood'
                                              , 'df_income')
            df_income_merged = df_income_merged.drop('neigh_name ', 'neighborhood').withColumnRenamed('neighborhood_reconciled', 'neighborhood')

            # Explode info column into year, pop, RFD
            df_income_merged = df_income_merged.withColumn('RFD', 
                col('info')[0]['RFD']).withColumn('pop', col('info')[0]['pop']).withColumn('year', col('info')[0]['year']).drop('info')

            print(f"Income after merge columns: {df_income_merged.columns}")

        # Process df_idealista
        df_idealista_list = list(self.dfs.get('idealista').values())
        if df_idealista_list is not None and isinstance(df_idealista_list, list):
            # =================================================================
            # Explode columns parkingSpace, detailedType, suggestedTexts
            # =================================================================
            df_idealista_list = explode_column(df_idealista_list, 'parkingSpace')
            df_idealista_list = explode_column(df_idealista_list, 'detailedType')
            df_idealista_list = explode_column(df_idealista_list, 'suggestedTexts')

            # Reconcile district
            df_idealista_join_column = 'district'  # Replace with the actual join column in each df_idealista
            df_lookup_idealista_column = 'di'  # Replace with the actual join column in df_lookup for df_idealista
            df_lookup = self.dfs['lookup']['rent_lookup_district.json'].select('di', 'di_re')
            df_idealista_merged = join_and_union(df_idealista_list, df_lookup, df_idealista_join_column, df_lookup_idealista_column, "df_idealista", ensure_same_schema=True)
            # Keep only reconciled column
            df_idealista_merged = df_idealista_merged.drop('di', 'district').withColumnRenamed('di_re', 'district')

            # Reconcile neighborhood
            df_lookup = self.dfs['lookup']['rent_lookup_neighborhood.json'].select(
                'ne', 'ne_re')
            df_idealista_merged = join_and_union([df_idealista_merged], df_lookup, 'neighborhood', 'ne', 'df_idealista')
            df_idealista_merged = df_idealista_merged.drop('neighborhood', 'ne').withColumnRenamed('ne_re', 'neighborhood')

            print(f"Idealista after merge columns: {df_idealista_merged.columns}")

        # Process df_airqual
        df_airqual_list = list(self.dfs.get('airqual').values())
        if df_airqual_list is not None and isinstance(df_airqual_list, list):
            # Transform district and neighborhood names to join them with the
            # lowercase names in the lookup tables
            def remove_accents(input_str):
                nfkd_form = unicodedata.normalize('NFKD', input_str)
                only_ascii = nfkd_form.encode('ASCII', 'ignore')
                return only_ascii.decode()
            def lowercase_lookup(s):
                # Replace this with your actual string function
                return remove_accents(s.lower().replace('-', ' '))
            lowercase_lookup_udf = udf(lowercase_lookup, StringType())

            # Apply the UDF to each row of the 'Nom_districte' column
            df_airqual_list = [df.withColumn('Nom_districte', lowercase_lookup_udf(df['Nom_districte'])).withColumn('Nom_barri', lowercase_lookup_udf(df['Nom_barri'])) for df in df_airqual_list]


            df_airqual_join_column = 'Nom_districte'
            df_lookup_airqual_column = 'district_name'

            '''# Transform the column in df_airqual to match the case before joining
            df_airqual_list_transformed = [
                df_airqual.withColumn(df_airqual_join_column, col(df_airqual_join_column).alias('district_name'))  # Transform the column to lowercase
                for df_airqual in df_airqual_list
            ]'''
            # df_lookup for df_airqual
            df_lookup = self.dfs['lookup']['income_lookup_district.json'].select(
                'district_name', 'district_reconciled')
            df_airqual_merged = join_and_union(
                df_airqual_list, df_lookup, df_airqual_join_column, df_lookup_airqual_column, "df_airqual")
            # Keep only reconciled column
            df_airqual_merged = df_airqual_merged.drop(
                'district_name', 'Nom_districte', 'Codi_districte').withColumnRenamed('district_reconciled', 'district')

            # Reconcile neigborhood
            df_lookup = self.dfs['lookup']['income_lookup_neighborhood.json'].select(
                'neighborhood_name', 'neighborhood_reconciled')
            df_airqual_merged = join_and_union(
                [df_airqual_merged], df_lookup, 'Nom_barri', 'neighborhood_name', 'df_airqual')
            df_airqual_merged = df_airqual_merged.drop('Nom_barri', 'neighborhood_name', 'Codi_barri').withColumnRenamed(
                'neighborhood_reconciled', 'neighborhood')

            print(f"Airqual after merge columns: {df_airqual_merged.columns}")

        '''print('Starting final join...')
        # Merge df_income, df_airqual, and df_idealista into a single DataFrame
        final_df = df_income_merged
        if df_airqual_merged is not None:
            final_df = final_df.join(df_airqual_merged, final_df[df_income_join_column] == df_airqual_merged[df_airqual_join_column], 'outer')
        if df_idealista_merged is not None:
            final_df = final_df.join(df_idealista_merged, final_df[df_income_join_column] == df_idealista_merged[df_idealista_join_column], 'outer')

        print_shape_info(final_df, "final_df")

        # Store the final DataFrame in the dictionary
        self.dfs['final'] = final_df

        return final_df'''

    def clean_data(self, df):
        # Step 0: Remove columns with > 30% null values
        threshold = 0.3 * df.count()
        for col in df.columns:
            if df.filter(df[col].isNull()).count() > threshold:
                df = df.drop(col)

        # Step 1: Remove rows with null values
        df = df.dropna()

        # Step 2: Find columns with wrong values (e.g., negative height)
        if 'height' in df.columns:
            df = df.filter(df['height'] >= 0)

        # Step 3: Transform wrong columns (if necessary)
        if 'age' in df.columns:
            df = df.withColumn('age', F.when(df['age'] < 0, None).otherwise(df['age']))

        # Step 4: Remove rows with wrong data that cannot be transformed
        df = df.dropna()

        return df



'''if __name__ == '__main__':

    # Initialize Spark connection
    try:
        spark = SparkSession.builder.appName(
            "P2FormattedZone").getOrCreate()
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
'''