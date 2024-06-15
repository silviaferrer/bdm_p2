import os
import re
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
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

        self.output_first_5_rows_to_csv(out_path)

        # Append, join, and reconciliate data
        print("Reconciliating data...")
        merged_df = self.reconciliate_data()
        print("Data reconciliated")

        # Clean data
        #self.clean_data()


    def output_first_5_rows_to_csv(self, output_dir):
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
                    pandas_df.head().to_csv(f"{output_dir}/{key}_first_5_rows.csv", index=False)


    def remove_duplicates(self):
        # Iterate over each DataFrame in the dictionary and drop duplicates
        for key, df in self.dfs.items():
            if isinstance(df, list):
                # Handle list of DataFrames
                self.dfs[key] = [sub_df.dropDuplicates() for sub_df in df]
            else:
                self.dfs[key] = df.dropDuplicates()


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
                            col_name, df[col_name].cast(lookup_data_type))
                    else:
                        # Otherwise, add or replace the column with null values
                        df = df.withColumn(col_name, lit(None).cast(lookup_data_type))



                print_shape_info(df, f"{df_name} before join")
                joined_df = df.join(df_lookup, df[join_column] == df_lookup[lookup_column], 'left')
                print_shape_info(joined_df, f"{df_name} after join")
                
                # Alias columns to avoid ambiguous references
                aliased_columns = []
                for col_name in joined_df.columns:
                    aliased_name = col_name
                    count = 1
                    while aliased_name in aliased_columns:
                        aliased_name = f"{col_name}_{count}"
                        count += 1
                    aliased_columns.append(aliased_name)
                    joined_df = joined_df.withColumnRenamed(col_name, aliased_name)
                
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
        df_income_list = self.dfs.get('income').values()
        if df_income_list is not None and isinstance(df_income_list, list):
            df_income_join_column = 'district_name'  # Replace with the actual join column in df_income
            df_lookup_income_column = 'district'  # Replace with the actual join column in df_lookup for df_income
            df_lookup = self.dfs['lookup']['income_lookup_district.json']
            df_income_merged = join_and_union(df_income_list, df_lookup, df_income_join_column, df_lookup_income_column, "df_income")
            print(f"Income after merge columns: {df_income_merged.columns}")
            #=======================
            # join and union on neighborhood
            # DROP LOOKUP COLUMNS != district_reconciled, neighborhood_reconciled
            #======================

        # Process df_idealista
        df_idealista_list = self.dfs.get('idealista').values()
        if df_idealista_list is not None and isinstance(df_idealista_list, list):
            df_idealista_join_column = 'district'  # Replace with the actual join column in each df_idealista
            df_lookup_idealista_column = 'district'  # Replace with the actual join column in df_lookup for df_idealista
            df_idealista_merged = join_and_union(df_idealista_list, df_idealista_join_column, df_lookup_idealista_column, "df_idealista", ensure_same_schema=True)

        # Process df_airqual
        df_airqual_list = self.dfs.get('airqual').values()
        if df_airqual_list is not None and isinstance(df_airqual_list, list):
            df_airqual_join_column = 'Nom_districte'  # Replace with the actual join column in df_airqual
            df_lookup_airqual_column = 'district_name'  # Replace with the actual join column in df_lookup for df_airqual

            # Transform the column in df_airqual to match the case before joining
            df_airqual_list_transformed = [
                df_airqual.withColumn(df_airqual_join_column, col(df_airqual_join_column).alias('district_name'))  # Transform the column to lowercase
                for df_airqual in df_airqual_list
            ]

            df_airqual_merged = join_and_union(df_airqual_list_transformed, df_airqual_join_column, df_lookup_airqual_column, "df_airqual")

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