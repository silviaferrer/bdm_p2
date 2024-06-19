import unicodedata

from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType


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
    ordered_columns = [
        col_name for col_name in all_columns if col_name in df.columns]
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
                df = df.withColumn(join_column, lit(
                    None).cast(lookup_data_type))

        # logger.info_shape_info(df, f"{df_name} before join")
        joined_df = df.join(
            df_lookup, df[join_column] == df_lookup[lookup_column], 'left')
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


def loadFromSpark(spark, mongoLoader, logger, collections):

    uri = f"mongodb+srv://airdac:1234@cluster0.brrlvo1.mongodb.net/{mongoLoader.database_name}?retryWrites=true&w=majority&appName=Cluster0"
      # Dictionary to hold the DataFrames
    dfs = {}

    # Load each collection into a DataFrame
    for collection in collections:
        logger.info(f"Loading collection '{collection}' into DataFrame")
        # df = self.spark.read.format("mongo").option("uri", f"mongodb+srv://airdac:1234@cluster0.brrlvo1.mongodb.net/{db_name}.{collection}?retryWrites=true&w=majority&appName=Cluster0").load()
        df = spark.read.format("mongo").option("uri", uri).option(
            'collection', collection).option("encoding", "utf-8-sig").load()
        # df = self.mongoLoader.read_collection(self.spark,collection)
        dfs[collection] = df
        logger.info(f"Loaded collection '{collection}' into DataFrame")

    # Example: Show the schema and first few rows of each DataFrame
    for collection, df in dfs.items():
        logger.info(f"Schema for collection '{collection}':" +
                            df.schema.simpleString())
        logger.info(f"First few rows of collection '{collection}':" +
                            df._show_string(5))

    return dfs
