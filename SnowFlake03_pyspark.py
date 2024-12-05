import os
from pyspark.sql import DataFrame, functions as F
from pyspark.sql import SparkSession

# Define function to initialize SparkSession
def get_spark_session(app_name="DefaultApp", local_mode=True, include_jars=False) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    if local_mode:
        builder = builder.master("local[*]")  # Use local mode for testing or small-scale runs
    if include_jars:
        # Add paths to required JAR files for Snowflake
        snowflake_spark_connector = "spark-snowflake_2.12-3.0.0.jar"
        snowflake_jdbc = "snowflake-jdbc-3.20.0.jar"
        builder = builder.config("spark.jars", f"{snowflake_spark_connector},{snowflake_jdbc}")
    return builder.getOrCreate()


# Initialize SparkSession for the script
spark = get_spark_session(app_name="SnowflakeIntegration", include_jars=True)


def get_df(spark: SparkSession) -> DataFrame:
    # Snowflake connection options
    sfOptions = {
        "sfURL": "https://kqksihm-qpb55785.snowflakecomputing.com",
        "sfDatabase": "SNOWFLAKE_SAMPLE_DATA",
        "sfSchema": "TPCH_SF1",
        "sfWarehouse": "COMPUTE_WH",
        "sfRole": "ACCOUNTADMIN",
        "sfUser": "ORUCHINSKY",
        "sfPassword": "ve367k7mEpwYuqB"
    }
    # Read data from Snowflake into a Spark DataFrame
    df = spark.read.format("snowflake") \
        .options(**sfOptions) \
        .option("dbtable", "CUSTOMER") \
        .load()
    return df


def lowercase_all_column_names(df: DataFrame) -> DataFrame:
    # Convert all column names to lowercase
    for col in df.columns:
        df = df.withColumnRenamed(col, col.lower())
    return df


def add_transformations(df: DataFrame) -> DataFrame:
    # Add a constant column, filter, and select specific columns
    df = df.withColumn("new_column", F.lit("example_value"))
    df = df.filter(F.col("c_acctbal") > 5000)
    df = df.select('c_custkey', 'c_name', 'c_acctbal', "new_column")
    return df


def test_row_count(df: DataFrame):
    """
    Validates the row count before and after transformations.
    """
    # Count rows in the original DataFrame
    original_count = df.count()
    print(f"Original DataFrame row count: {original_count}")

    # Apply lowercase column name transformation
    df = lowercase_all_column_names(df)

    # Apply additional transformations
    transformed_df = add_transformations(df)
    transformed_count = transformed_df.count()
    print(f"Transformed DataFrame row count: {transformed_count}")

    # Log the results
    print(f"Row count before transformations: {original_count}")
    print(f"Row count after transformations: {transformed_count}")

    return original_count, transformed_count


def main():
    df = get_df(spark)
    print("Original DataFrame:")
    df.show()

    # Apply transformations
    df = lowercase_all_column_names(df)
    print("After Lowercasing Column Names:")
    df.show()

    df = add_transformations(df)
    print("After Applying Transformations:")
    df.show()

    # Test row count
    original_count, transformed_count = test_row_count(df)

    print(f"Test completed successfully.")
    print(f"Row count before transformations: {original_count}")
    print(f"Row count after transformations: {transformed_count}")

if __name__ == "__main__":
    main()
