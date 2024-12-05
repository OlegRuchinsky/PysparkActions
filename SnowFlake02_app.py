import os

os.environ['SPARK_LOCAL_IP'] = '10.0.0.68'  # Set your desired IP address
from pyspark.sql import DataFrame, functions as F
from pyspark.sql import SparkSession


# Paths to the required JAR files
snowflake_spark_connector = "spark-snowflake_2.12-3.0.0.jar"
snowflake_jdbc = "snowflake-jdbc-3.20.0.jar"

# Initialize SparkSession with Snowflake dependencies
spark = SparkSession.builder \
    .appName("SnowflakeIntegration") \
    .config("spark.jars", f"{snowflake_spark_connector},{snowflake_jdbc}") \
    .getOrCreate()


def get_df() -> DataFrame:
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
    for col in df.columns:
        df = df.withColumnRenamed(col, col.lower())
    return df


def add_transformations(df: DataFrame) -> DataFrame:
    # Example transformations
    # 1. Add a new column with a constant value
    df = df.withColumn("new_column", F.lit("example_value"))
    # 2. Filter rows where 'acctbal' is greater than 5000
    df = df.filter(F.col("c_acctbal") > 5000)
    # 3. Select specific columns
    df = df.select('c_custkey', 'c_name', 'c_acctbal', "new_column")
    return df


def main():
    df = get_df()
    print("Original DataFrame:")
    df.show()

    # Apply transformations
    df = lowercase_all_column_names(df)
    print("After Lowercasing Column Names:")
    df.show()

    df = add_transformations(df)
    print("After Applying Transformations:")
    df.show()


if __name__ == "__main__":
    main()
