
import os
os.environ['SPARK_LOCAL_IP'] = '10.0.0.68'  # Set your desired IP address

from pyspark.sql import SparkSession

# Paths to the required JAR files
snowflake_spark_connector = "spark-snowflake_2.12-3.0.0.jar"
snowflake_jdbc = "snowflake-jdbc-3.20.0.jar"

# Initialize SparkSession with Snowflake dependencies
spark = SparkSession.builder \
    .appName("SnowflakeIntegration") \
    .config("spark.jars", f"{snowflake_spark_connector},{snowflake_jdbc}") \
    .getOrCreate()

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

# Show the first few rows of the DataFrame
df.show(truncate=True)

