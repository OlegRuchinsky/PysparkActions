from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("Datacamp Pyspark Tutorial")
         .config("spark.memory.offHeap.enabled", "true")
         .config("spark.memory.offHeap.size", "10g")
         .master('local').getOrCreate())

df_children_with_schema = spark.createDataFrame(
  data=[("Mikhail", 15), ("Zaky", 21), ("Zoya", 8)],
  schema=StructType([
    StructField('name', StringType(), True),
    StructField('age', IntegerType(), True)
  ])
)
print(spark.sparkContext.version) # 3.5.3

# Print the Spark version
print("Spark Version:", spark.version)

print("Scala Version:", spark.sparkContext.getConf().get("spark.driver.extraClassPath"))
# spark-shell --version
# export SPARK_LOCAL_IP=10.0.0.68