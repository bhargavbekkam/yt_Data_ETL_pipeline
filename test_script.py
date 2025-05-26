import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Local Spark Test") \
    .master("local[*]") \
    .getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Charlie", 28)]
df = spark.createDataFrame(data, ["Name", "Age"])

df.show()
