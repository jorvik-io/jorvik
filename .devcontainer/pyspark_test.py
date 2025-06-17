from pyspark.sql import SparkSession

# Simple example to verify PySpark installation
spark = SparkSession.builder.appName("InlineTableExample").getOrCreate()
data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)
df.show()
