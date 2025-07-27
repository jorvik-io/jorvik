from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType


schema=StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("registration_date", DateType(), True)
])
path="/mnt/bronze/raw_customers/data"
format="delta"
