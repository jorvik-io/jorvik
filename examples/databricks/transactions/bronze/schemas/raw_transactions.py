from pyspark.sql.types import StringType, StructField, StructType, IntegerType, FloatType, TimestampType


schema = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), True),
        StructField("price", FloatType(), True),
        StructField("timestamp", TimestampType(), True)
    ])
path="/mnt/bronze/raw_transactions/data"
format="delta"
