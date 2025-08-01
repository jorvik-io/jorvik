from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType, TimestampType, DoubleType


schema=StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("total_transactions", LongType(), False),
    StructField("total_spent", DoubleType(), True),
    StructField("avg_transaction_value", DoubleType(), True),
    StructField("first_purchase_date", DateType(), True),
    StructField("last_purchase_date", DateType(), True),
    StructField("customer_segment", StringType(), False)
])
path="/mnt/silver/customer_summary/data"
format="delta"
