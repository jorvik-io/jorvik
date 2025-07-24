from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType


schema = schema = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", FloatType(), True),
        StructField("total_amount", FloatType(), True),
        StructField("transaction_date", DateType(), True),
        StructField("transaction_hour", IntegerType(), True)
    ])
path = "/mnt/silver/clean_transactions/data"
format = "delta"
