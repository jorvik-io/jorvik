from datetime import date

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Jorvik test").getOrCreate()

spark.createDataFrame(  # noqa: F821
    [
        ("1", "John Doe", "jhon.doe@mail.com", 30, "New York", date(2022, 1, 1)),
        ("2", "Jane Doe", "jane.doe@mail.com", 25, "Los Angeles", date(2022, 1, 1)),
        ("3", "Mike Smith", "mike.smith@mail.com", 40, "Chicago", date(2022, 1, 1)),
        ("4", "Sara Johnson", "sara.johnson@mail.com", 35, "Houston", date(2022, 1, 1)),
        ("5", "Tom Brown", "tom.brown@mail.com", 28, "Miami", date(2022, 1, 1)),
    ]).show()
