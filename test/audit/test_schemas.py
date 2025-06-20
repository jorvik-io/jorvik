from jorvik.audit import schemas

from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def test_schemas_are_equal():
    """ Test that the two schemas are equal. """

    schema1 = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("address", StructType([StructField("city", StringType(), True)]), True),
        ]
    )
    schema2 = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("address", StructType([StructField("city", StringType(), True)]), True),
        ]
    )

    assert schemas.are_equal(schema1, schema2)


def test_schemas_are_not_equal():
    """ Test that the two schemas are not equal. """

    schema1 = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("address", StructType([StructField("city", StringType(), True)]), True),
        ]
    )
    schema2 = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("address", StructType([StructField("city", StringType(), True),
                                               StructField("street", StringType(), True)]), True),
        ]
    )

    assert not schemas.are_equal(schema1, schema2)


def test_is_subset():
    """ Test that the schema is a subset of the reference schema. """

    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    ref_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("address", StructType([StructField("city", StringType(), True)]), True),
        ]
    )

    assert schemas.is_subset(schema, ref_schema)


def test_is_not_subset():
    """ Test that the schema is not a subset of the reference schema. """

    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("height", IntegerType(), True),
        ]
    )
    ref_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("address", StructType([StructField("city", StringType(), True)]), True),
        ]
    )

    assert not schemas.is_subset(schema, ref_schema)
