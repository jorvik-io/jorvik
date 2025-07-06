from pyspark.sql.types import StructType


def is_subset(schema: StructType, ref_schema: StructType) -> bool:
    """ Check if given schema is a subset of the given ref_schema.

    Args:
        schema (StructType): The schema to check.
        ref_schema (StructType): The schema to check against.
    Returns:
        bool: True if the schema is a subset of the ref_schema, False otherwise.
    """
    fields = {field.name: field for field in ref_schema}
    for field in schema.fields:
        if field.name not in fields:
            return False

        if field.dataType != fields[field.name].dataType:
            return False

        if isinstance(field, StructType):
            if not are_equal(field.dataType, fields[field.name].dataType):
                return False
    return True


def are_equal(schema1: StructType, schema2: StructType) -> bool:
    """ Compare two schemas to see if they are equal.
    Ignores column ordering and whether the columns are Nullable.

    Args:
        schema1 (StructType): The first schema to compare.
        schema2 (StructType): The second schema to compare.
    Returns:
        bool: True if the schemas are equal, False otherwise.
    """

    if len(schema1.fields) != len(schema2.fields):
        return False

    fields1 = {field.name: field for field in schema1}
    fields2 = {field.name: field for field in schema2}

    if len(fields1) != len(fields2):
        return False

    for name in fields1:
        if name not in fields2:
            return False
        if fields1[name].dataType != fields2[name].dataType:
            return False
        if isinstance(fields1[name].dataType, StructType):
            if not are_equal(fields1[name].dataType, fields2[name].dataType):
                return False

    return True
