from pyspark.sql import Column, DataFrame


def add_column(df: DataFrame, column_name: str, value: Column) -> DataFrame:
    """
    Adds a new column to the DataFrame with the specified value.
    
    Args:
        df (DataFrame): The DataFrame to which the column will be added.
        column_name (str): The name of the new column.
        value (Column): The value to be assigned to the new column.
    Returns:
        DataFrame: The updated DataFrame with the new column.
    """
    return  df.withColumn(column_name, value)
