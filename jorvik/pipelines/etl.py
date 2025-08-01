from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable
import warnings

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from jorvik import storage
from jorvik.audit import schemas


@dataclass
class Input(ABC):
    schema = None

    @abstractmethod
    def extract(self):
        """ Extract data from the source. """
        ...


@dataclass
class Output(ABC):
    schema = None

    @abstractmethod
    def load(self, df: DataFrame):
        """ Load data into the target system. """
        ...


@dataclass
class FileInput(Input):
    path: str
    format: str
    options: dict = None
    schema: StructType = None

    def __post_init__(self):
        if self.schema is None:
            warnings.warn(
                "Missing schema definition. Specifying a schema increases a jobs cohesion and robustness.",
                UserWarning,
            )
            return

    def extract(self) -> DataFrame:
        """ Extract data from a file."""
        st = storage.configure()
        return st.read(self.path, self.format, self.options)


@dataclass
class StreamFileInput(Input):
    path: str
    format: str
    options: dict = None
    schema: StructType = None

    def __post_init__(self):
        if self.schema is None:
            warnings.warn(
                "Missing schema definition. Specifying a schema increases a jobs cohesion and robustness.",
                UserWarning,
            )
            return

    def extract(self) -> DataFrame:
        """ Extract data from a file. """
        st = storage.configure()
        return st.readStream(self.path, self.format, self.options)


@dataclass
class FileOutput(Output):
    path: str
    format: str
    mode: str
    partition_fields: str | list = ""
    options: dict = None
    schema: StructType = None

    def __post_init__(self):
        if self.schema is None:
            warnings.warn(
                "Missing schema definition. Specifying a schema increases a jobs cohesion and robustness.",
                UserWarning,
            )
            return

    def load(self, df: DataFrame):
        """ Load data into a file. """
        st = storage.configure()
        st.write(df, self.path, self.format, self.mode, self.partition_fields, self.options)


@dataclass
class MergeDeltaOutput(Output):
    path: str
    merge_condition: str
    merge_schemas: bool = False
    update_condition: str = None
    insert_condition: str = None
    schema: StructType = None

    def __post_init__(self):
        if self.schema is None:
            warnings.warn(
                "Missing schema definition. Specifying a schema increases a jobs cohesion and robustness.",
                UserWarning,
            )
            return

    def load(self, df: DataFrame):
        """ Load data into a file. """
        st = storage.configure()
        st.merge(df, self.path, self.merge_condition, self.merge_schemas,
                 self.update_condition, self.insert_condition)


@dataclass
class StreamFileOutput(Output):
    path: str
    format: str
    checkpoint: str
    partition_fields: str | list = ""
    options: dict = None
    schema: StructType = None

    def __post_init__(self):
        if self.schema is None:
            warnings.warn(
                "Missing schema definition. Specifying a schema increases a jobs cohesion and robustness.",
                UserWarning,
            )
            return

    def load(self, df: DataFrame):
        """ Load data into a file. """
        st = storage.configure()
        st.writeStream(df, self.path, self.format, self.checkpoint, self.partition_fields, self.options)


class ETL:
    def __init__(self, inputs: list[Input] | Input, outputs: list[Output] | Output,
                 transform_func: Callable[[tuple[DataFrame, ...]], tuple[DataFrame, ...]],
                 validate_schemas: bool = True):
        if not isinstance(inputs, list):
            inputs = [inputs]
        if not isinstance(outputs, list):
            outputs = [outputs]
        self.inputs = inputs
        self.outputs = outputs
        self.transform_func = transform_func
        self.validate_schemas = validate_schemas

    def __call__(self):
        """ Run the ETL process. """
        self.run()

    def extract(self) -> tuple[DataFrame, ...]:
        """ Extract data from the source. """
        return tuple(i.extract() for i in self.inputs)

    def load(self, *transformed: DataFrame):
        """ Load the transformed data into the target system. """
        if len(transformed) != len(self.outputs):
            raise RuntimeError("Number of transformed dataframes must match number of outputs")
        for df, out in zip(transformed, self.outputs):
            out.load(df)

    def run(self):
        """ Run the ETL process. """
        data = self.extract()
        if self.validate_schemas:
            self.verify_input_schemas(data)

        transformed = self.transform_func(*data)

        if not isinstance(transformed, tuple):
            transformed = (transformed,)

        if self.validate_schemas:
            self.verify_output_schemas(transformed)

        self.load(*transformed)

    def verify_input_schemas(self, data: tuple[DataFrame, ...]):
        """ Verify that the input schema matches the expected schema. """
        for i, df in zip(self.inputs, data):
            if i.schema is None:
                raise RuntimeError("No schema defined for input and the validate_schemas parameter is set to True."
                                   " To suppress this set the validate_schemas parameter to False.")
            if not schemas.is_subset(i.schema, df.schema):
                expected = "\n".join(map(str, i.schema.fields))
                actual = "\n".join(map(str, df.schema.fields))
                raise RuntimeError("Input schema did not match expectations"
                                   f"\nexpected: \n{expected} \n\nactual: \n{actual}")

    def verify_output_schemas(self, data: tuple[DataFrame, ...]):
        """ Verify that the output schema matches the expected schema. """
        for o, df in zip(self.outputs, data):
            if o.schema is None:
                raise RuntimeError("No schema defined for output and the validate_schemas parameter is set to True."
                                   " To suppress this set the validate_schemas parameter to False.")
            if not schemas.are_equal(o.schema, df.schema):
                expected = "\n".join(map(str, o.schema.fields))
                actual = "\n".join(map(str, df.schema.fields))
                raise RuntimeError("Output schema did not match expectations"
                                   f"\nexpected: \n{expected} \n\nactual: \n{actual}")

def etl(inputs: list[Input], outputs: list[Output], validate_schemas: bool = True):
    """ Decorator to run the ETL process. """
    def wrapper(func):
        return ETL(inputs, outputs, func, validate_schemas=validate_schemas)
    return wrapper
