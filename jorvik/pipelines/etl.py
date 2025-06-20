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

    @abstractmethod
    def extract(self):
        """ Extract data from the source. """
        ...


@dataclass
class Output(ABC):

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
    def __init__(self, inputs: list[Input], outputs: list[Output],
                 transform_func: Callable[[tuple[DataFrame, ...]], tuple[DataFrame, ...]],
                 validate_schemas: bool = True):
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
        assert len(transformed) == len(self.outputs), "Number of transformed dataframes must match number of outputs"
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
            if i.schema is not None:
                assert schemas.is_subset(i.schema, df.schema)

    def verify_output_schemas(self, data: tuple[DataFrame, ...]):
        """ Verify that the output schema matches the expected schema. """
        for o, df in zip(self.outputs, data):
            if o.schema is not None:
                assert schemas.are_equal(o.schema, df.schema)

def etl(inputs: list[Input], outputs: list[Output], validate_schemas: bool = True):
    """ Decorator to run the ETL process. """
    def wrapper(func):
        return ETL(inputs, outputs, func, validate_schemas=validate_schemas)
    return wrapper
