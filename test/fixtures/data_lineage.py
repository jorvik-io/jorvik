import pytest
from unittest.mock import patch
from pyspark.sql import SparkSession
import delta

# flake8: noqa: E501

LINEAGE_LOG_PATH = "/tmp/data_lineage/logs"

@pytest.fixture
def spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
        .config("io.jorvik.data_lineage.log_path", LINEAGE_LOG_PATH)  # Set the log path for lineage
    )
    return delta.configure_spark_with_delta_pip(builder).getOrCreate()


# Simple explain result of a delta table joined with an inline DataFrame
EXPLAIN_RESULT = '''== Physical Plan ==
AdaptiveSparkPlan (12)
+- == Initial Plan ==
   Project (11)
   +- SortMergeJoin LeftOuter (10)
      :- Sort (5)
      :  +- Exchange (4)
      :     +- Project (3)
      :        +- Filter (2)
      :           +- Scan parquet  (1)
      +- Sort (9)
         +- Exchange (8)
            +- Filter (7)
               +- Scan ExistingRDD (6)


(1) Scan parquet 
Output [11]: [SalesOrderNumber#16, OrderDate#17, ProductKey#18, ResellerKey#19, EmployeeKey#20, SalesTerritoryKey#21, Quantity#22, UnitPrice#23, Sales#24, Cost#25, _databricks_internal_edge_computed_column_skip_row#255]
Batched: true
Location: PreparedDeltaFileIndex [dbfs:/mnt/bronze/adventure_works/sales/data]
ReadSchema: struct<SalesOrderNumber:string,OrderDate:string,ProductKey:int,ResellerKey:int,EmployeeKey:int,SalesTerritoryKey:int,Quantity:int,UnitPrice:string,Sales:string,Cost:string,_databricks_internal_edge_computed_column_skip_row:boolean>

(2) Filter
Input [11]: [SalesOrderNumber#16, OrderDate#17, ProductKey#18, ResellerKey#19, EmployeeKey#20, SalesTerritoryKey#21, Quantity#22, UnitPrice#23, Sales#24, Cost#25, _databricks_internal_edge_computed_column_skip_row#255]
Condition : if (isnotnull(_databricks_internal_edge_computed_column_skip_row#255)) (_databricks_internal_edge_computed_column_skip_row#255 = false) else isnotnull(raise_error(DELTA_SKIP_ROW_COLUMN_NOT_FILLED, map(keys: [], values: []), NullType))

(3) Project
Output [10]: [SalesOrderNumber#16, OrderDate#17, ProductKey#18, ResellerKey#19, EmployeeKey#20, SalesTerritoryKey#21, Quantity#22, UnitPrice#23, Sales#24, Cost#25]
Input [11]: [SalesOrderNumber#16, OrderDate#17, ProductKey#18, ResellerKey#19, EmployeeKey#20, SalesTerritoryKey#21, Quantity#22, UnitPrice#23, Sales#24, Cost#25, _databricks_internal_edge_computed_column_skip_row#255]

(4) Exchange
Input [10]: [SalesOrderNumber#16, OrderDate#17, ProductKey#18, ResellerKey#19, EmployeeKey#20, SalesTerritoryKey#21, Quantity#22, UnitPrice#23, Sales#24, Cost#25]
Arguments: hashpartitioning(cast(SalesTerritoryKey#21 as bigint), 200), ENSURE_REQUIREMENTS, [plan_id=236]

(5) Sort
Input [10]: [SalesOrderNumber#16, OrderDate#17, ProductKey#18, ResellerKey#19, EmployeeKey#20, SalesTerritoryKey#21, Quantity#22, UnitPrice#23, Sales#24, Cost#25]
Arguments: [cast(SalesTerritoryKey#21 as bigint) ASC NULLS FIRST], false, 0

(6) Scan ExistingRDD
Output [2]: [SalesTerritoryKey#36L, region_name#37]
Arguments: [SalesTerritoryKey#36L, region_name#37], MapPartitionsRDD[4] at applySchemaToPythonRDD at NativeMethodAccessorImpl.java:0, ExistingRDD, UnknownPartitioning(0)

(7) Filter
Input [2]: [SalesTerritoryKey#36L, region_name#37]
Condition : isnotnull(SalesTerritoryKey#36L)

(8) Exchange
Input [2]: [SalesTerritoryKey#36L, region_name#37]
Arguments: hashpartitioning(SalesTerritoryKey#36L, 200), ENSURE_REQUIREMENTS, [plan_id=237]

(9) Sort
Input [2]: [SalesTerritoryKey#36L, region_name#37]
Arguments: [SalesTerritoryKey#36L ASC NULLS FIRST], false, 0

(10) SortMergeJoin
Left keys [1]: [cast(SalesTerritoryKey#21 as bigint)]
Right keys [1]: [SalesTerritoryKey#36L]
Join type: LeftOuter
Join condition: None

(11) Project
Output [11]: [SalesTerritoryKey#21, SalesOrderNumber#16, OrderDate#17, ProductKey#18, ResellerKey#19, EmployeeKey#20, Quantity#22, UnitPrice#23, Sales#24, Cost#25, region_name#37]
Input [12]: [SalesOrderNumber#16, OrderDate#17, ProductKey#18, ResellerKey#19, EmployeeKey#20, SalesTerritoryKey#21, Quantity#22, UnitPrice#23, Sales#24, Cost#25, SalesTerritoryKey#36L, region_name#37]

(12) AdaptiveSparkPlan
Output [11]: [SalesTerritoryKey#21, SalesOrderNumber#16, OrderDate#17, ProductKey#18, ResellerKey#19, EmployeeKey#20, Quantity#22, UnitPrice#23, Sales#24, Cost#25, region_name#37]
Arguments: isFinalPlan=false
'''

EXECUTION_PLAN, NODE_DETAILS = EXPLAIN_RESULT.split('\n\n\n')[:2]

@pytest.fixture
def explain_result() -> str:
    return EXPLAIN_RESULT

@pytest.fixture
def execution_plan_str() -> str:
    return EXECUTION_PLAN

@pytest.fixture
def node_details_str() -> str:
    return NODE_DETAILS

@pytest.fixture
def mock_capture_explain():
    with patch('jorvik.data_lineage.execution_plan.capture_explain', return_value=EXPLAIN_RESULT):
        yield
