# Storage
The Storage package provides utilities for reading and writing files in the storage layer.

## Basic
```python
from jorvik import storage
st = storage.configure()
df = st.read("/path/to/table/", format="delta")
st.write(df, "/new/path/to/table", format="parquet", mode="overwrite")
```

## Storage isolation

### Isolated storage
- TO BE DOCUMENTED: `IsolatedStorage` class

### Isolation provider

The Isolation Provider determines how Jorvik establishes an isolation context to separate data across development environments.

You configure the isolation provider by setting the Spark configuration key `io.jorvik.storage.isolation_provider` to one of the supported values.

Supported values and their behaviors are described below:
1. `DATABRICKS_GIT_BRANCH`  
Jorvik uses the Git branch in Databricks as the isolation context. This applies to:
    - Interactive notebooks hosted in a Git folder
    - Workflows where the source is a Git provider
2. `DATABRICKS_USER`  
Jorvik uses the current Databricks username as the isolation context. Each user gets access to their own isolated environment.
3. `DATABRICKS_CLUSTER`  
Jorvik uses the ID of the cluster currently attached to the Databricks notebook as the isolation context. All code files running on the same cluster share a common isolated environment.
4. `GIT_BRANCH`  
Jorvik uses the currently active local Git branch as the isolation context.
    > **Note** This requires the code to reside in a Git repository and the Git CLI to be installed on the host machine.
5. `ENVIRONMENT_VARIABLE`  
Jorvik uses the value of the environment variable `JORVIK_ISOLATION_CONTEXT` as the isolation context.  
If the environment variable is not set, the context defaults to an empty string — meaning no isolation.
6. `SPARK_CONFIG`
Jorvik uses the value of the Spark configuration key `io.jorvik.storage.isolation_context` as the isolation context.  
If this key is not set, the isolation context defaults to an empty string — meaning no isolation.

## Data lineage
[Enable Data Lineage Tracking](https://github.com/jorvik-io/jorvik/blob/main/jorvik/data_lineage/README.md)
