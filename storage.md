# Storage
The Storage package provides utilities for reading and writing files in the storage layer.

## Basic
```python
from jorvik import storage

st = storage.configure()

df = st.read("/path/to/table/", format="delta")

st.write(df, "/new/path/to/table", format="parquet", mode="overwrite")
```
