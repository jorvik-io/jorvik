## Data lineage
To enable data lineage tracking, configure the property `io.jorvik.data_lineage.log_path` with a valid storage path when initializing the Spark session.  
Once this property is set, all write operations using `st.write()` will automatically generate a lineage log entry at the specified path.  
Data lineage log is stored in Delta table format.

To disable data lineage tracking for a specific write operation, pass the parameter `track_lineage=False` to the `configure()` function:
```python
st = configure(track_lineage=False)
```