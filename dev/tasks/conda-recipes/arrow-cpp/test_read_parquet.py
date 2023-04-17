import pyarrow as pa
import pyarrow.parquet as pq

table = pa.Table.from_pydict({"a": [1, 2]})
pq.write_table(table, "test.parquet")
