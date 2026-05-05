Sure. Here's a well-structured GitHub issue for the Apache Arrow repository:

---

**Title:** `[Python][Parquet] bloom_filter_offset not present in ColumnChunkMetaData.to_dict() output despite bloom filter being written`

**Labels:** `Component: Parquet`, `Component: Python`, `Type: bug`

---

**Describe the bug**

When writing a Parquet file with `bloom_filter_options` via `pyarrow.parquet.write_table`, the bloom filter is correctly written to disk (verified via file size delta matching SBBF sizing formula), but the bloom filter offset and length are not reflected in `ColumnChunkMetaData.to_dict()`. The keys `bloom_filter_offset` and `bloom_filter_length` are absent from the returned dict, making it impossible to programmatically verify bloom filter presence via the Python metadata API.

**pyarrow version:** 24.0.0

---

**To Reproduce**

```python
import os
import pyarrow as pa
import pyarrow.parquet as pq

table = pa.table({
    "event_id": [f"id_{i:06d}" for i in range(10_000)],
    "value":    [f"data_{i}"   for i in range(10_000)],
})

# Write without bloom filter
pq.write_table(table, "/tmp/no_bloom.parquet")

# Write with bloom filter
pq.write_table(
    table,
    "/tmp/with_bloom.parquet",
    bloom_filter_options={"event_id": {"fpp": 0.05, "ndv": 10_000}},
)

# File size confirms bloom filter is written
size_no   = os.path.getsize("/tmp/no_bloom.parquet")
size_with = os.path.getsize("/tmp/with_bloom.parquet")
print(f"Without bloom: {size_no:,} bytes")
print(f"With bloom:    {size_with:,} bytes")
print(f"Difference:    {size_with - size_no:,} bytes")  # ~16KB confirming bloom written

# But metadata does not reflect it
pf = pq.ParquetFile("/tmp/with_bloom.parquet")
col = pf.metadata.row_group(0).column(0)  # event_id column
print(col.to_dict())
# Expected: dict contains 'bloom_filter_offset' and 'bloom_filter_length'
# Actual:   neither key is present
```

**Expected output**
```
Without bloom: 135,278 bytes
With bloom:    151,687 bytes
Difference:    +16,409 bytes

{
  'path_in_schema': 'event_id',
  ...
  'bloom_filter_offset': <some_int>,
  'bloom_filter_length': <some_int>,   # if exposed
  ...
}
```

**Actual output**
```
Without bloom: 135,278 bytes
With bloom:    151,687 bytes
Difference:    +16,409 bytes

{
  'file_offset': 0,
  'file_path': '',
  'physical_type': 'BYTE_ARRAY',
  'num_values': 10000,
  'path_in_schema': 'event_id',
  'is_stats_set': True,
  'statistics': {...},
  'compression': 'SNAPPY',
  'encodings': ('PLAIN', 'RLE', 'RLE_DICTIONARY'),
  'has_dictionary_page': True,
  'dictionary_page_offset': 4,
  'data_page_offset': 49,
  'total_compressed_size': 96,
  'total_uncompressed_size': 93
  # bloom_filter_offset is absent
}
```

---

**Additional context**

The `bloom_filter_offset` field exists in the underlying Parquet Thrift `ColumnMetaData` spec and is populated when a bloom filter is written. The file size delta between the two files matches the expected SBBF sizing for `ndv=10_000, fpp=0.05` confirming the bloom filter data is physically present in the file.

The `ColumnChunkMetaData` C++ class exposes `bloom_filter_offset()` — it would be useful to have this surfaced in the Python `to_dict()` output so users can programmatically verify bloom filter presence without resorting to file size comparison.

Tested variants of `bloom_filter_options` and their observed file size deltas confirming bloom filters are written in all cases:

```
bool_true   → ndv=1,048,576 (default) → +1,048,603 bytes
empty_dict  → ndv=1,048,576 (default) → +1,048,603 bytes  
fpp_only    → ndv=1,048,576 (default) → +1,048,603 bytes
ndv_only    → ndv=10,000              → +16,409 bytes
fpp_and_ndv → ndv=10,000              → +16,409 bytes
tight_fpp   → fpp=0.001, ndv=10,000  → +32,793 bytes
both_cols   → 2 columns, default ndv  → +2,097,207 bytes (~2×)
```

Related: GH-49376 (added `bloom_filter_options` to `write_table` in 24.0.0)

---

You can file this at `https://github.com/apache/arrow/issues/new` with component tags `Parquet` and `Python`. The reproduction is clean and self-contained which gives it a good chance of being picked up quickly.