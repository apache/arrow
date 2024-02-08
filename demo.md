```python
import pyarrow as pa
import nanoarrow as na
chunked = pa.chunked_array([pa.array([0, 1, 2]), pa.array([3, 4, 5])])
[na.c_array_view(item) for item in na.c_array_stream(chunked)]
```




    [<nanoarrow.c_lib.CArrayView>
     - storage_type: 'int64'
     - length: 3
     - offset: 0
     - null_count: 0
     - buffers[2]:
       - <bool validity[0 b] >
       - <int64 data[24 b] 0 1 2>
     - dictionary: NULL
     - children[0]:,
     <nanoarrow.c_lib.CArrayView>
     - storage_type: 'int64'
     - length: 3
     - offset: 0
     - null_count: 0
     - buffers[2]:
       - <bool validity[0 b] >
       - <int64 data[24 b] 3 4 5>
     - dictionary: NULL
     - children[0]:]




```python
stream_capsule = chunked.__arrow_c_stream__()
chunked2 = chunked._import_from_c_capsule(stream_capsule)
chunked2
```




    <pyarrow.lib.ChunkedArray object at 0x105bb70b0>
    [
      [
        0,
        1,
        2
      ],
      [
        3,
        4,
        5
      ]
    ]


