.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

.. currentmodule:: pyarrow
.. _parquet_single_file:


Reading and Writing Single Files
================================

The functions :func:`~.parquet.read_table` and :func:`~.parquet.write_table`
read and write the :ref:`pyarrow.Table <data.table>` object, respectively.

Let's look at a simple table:

.. code-block:: python

   >>> import numpy as np
   >>> import pandas as pd
   >>> import pyarrow as pa
   >>> df = pd.DataFrame({'one': [-1, np.nan, 2.5],
   ...                    'two': ['foo', 'bar', 'baz'],
   ...                    'three': [True, False, True]},
   ...                    index=list('abc'))
   >>> table = pa.Table.from_pandas(df)

We write this to Parquet format with ``write_table``:

.. code-block:: python

   >>> import pyarrow.parquet as pq
   >>> pq.write_table(table, 'example.parquet')

This creates a single Parquet file. In practice, a Parquet dataset may consist
of many files in many directories. We can read a single file back with
``read_table``:

.. code-block:: python

   >>> table2 = pq.read_table('example.parquet')
   >>> table2.to_pandas()
      one  two  three
   a -1.0  foo   True
   b  NaN  bar  False
   c  2.5  baz   True

You can pass a subset of columns to read, which can be much faster than reading
the whole file (due to the columnar layout):

.. code-block:: python

   >>> pq.read_table('example.parquet', columns=['one', 'three'])
   pyarrow.Table
   one: double
   three: bool
   ----
   one: [[-1,null,2.5]]
   three: [[true,false,true]]

When reading a subset of columns from a file that used a Pandas dataframe as the
source, we use ``read_pandas`` to maintain any additional index column data:

.. code-block:: python

   >>> pq.read_pandas('example.parquet', columns=['two']).to_pandas()
      two
   a  foo
   b  bar
   c  baz

We do not need to use a string to specify the origin of the file. It can be any of:

* A file path as a string
* A :ref:`NativeFile <io.native_file>` from PyArrow
* A Python file object

In general, a Python file object will have the worst read performance, while a
string file path or an instance of :class:`~.NativeFile` (especially memory
maps) will perform the best.

.. _parquet_mmap:

Reading Parquet and Memory Mapping
----------------------------------

Because Parquet data needs to be decoded from the Parquet format
and compression, it can't be directly mapped from disk.
Thus the ``memory_map`` option might perform better on some systems
but won't help much with resident memory consumption.

.. code-block:: python

   >>> pq_array = pa.parquet.read_table(path, memory_map=True)  # doctest: +SKIP
   >>> print("RSS: {}MB".format(pa.total_allocated_bytes() >> 20))  # doctest: +SKIP
   RSS: 4299MB

   >>> pq_array = pa.parquet.read_table(path, memory_map=False)  # doctest: +SKIP
   >>> print("RSS: {}MB".format(pa.total_allocated_bytes() >> 20))  # doctest: +SKIP
   RSS: 4299MB

If you need to deal with Parquet data bigger than memory,
the :ref:`dataset` and partitioning is probably what you are looking for.

Parquet file writing options
----------------------------

:func:`~pyarrow.parquet.write_table()` has a number of options to
control various settings when writing a Parquet file.

* ``version``, the Parquet format version to use.  ``'1.0'`` ensures
  compatibility with older readers, while ``'2.4'`` and greater values
  enable more Parquet types and encodings.
* ``data_page_size``, to control the approximate size of encoded data
  pages within a column chunk. This currently defaults to 1MB.
* ``max_rows_per_page``, to cap the number of rows per data page within
  a column chunk (default 20000). Smaller values reduce memory usage
  during reads at the cost of more page metadata.
* ``flavor``, to set compatibility options particular to a Parquet
  consumer like ``'spark'`` for Apache Spark.
* ``store_decimal_as_integer``, to store decimals with precision 1–18
  as ``int32`` or ``int64`` instead of ``fixed_len_byte_array``.
  This produces more compact files but may not be supported by all readers.
* ``write_time_adjusted_to_utc``, to mark ``TIME`` columns as
  adjusted to UTC (``isAdjustedToUTC=True``). When ``False`` (the default),
  the time is treated as local/unknown timezone.
* ``write_page_index``, to write statistics to the page index
  instead of writing it to each data page header.
  Note that PyArrow does not yet use the page index on the read side.
* ``write_page_checksum``, to write a page checksum. Use with
  ``page_checksum_verification=True`` on read to detect data corruption.
* ``sorting_columns``, to record the sort order of the data in each
  row group's metadata. The writer does not sort the data nor does it verify
  that the data is sorted. Readers can use this metadata to optimize queries.

  Sort order is expressed as a sequence of :class:`~pyarrow.parquet.SortingColumn`
  objects.

See the :func:`~pyarrow.parquet.write_table()` docstring for more details.

There are some additional data type handling-specific options
described below.

Omitting the DataFrame index
----------------------------

When using ``pa.Table.from_pandas`` to convert to an Arrow table, by default
one or more special columns are added to keep track of the index (row
labels). Storing the index takes extra space, so if your index is not valuable,
you may choose to omit it by passing ``preserve_index=False``

.. code-block:: python

   >>> df = pd.DataFrame({'one': [-1, np.nan, 2.5],
   ...                    'two': ['foo', 'bar', 'baz'],
   ...                    'three': [True, False, True]},
   ...                    index=list('abc'))
   >>> table = pa.Table.from_pandas(df, preserve_index=False)

Then we have:

.. code-block:: python

   >>> pq.write_table(table, 'example_noindex.parquet')
   >>> t = pq.read_table('example_noindex.parquet')
   >>> t.to_pandas()
      one  two  three
   0 -1.0  foo   True
   1  NaN  bar  False
   2  2.5  baz   True

Here you see the index did not survive the round trip.

Finer-grained Reading and Writing
---------------------------------

``read_table`` uses the :class:`~.ParquetFile` class, which has other features:

.. code-block:: python

   >>> parquet_file = pq.ParquetFile('example.parquet')
   >>> parquet_file.metadata
   <pyarrow._parquet.FileMetaData object at ...>
     created_by: parquet-cpp-arrow version ...
     num_columns: 4
     num_rows: 3
     num_row_groups: 1
     format_version: 2.6
     serialized_size: ...
   >>> parquet_file.schema
   <pyarrow._parquet.ParquetSchema object at ...>
   required group field_id=-1 schema {
     optional double field_id=-1 one;
     optional binary field_id=-1 two (String);
     optional boolean field_id=-1 three;
     optional binary field_id=-1 __index_level_0__ (String);
   }
   <BLANKLINE>

As you can learn more in the `Apache Parquet format
<https://github.com/apache/parquet-format>`_, a Parquet file consists of
multiple row groups. ``read_table`` will read all of the row groups and
concatenate them into a single table. You can read individual row groups with
``read_row_group``:

.. code-block:: python

   >>> parquet_file.num_row_groups
   1
   >>> parquet_file.read_row_group(0)
   pyarrow.Table
   one: double
   two: large_string
   three: bool
   __index_level_0__: large_string
   ----
   one: [[-1,null,2.5]]
   two: [["foo","bar","baz"]]
   three: [[true,false,true]]
   __index_level_0__: [["a","b","c"]]

We can similarly write a Parquet file with multiple row groups by using
``ParquetWriter``:

.. code-block:: python

   >>> with pq.ParquetWriter('example2.parquet', table.schema) as writer:
   ...     for i in range(3):
   ...         writer.write_table(table)
   >>> pf2 = pq.ParquetFile('example2.parquet')
   >>> pf2.num_row_groups
   3

For memory-efficient reads of large files, :meth:`~.ParquetFile.iter_batches`
streams the file as a sequence of :class:`~pyarrow.RecordBatch` objects rather
than loading the entire file into a single table:

.. code-block:: python

   >>> parquet_file = pq.ParquetFile('example.parquet')
   >>> for batch in parquet_file.iter_batches(batch_size=2):
   ...     print(batch.num_rows)
   2
   1

The ``batch_size`` parameter controls the maximum number of rows per
batch. The ``row_groups`` and ``columns`` parameters allow limiting
which row groups and columns are read.

.. _inspecting_parquet_file_metadata:

Inspecting the Parquet File Metadata
------------------------------------

The ``FileMetaData`` of a Parquet file can be accessed through
:class:`~.ParquetFile` as shown above:

.. code-block:: python

   >>> parquet_file = pq.ParquetFile('example.parquet')
   >>> metadata = parquet_file.metadata
   >>> metadata
   <pyarrow._parquet.FileMetaData object at ...>
     created_by: parquet-cpp-arrow version ...
     num_columns: 4
     num_rows: 3
     num_row_groups: 1
     format_version: 2.6
     serialized_size: ...

or can also be read directly using :func:`~parquet.read_metadata`:

.. code-block:: python

   >>> metadata = pq.read_metadata('example.parquet')
   >>> metadata
   <pyarrow._parquet.FileMetaData object at ...>
     created_by: parquet-cpp-arrow version ...
     num_columns: 4
     num_rows: 3
     num_row_groups: 1
     format_version: 2.6
     serialized_size: ...

The returned ``FileMetaData`` object allows to inspect the
`Parquet file metadata <https://github.com/apache/parquet-format#metadata>`__,
such as the row groups and column chunk metadata and statistics:

.. code-block:: python

   >>> metadata.row_group(0)
   <pyarrow._parquet.RowGroupMetaData object at ...>
     num_columns: 4
     num_rows: 3
     total_byte_size: 290
     sorting_columns: ()
   >>> metadata.row_group(0).column(0)
   <pyarrow._parquet.ColumnChunkMetaData object at ...>
     file_offset: 0
     file_path:...
     physical_type: DOUBLE
     num_values: 3
     path_in_schema: one
     is_stats_set: True
     statistics:
       <pyarrow._parquet.Statistics object at ...>
         has_min_max: True
         min: -1.0
         max: 2.5
         null_count: 1
         distinct_count: None
         num_values: 2
         physical_type: DOUBLE
         logical_type: None
         converted_type (legacy): NONE
     geo_statistics:
       None
     compression: SNAPPY
     encodings: ('PLAIN', 'RLE', 'RLE_DICTIONARY')
     has_dictionary_page: True
     dictionary_page_offset: 4
     data_page_offset: 36
     total_compressed_size: 106
     total_uncompressed_size: 102
     bloom_filter_offset: None
     bloom_filter_length: None

.. _parquet_filtering:

Filtering / Predicate Pushdown
------------------------------

The ``filters`` parameter of :func:`~pyarrow.parquet.read_table` pushes
predicates into the reader, skipping entire row groups based on their
column statistics (min/max values stored in the row group metadata).
For large files with many row groups this can dramatically reduce the
data read.

Predicates can be expressed as a :class:`~pyarrow.compute.Expression`:

.. code-block:: python

   >>> import pyarrow.compute as pc
   >>> pq.read_table('example.parquet', filters=pc.field('three') == True)
   pyarrow.Table
   one: double
   two: large_string
   three: bool
   __index_level_0__: large_string
   ----
   one: [[-1,2.5]]
   two: [["foo","baz"]]
   three: [[true,true]]
   __index_level_0__: [["a","c"]]

Or in disjunctive normal form (DNF) as a list of ``(column, operator,
value)`` tuples:

.. code-block:: python

   >>> pq.read_table('example.parquet', filters=[('three', '==', True)])
   pyarrow.Table
   one: double
   two: large_string
   three: bool
   __index_level_0__: large_string
   ----
   one: [[-1,2.5]]
   two: [["foo","baz"]]
   three: [[true,true]]
   __index_level_0__: [["a","c"]]

Supported operators are ``=`` / ``==``, ``!=``, ``<``, ``>``, ``<=``,
``>=``, ``in``, and ``not in``. Multiple predicates in the inner list are
combined as AND; the outer list combines predicate groups as OR:

.. code-block:: python

   >>> filters = [
   ...     [('one', '>', 0), ('three', '==', True)],
   ...     [('two', 'in', ['foo', 'bar'])],
   ... ]
   >>> pq.read_table('example.parquet', filters=filters)
   pyarrow.Table
   one: double
   two: large_string
   three: bool
   __index_level_0__: large_string
   ----
   one: [[-1,null,2.5]]
   two: [["foo","bar","baz"]]
   three: [[true,false,true]]
   __index_level_0__: [["a","b","c"]]

Row group skipping based on column statistics is automatic when filters are
applied.

.. _parquet_bloom_filters:

Writing Bloom Filters
---------------------

Bloom filters are a probabilistic data structure stored per column per row
group that can answer, for a given value, either that value is
“definitely not present” or “probably present”. This makes them useful for
readers that support Bloom filter-based row group skipping.

The probability of false positives is configurable.

.. note::

   PyArrow reader does not currently use Bloom filters during
   filtering on read.

Bloom filters are enabled per column via the ``bloom_filter_options`` parameter:

.. code-block:: python

   >>> pq.write_table(table, 'bloom.parquet',
   ...                bloom_filter_options={'two': True})

Passing ``True`` uses the defaults: ``ndv=1048576`` (number of distinct values)
and ``fpp=0.05`` (false-positive probability).

Both can also be configured explicitly via a dictionary:

.. code-block:: python

   >>> pq.write_table(table, 'bloom.parquet',
   ...                bloom_filter_options={
   ...                    'two': {'ndv': 100, 'fpp': 0.01},
   ...                })

Recommended value for ``ndv`` is the number of rows. Lower values of ``fpp``
reduce false positives but require more space (space grows roughly proportional
to ``log(1/FPP)``). Recommended values are 0.1, 0.05, or 0.01.

Whether a Bloom filter was written can be confirmed with observing column chunk
metadata, see: :ref:`inspecting_parquet_file_metadata`.

Multithreaded Reads
-------------------

Each of the reading functions by default use multi-threading for reading
columns in parallel. Depending on the speed of IO
and how expensive it is to decode the columns in a particular file
(particularly with GZIP compression), this can yield significantly higher data
throughput.

This can be disabled by specifying ``use_threads=False``.

.. note::
   The number of threads to use concurrently is automatically inferred by Arrow
   and can be inspected using the :func:`~pyarrow.cpu_count()` function.

Compression, Encoding, and File Compatibility
---------------------------------------------

The most commonly used Parquet implementations use dictionary encoding when
writing files; if the dictionaries grow too large, then they "fall back" to
plain encoding. Whether dictionary encoding is used can be toggled using the
``use_dictionary`` option:

.. code-block:: python

   >>> pq.write_table(table, 'example.parquet', use_dictionary=False)

The data pages within a column in a row group can be compressed after the
encoding passes (dictionary, RLE encoding). In PyArrow we use Snappy
compression by default, but Brotli, Gzip, ZSTD, LZ4, and uncompressed are
also supported:

.. code-block:: python

   >>> pq.write_table(table, 'example.parquet', compression='snappy')
   >>> pq.write_table(table, 'example.parquet', compression='gzip')
   >>> pq.write_table(table, 'example.parquet', compression='brotli')
   >>> pq.write_table(table, 'example.parquet', compression='zstd')
   >>> pq.write_table(table, 'example.parquet', compression='lz4')
   >>> pq.write_table(table, 'example.parquet', compression='none')

Snappy generally results in better performance, while Gzip may yield smaller
files.

``'lz4_raw'`` is also accepted as an alias for ``'lz4'``. Both use the
LZ4_RAW codec as defined in the Parquet specification.

These settings can also be set on a per-column basis:

.. code-block:: python

   >>> pq.write_table(table, 'example.parquet', compression={'one': 'snappy', 'two': 'gzip'},
   ...                use_dictionary=['one', 'two'])

Using with Spark
----------------

Spark places some constraints on the types of Parquet files it will read. The
option ``flavor='spark'`` will set these options automatically and also
sanitize field characters unsupported by Spark SQL.

Reading from cloud storage
--------------------------

In addition to local files, pyarrow supports other filesystems, such as cloud
filesystems, through the ``filesystem`` keyword:

.. code-block:: python

   >>> from pyarrow import fs

   >>> s3  = fs.S3FileSystem(region="us-east-2")  # doctest: +SKIP
   >>> table = pq.read_table("bucket/object/key/prefix", filesystem=s3)  # doctest: +SKIP

Currently, :class:`HDFS <pyarrow.fs.HadoopFileSystem>` and
:class:`Amazon S3-compatible storage <pyarrow.fs.S3FileSystem>` are
supported. See the :ref:`filesystem` docs for more details. For those
built-in filesystems, the filesystem can also be inferred from the file path,
if specified as a URI:

.. code-block:: python

   >>> table = pq.read_table("s3://bucket/object/key/prefix")  # doctest: +SKIP

Other filesystems can still be supported if there is an
`fsspec <https://filesystem-spec.readthedocs.io/en/latest/>`__-compatible
implementation available. See :ref:`filesystem-fsspec` for more details.
One example is Azure Blob storage, which can be interfaced through the
`adlfs <https://github.com/dask/adlfs>`__ package.

.. code-block:: python

   >>> from adlfs import AzureBlobFileSystem  # doctest: +SKIP
   >>> abfs = AzureBlobFileSystem(account_name="XXXX", account_key="XXXX", container_name="XXXX")  # doctest: +SKIP
   >>> table = pq.read_table("file.parquet", filesystem=abfs)  # doctest: +SKIP

Content-Defined Chunking
------------------------

.. note::
   This feature is experimental and may change in future releases.

PyArrow introduces an experimental feature for optimizing Parquet files for content
addressable storage (CAS) systems using content-defined chunking (CDC). This feature
enables efficient deduplication of data across files, improving network transfers and
storage efficiency.

When enabled, data pages are written according to content-defined chunk boundaries,
determined by a rolling hash algorithm that identifies chunk boundaries based on the
actual content of the data. When data in a column is modified (e.g., inserted, deleted,
or updated), this approach minimizes the number of changed data pages.

The feature can be enabled by setting the ``use_content_defined_chunking`` parameter in
the Parquet writer. It accepts either a boolean or a dictionary for configuration:

- ``True``: Uses the default configuration with:
   - Minimum chunk size: 256 KiB
   - Maximum chunk size: 1024 KiB
   - Normalization level: 0

- ``dict``: Allows customization of the chunking parameters:
   - ``min_chunk_size``: Minimum chunk size in bytes (default: 256 KiB).
   - ``max_chunk_size``: Maximum chunk size in bytes (default: 1024 KiB).
   - ``norm_level``: Normalization level to adjust chunk size distribution (default: 0).

Note that the chunk size is calculated on the logical values before applying any encoding
or compression. The actual size of the data pages may vary based on the encoding and
compression used.

.. note::
   To make the most of this feature, you should ensure that Parquet write options
   remain consistent across writes and files.
   Using different write options (like compression, encoding, or row group size)
   for different files may prevent proper deduplication and lead to suboptimal
   storage efficiency.

.. code-block:: python

   >>> table = pa.Table.from_pandas(df)

   >>> # Enable content-defined chunking with default settings
   >>> pq.write_table(table, 'example.parquet', use_content_defined_chunking=True)

   >>> # Enable content-defined chunking with custom settings
   >>> pq.write_table(
   ...     table,
   ...     'example_custom.parquet',
   ...     use_content_defined_chunking={
   ...         'min_chunk_size': 128 * 1024,  # 128 KiB
   ...         'max_chunk_size': 512 * 1024,  # 512 KiB
   ...     }
   ... )
