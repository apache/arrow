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
.. _parquet:

Reading and Writing the Apache Parquet Format
=============================================

The `Apache Parquet <http://parquet.apache.org/>`_ project provides a
standardized open-source columnar storage format for use in data analysis
systems. It was created originally for use in `Apache Hadoop
<http://hadoop.apache.org/>`_ with systems like `Apache Drill
<http://drill.apache.org>`_, `Apache Hive <http://hive.apache.org>`_, `Apache
Impala (incubating) <http://impala.apache.org>`_, and `Apache Spark
<http://spark.apache.org>`_ adopting it as a shared standard for high
performance data IO.

Apache Arrow is an ideal in-memory transport layer for data that is being read
or written with Parquet files. We have been concurrently developing the `C++
implementation of Apache Parquet <http://github.com/apache/parquet-cpp>`_,
which includes a native, multithreaded C++ adapter to and from in-memory Arrow
data. PyArrow includes Python bindings to this code, which thus enables reading
and writing Parquet files with pandas as well.

Obtaining pyarrow with Parquet Support
--------------------------------------

If you installed ``pyarrow`` with pip or conda, it should be built with Parquet
support bundled:

.. ipython:: python

   import pyarrow.parquet as pq

If you are building ``pyarrow`` from source, you must use
``-DARROW_PARQUET=ON`` when compiling the C++ libraries and enable the Parquet
extensions when building ``pyarrow``. See the :ref:`Python Development
<python-development>` page for more details.

Reading and Writing Single Files
--------------------------------

The functions :func:`~.parquet.read_table` and :func:`~.parquet.write_table`
read and write the :ref:`pyarrow.Table <data.table>` objects, respectively.

Let's look at a simple table:

.. ipython:: python

   import numpy as np
   import pandas as pd
   import pyarrow as pa

   df = pd.DataFrame({'one': [-1, np.nan, 2.5],
                      'two': ['foo', 'bar', 'baz'],
                      'three': [True, False, True]},
                      index=list('abc'))
   table = pa.Table.from_pandas(df)

We write this to Parquet format with ``write_table``:

.. ipython:: python

   import pyarrow.parquet as pq
   pq.write_table(table, 'example.parquet')

This creates a single Parquet file. In practice, a Parquet dataset may consist
of many files in many directories. We can read a single file back with
``read_table``:

.. ipython:: python

   table2 = pq.read_table('example.parquet')
   table2.to_pandas()

You can pass a subset of columns to read, which can be much faster than reading
the whole file (due to the columnar layout):

.. ipython:: python

   pq.read_table('example.parquet', columns=['one', 'three'])

When reading a subset of columns from a file that used a Pandas dataframe as the
source, we use ``read_pandas`` to maintain any additional index column data:

.. ipython:: python

   pq.read_pandas('example.parquet', columns=['two']).to_pandas()

We need not use a string to specify the origin of the file. It can be any of:

* A file path as a string
* A :ref:`NativeFile <io.native_file>` from PyArrow
* A Python file object

In general, a Python file object will have the worst read performance, while a
string file path or an instance of :class:`~.NativeFile` (especially memory
maps) will perform the best.

Parquet file writing options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:func:`~pyarrow.parquet.write_table()` has a number of options to
control various settings when writing a Parquet file.

* ``version``, the Parquet format version to use, whether ``'1.0'``
  for compatibility with older readers, or ``'2.0'`` to unlock more
  recent features.
* ``data_page_size``, to control the approximate size of encoded data
  pages within a column chunk. This currently defaults to 1MB
* ``flavor``, to set compatibility options particular to a Parquet
  consumer like ``'spark'`` for Apache Spark.

There are some additional data type handling-specific options
described below.

Omitting the DataFrame index
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When using ``pa.Table.from_pandas`` to convert to an Arrow table, by default
one or more special columns are added to keep track of the index (row
labels). Storing the index takes extra space, so if your index is not valuable,
you may choose to omit it by passing ``preserve_index=False``

.. ipython:: python

   df = pd.DataFrame({'one': [-1, np.nan, 2.5],
                      'two': ['foo', 'bar', 'baz'],
                      'three': [True, False, True]},
                      index=list('abc'))
   df
   table = pa.Table.from_pandas(df, preserve_index=False)

Then we have:

.. ipython:: python

   pq.write_table(table, 'example_noindex.parquet')
   t = pq.read_table('example_noindex.parquet')
   t.to_pandas()

Here you see the index did not survive the round trip.

Finer-grained Reading and Writing
---------------------------------

``read_table`` uses the :class:`~.ParquetFile` class, which has other features:

.. ipython:: python

   parquet_file = pq.ParquetFile('example.parquet')
   parquet_file.metadata
   parquet_file.schema

As you can learn more in the `Apache Parquet format
<https://github.com/apache/parquet-format>`_, a Parquet file consists of
multiple row groups. ``read_table`` will read all of the row groups and
concatenate them into a single table. You can read individual row groups with
``read_row_group``:

.. ipython:: python

   parquet_file.num_row_groups
   parquet_file.read_row_group(0)

We can similarly write a Parquet file with multiple row groups by using
``ParquetWriter``:

.. ipython:: python

   writer = pq.ParquetWriter('example2.parquet', table.schema)
   for i in range(3):
       writer.write_table(table)
   writer.close()

   pf2 = pq.ParquetFile('example2.parquet')
   pf2.num_row_groups

Alternatively python ``with`` syntax can also be use:

.. ipython:: python

   with pq.ParquetWriter('example3.parquet', table.schema) as writer:
       for i in range(3):
           writer.write_table(table)

.. ipython:: python
   :suppress:

   !rm example.parquet
   !rm example_noindex.parquet
   !rm example2.parquet
   !rm example3.parquet

Data Type Handling
------------------

Reading types as DictionaryArray
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``read_dictionary`` option in ``read_table`` and ``ParquetDataset`` will
cause columns to be read as ``DictionaryArray``, which will become
``pandas.Categorical`` when converted to pandas. This option is only valid for
string and binary column types, and it can yield significantly lower memory use
and improved performance for columns with many repeated string values.

.. code-block:: python

   pq.read_table(table, where, read_dictionary=['binary_c0', 'stringb_c2'])

Storing timestamps
~~~~~~~~~~~~~~~~~~

Some Parquet readers may only support timestamps stored in millisecond
(``'ms'``) or microsecond (``'us'``) resolution. Since pandas uses nanoseconds
to represent timestamps, this can occasionally be a nuisance. We provide the
``coerce_timestamps`` option to allow you to select the desired resolution:

.. code-block:: python

   pq.write_table(table, where, coerce_timestamps='ms')

If a cast to a lower resolution value may result in a loss of data, by default
an exception will be raised. This can be suppressed by passing
``allow_truncated_timestamps=True``:

.. code-block:: python

   pq.write_table(table, where, coerce_timestamps='ms',
                  allow_truncated_timestamps=True)

Older Parquet implementations use ``INT96`` based storage of
timestamps, but this is now deprecated. This includes some older
versions of Apache Impala and Apache Spark. To write timestamps in
this format, set the ``use_deprecated_int96_timestamps`` option to
``True`` in ``write_table``.

.. code-block:: python

   pq.write_table(table, where, use_deprecated_int96_timestamps=True)

Compression, Encoding, and File Compatibility
---------------------------------------------

The most commonly used Parquet implementations use dictionary encoding when
writing files; if the dictionaries grow too large, then they "fall back" to
plain encoding. Whether dictionary encoding is used can be toggled using the
``use_dictionary`` option:

.. code-block:: python

   pq.write_table(table, where, use_dictionary=False)

The data pages within a column in a row group can be compressed after the
encoding passes (dictionary, RLE encoding). In PyArrow we use Snappy
compression by default, but Brotli, Gzip, and uncompressed are also supported:

.. code-block:: python

   pq.write_table(table, where, compression='snappy')
   pq.write_table(table, where, compression='gzip')
   pq.write_table(table, where, compression='brotli')
   pq.write_table(table, where, compression='none')

Snappy generally results in better performance, while Gzip may yield smaller
files.

These settings can also be set on a per-column basis:

.. code-block:: python

   pq.write_table(table, where, compression={'foo': 'snappy', 'bar': 'gzip'},
                  use_dictionary=['foo', 'bar'])

Partitioned Datasets (Multiple Files)
------------------------------------------------

Multiple Parquet files constitute a Parquet *dataset*. These may present in a
number of ways:

* A list of Parquet absolute file paths
* A directory name containing nested directories defining a partitioned dataset

A dataset partitioned by year and month may look like on disk:

.. code-block:: text

   dataset_name/
     year=2007/
       month=01/
          0.parq
          1.parq
          ...
       month=02/
          0.parq
          1.parq
          ...
       month=03/
       ...
     year=2008/
       month=01/
       ...
     ...

Writing to Partitioned Datasets
-------------------------------

You can write a partitioned dataset for any ``pyarrow`` file system that is a
file-store (e.g. local, HDFS, S3). The default behaviour when no filesystem is
added is to use the local filesystem.

.. code-block:: python

   # Local dataset write
   pq.write_to_dataset(table, root_path='dataset_name',
                       partition_cols=['one', 'two'])

The root path in this case specifies the parent directory to which data will be
saved. The partition columns are the column names by which to partition the
dataset. Columns are partitioned in the order they are given. The partition
splits are determined by the unique values in the partition columns.

To use another filesystem you only need to add the filesystem parameter, the
individual table writes are wrapped using ``with`` statements so the
``pq.write_to_dataset`` function does not need to be.

.. code-block:: python

   # Remote file-system example
   fs = pa.hdfs.connect(host, port, user=user, kerb_ticket=ticket_cache_path)
   pq.write_to_dataset(table, root_path='dataset_name',
                       partition_cols=['one', 'two'], filesystem=fs)

Compatibility Note: if using ``pq.write_to_dataset`` to create a table that
will then be used by HIVE then partition column values must be compatible with
the allowed character set of the HIVE version you are running.

Reading from Partitioned Datasets
------------------------------------------------

The :class:`~.ParquetDataset` class accepts either a directory name or a list
or file paths, and can discover and infer some common partition structures,
such as those produced by Hive:

.. code-block:: python

   dataset = pq.ParquetDataset('dataset_name/')
   table = dataset.read()

You can also use the convenience function ``read_table`` exposed by
``pyarrow.parquet`` that avoids the need for an additional Dataset object
creation step.

.. code-block:: python

   table = pq.read_table('dataset_name')

Note: the partition columns in the original table will have their types
converted to Arrow dictionary types (pandas categorical) on load. Ordering of
partition columns is not preserved through the save/load process. If reading
from a remote filesystem into a pandas dataframe you may need to run
``sort_index`` to maintain row ordering (as long as the ``preserve_index``
option was enabled on write).

Using with Spark
----------------

Spark places some constraints on the types of Parquet files it will read. The
option ``flavor='spark'`` will set these options automatically and also
sanitize field characters unsupported by Spark SQL.

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


Reading a Parquet File from Azure Blob storage
----------------------------------------------

The code below shows how to use Azure's storage sdk along with pyarrow to read
a parquet file into a Pandas dataframe.
This is suitable for executing inside a Jupyter notebook running on a Python 3
kernel.

Dependencies:

* python 3.6.2
* azure-storage 0.36.0
* pyarrow 0.8.0

.. code-block:: python

   import pyarrow.parquet as pq
   from io import BytesIO
   from azure.storage.blob import BlockBlobService

   account_name = '...'
   account_key = '...'
   container_name = '...'
   parquet_file = 'mysample.parquet'

   byte_stream = io.BytesIO()
   block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)
   try:
      block_blob_service.get_blob_to_stream(container_name=container_name, blob_name=parquet_file, stream=byte_stream)
      df = pq.read_table(source=byte_stream).to_pandas()
      # Do work on df ...
   finally:
      # Add finally block to ensure closure of the stream
      byte_stream.close()

Notes:

* The ``account_key`` can be found under ``Settings -> Access keys`` in the
  Microsoft Azure portal for a given container
* The code above works for a container with private access, Lease State =
  Available, Lease Status = Unlocked
* The parquet file was Blob Type = Block blob
