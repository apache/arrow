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

Obtaining PyArrow with Parquet Support
--------------------------------------

If you installed ``pyarrow`` with pip or conda, it should be built with Parquet
support bundled:

.. ipython:: python

   import pyarrow.parquet as pq

If you are building ``pyarrow`` from source, you must also build `parquet-cpp
<http://github.com/apache/parquet-cpp>`_ and enable the Parquet extensions when
building ``pyarrow``. See the :ref:`Development <development>` page for more
details.

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
                      'three': [True, False, True]})
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

We need not use a string to specify the origin of the file. It can be any of:

* A file path as a string
* A :ref:`NativeFile <io.native_file>` from PyArrow
* A Python file object

In general, a Python file object will have the worst read performance, while a
string file path or an instance of :class:`~.NativeFIle` (especially memory
maps) will perform the best.

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

.. ipython:: python
   :suppress:

   !rm example.parquet
   !rm example2.parquet

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

   pa.write_table(table, where, compression={'foo': 'snappy', 'bar': 'gzip'},
                  use_dictionary=['foo', 'bar'])

Reading Multiples Files and Partitioned Datasets
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

The :class:`~.ParquetDataset` class accepts either a directory name or a list
or file paths, and can discover and infer some common partition structures,
such as those produced by Hive:

.. code-block:: python

   dataset = pq.ParquetDataset('dataset_name/')
   table = dataset.read()

Multithreaded Reads
-------------------

Each of the reading functions have an ``nthreads`` argument which will read
columns with the indicated level of parallelism. Depending on the speed of IO
and how expensive it is to decode the columns in a particular file
(particularly with GZIP compression), this can yield significantly higher data
throughput:

.. code-block:: python

   pq.read_table(where, nthreads=4)
   pq.ParquetDataset(where).read(nthreads=4)
