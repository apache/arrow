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

.. currentmodule:: pyarrow.csv
.. _csv:

Reading and Writing CSV files
=============================

Arrow supports reading and writing columnar data from/to CSV files.
The features currently offered are the following:

* multi-threaded or single-threaded reading
* automatic decompression of input files (based on the filename extension,
  such as ``my_data.csv.gz``)
* fetching column names from the first row in the CSV file
* column-wise type inference and conversion to one of ``null``, ``int64``,
  ``float64``, ``date32``, ``time32[s]``, ``timestamp[s]``, ``timestamp[ns]``,
  ``string`` or ``binary`` data
* opportunistic dictionary encoding of ``string`` and ``binary`` columns
  (disabled by default)
* detecting various spellings of null values such as ``NaN`` or ``#N/A``
* writing CSV files with options to configure the exact output format

Usage
-----

CSV reading and writing functionality is available through the
:mod:`pyarrow.csv` module.  In many cases, you will simply call the
:func:`read_csv` function with the file path you want to read from::

   >>> from pyarrow import csv
   >>> fn = 'tips.csv.gz'
   >>> table = csv.read_csv(fn)
   >>> table
   pyarrow.Table
   total_bill: double
   tip: double
   sex: string
   smoker: string
   day: string
   time: string
   size: int64
   >>> len(table)
   244
   >>> df = table.to_pandas()
   >>> df.head()
      total_bill   tip     sex smoker  day    time  size
   0       16.99  1.01  Female     No  Sun  Dinner     2
   1       10.34  1.66    Male     No  Sun  Dinner     3
   2       21.01  3.50    Male     No  Sun  Dinner     3
   3       23.68  3.31    Male     No  Sun  Dinner     2
   4       24.59  3.61  Female     No  Sun  Dinner     4

To write CSV files, just call :func:`write_csv` with a
:class:`pyarrow.RecordBatch` or :class:`pyarrow.Table` and a path or
file-like object::

  >>> import pyarrow as pa
  >>> import pyarrow.csv as csv
  >>> csv.write_csv(table, "tips.csv")
  >>> with pa.CompressedOutputStream("tips.csv.gz", "gzip") as out:
  ...     csv.write_csv(table, out)

.. note:: The writer does not yet support all Arrow types.

Customized parsing
------------------

To alter the default parsing settings in case of reading CSV files with an
unusual structure, you should create a :class:`ParseOptions` instance
and pass it to :func:`read_csv`.

Customized conversion
---------------------

To alter how CSV data is converted to Arrow types and data, you should create
a :class:`ConvertOptions` instance and pass it to :func:`read_csv`::

   import pyarrow as pa
   import pyarrow.csv as csv

   table = csv.read_csv('tips.csv.gz', convert_options=pa.csv.ConvertOptions(
       column_types={
           'total_bill': pa.decimal128(precision=10, scale=2),
           'tip': pa.decimal128(precision=10, scale=2),
       }
   ))


Incremental reading
-------------------

For memory-constrained environments, it is also possible to read a CSV file
one batch at a time, using :func:`open_csv`.

There are a few caveats:

1. For now, the incremental reader is always single-threaded (regardless of
   :attr:`ReadOptions.use_threads`)

2. Type inference is done on the first block and types are frozen afterwards;
   to make sure the right data types are inferred, either set
   :attr:`ReadOptions.block_size` to a large enough value, or use
   :attr:`ConvertOptions.column_types` to set the desired data types explicitly.

Character encoding
------------------

By default, CSV files are expected to be encoded in UTF8.  Non-UTF8 data
is accepted for ``binary`` columns.  The encoding can be changed using
the :class:`ReadOptions` class.

Customized writing
------------------

To alter the default write settings in case of writing CSV files with
different conventions, you can create a :class:`WriteOptions` instance and
pass it to :func:`write_csv`::

  >>> import pyarrow as pa
  >>> import pyarrow.csv as csv
  >>> # Omit the header row (include_header=True is the default)
  >>> options = csv.WriteOptions(include_header=False)
  >>> csv.write_csv(table, "data.csv", options)

Incremental writing
-------------------

To write CSV files one batch at a time, create a :class:`CSVWriter`. This
requires the output (a path or file-like object), the schema of the data to
be written, and optionally write options as described above::

  >>> import pyarrow as pa
  >>> import pyarrow.csv as csv
  >>> with csv.CSVWriter("data.csv", table.schema) as writer:
  >>>     writer.write_table(table)

Performance
-----------

Due to the structure of CSV files, one cannot expect the same levels of
performance as when reading dedicated binary formats like
:ref:`Parquet <Parquet>`.  Nevertheless, Arrow strives to reduce the
overhead of reading CSV files.  A reasonable expectation is at least
100 MB/s per core on a performant desktop or laptop computer (measured
in source CSV bytes, not target Arrow data bytes).

Performance options can be controlled through the :class:`ReadOptions` class.
Multi-threaded reading is the default for highest performance, distributing
the workload efficiently over all available cores.

.. note::
   The number of concurrent threads is automatically inferred by Arrow.
   You can inspect and change it using the :func:`~pyarrow.cpu_count()`
   and :func:`~pyarrow.set_cpu_count()` functions, respectively.
