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

Reading Single Parquet Files
----------------------------

To read a Parquet file into Arrow memory, you can use the following code
snippet. It will read the whole Parquet file into memory as an
:class:`~pyarrow.Table`.

.. code-block:: python

    import pyarrow.parquet as pq

    table = pq.read_table('<filename>')

As DataFrames stored as Parquet are often stored in multiple files, a
convenience method :meth:`~pyarrow.parquet.read_multiple_files` is provided.

If you already have the Parquet available in memory or get it via non-file
source, you can utilize :class:`pyarrow.BufferReader` to read it from
memory. As input to the :class:`~pyarrow.BufferReader` you can either supply
a Python ``bytes`` object or a :class:`pyarrow.Buffer`.

.. code:: python

    import pyarrow.io as paio
    import pyarrow.parquet as pq

    buf = ... # either bytes or paio.Buffer
    reader = paio.BufferReader(buf)
    table = pq.read_table(reader)

Reading Multiples Files and Partitioned Datasets
------------------------------------------------

Writing Parquet
---------------

Given an instance of :class:`pyarrow.table.Table`, the most simple way to
persist it to Parquet is by using the :meth:`pyarrow.parquet.write_table`
method.

.. code-block:: python

    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table(..)
    pq.write_table(table, '<filename>')

By default this will write the Table as a single RowGroup using ``DICTIONARY``
encoding. To increase the potential of parallelism a query engine can process
a Parquet file, set the ``chunk_size`` to a fraction of the total number of rows.

If you also want to compress the columns, you can select a compression
method using the ``compression`` argument. Typically, ``GZIP`` is the choice if
you want to minimize size and ``SNAPPY`` for performance.

Instead of writing to a file, you can also write to Python ``bytes`` by
utilizing an :class:`pyarrow.io.InMemoryOutputStream()`:

.. code:: python

    import pyarrow.io as paio
    import pyarrow.parquet as pq

    table = ...
    output = paio.InMemoryOutputStream()
    pq.write_table(table, output)
    pybytes = output.get_result().to_pybytes()
