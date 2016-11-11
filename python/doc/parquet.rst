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

Reading/Writing Parquet files
=============================

If you have built ``pyarrow`` with Parquet support, i.e. ``parquet-cpp`` was
found during the build, you can read files in the Parquet format to/from Arrow
memory structures. The Parquet support code is located in the
:mod:`pyarrow.parquet` module and your package needs to be built with the
``--with-parquet`` flag for ``build_ext``.

Reading Parquet
---------------

To read a Parquet file into Arrow memory, you can use the following code
snippet. It will read the whole Parquet file into memory as an
:class:`pyarrow.table.Table`.

.. code-block:: python

    import pyarrow
    import pyarrow.parquet

    A = pyarrow

    table = A.parquet.read_table('<filename>')

Writing Parquet
---------------

Given an instance of :class:`pyarrow.table.Table`, the most simple way to
persist it to Parquet is by using the :meth:`pyarrow.parquet.write_table`
method.

.. code-block:: python

    import pyarrow
    import pyarrow.parquet

    A = pyarrow

    table = A.Table(..)
    A.parquet.write_table(table, '<filename>')

By default this will write the Table as a single RowGroup using ``DICTIONARY``
encoding. To increase the potential of parallelism a query engine can process
a Parquet file, set the ``chunk_size`` to a fraction of the total number of rows.

If you also want to compress the columns, you can select a compression
method using the ``compression`` argument. Typically, ``GZIP`` is the choice if
you want to minimize size and ``SNAPPY`` for performance.
