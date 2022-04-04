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
.. _orc:

Reading and Writing the Apache ORC Format
=============================================

The `Apache ORC <http://orc.apache.org/>`_ project provides a
standardized open-source columnar storage format for use in data analysis
systems. It was created originally for use in `Apache Hadoop
<http://hadoop.apache.org/>`_ with systems like `Apache Drill
<http://drill.apache.org>`_, `Apache Hive <http://hive.apache.org>`_, `Apache
Impala (incubating) <http://impala.apache.org>`_, and `Apache Spark
<http://spark.apache.org>`_ adopting it as a shared standard for high
performance data IO.

Apache Arrow is an ideal in-memory representation layer for data that is being read
or written with ORC files.

Obtaining pyarrow with ORC Support
--------------------------------------

If you installed ``pyarrow`` with pip or conda, it should be built with ORC
support bundled::

   >>> from pyarrow import orc

If you are building ``pyarrow`` from source, you must use
``-DARROW_ORC=ON`` when compiling the C++ libraries and enable the ORC
extensions when building ``pyarrow``. See the :ref:`Python Development
<python-development>` page for more details.

Reading and Writing Single Files
--------------------------------

The functions :func:`~.orc.read_table` and :func:`~.orc.write_table`
read and write the :ref:`pyarrow.Table <data.table>` object, respectively.

Let's look at a simple table::

   >>> import numpy as np
   >>> import pyarrow as pa

   >>> table = pa.table(
   ...     {
   ...         'one': [-1, np.nan, 2.5],
   ...         'two': ['foo', 'bar', 'baz'],
   ...         'three': [True, False, True]
   ...     }
   ... )

We write this to ORC format with ``write_table``::

   >>> from pyarrow import orc
   >>> orc.write_table(table, 'example.orc')

This creates a single ORC file. In practice, an ORC dataset may consist
of many files in many directories. We can read a single file back with
``read_table``::

   >>> table2 = orc.read_table('example.orc')

You can pass a subset of columns to read, which can be much faster than reading
the whole file (due to the columnar layout)::

   >>> orc.read_table('example.orc', columns=['one', 'three'])
   pyarrow.Table
   one: double
   three: bool
   ----
   one: [[-1,nan,2.5]]
   three: [[true,false,true]]

We need not use a string to specify the origin of the file. It can be any of:

* A file path as a string
* A Python file object
* A pathlib.Path object
* A :ref:`NativeFile <io.native_file>` from PyArrow

In general, a Python file object will have the worst read performance, while a
string file path or an instance of :class:`~.NativeFile` (especially memory
maps) will perform the best.

We can also read partitioned datasets with multiple ORC files through the
:mod:`pyarrow.dataset <dataset>` interface.

.. seealso::
   :ref:`Documentation for datasets <dataset>`.

ORC file writing options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:func:`~pyarrow.orc.write_table()` has a number of options to
control various settings when writing an ORC file.

* ``file_version``, the ORC format version to use.  ``'0.11'`` ensures
  compatibility with older readers, while ``'0.12'`` is the newer one.
* ``stripe_size``, to control the approximate size of data within a column 
  stripe. This currently defaults to 64MB.

See the :func:`~pyarrow.orc.write_table()` docstring for more details.

Finer-grained Reading and Writing
---------------------------------

``read_table`` uses the :class:`~.ORCFile` class, which has other features::

   >>> orc_file = orc.ORCFile('example.orc')
   >>> orc_file.metadata

   -- metadata --
   >>> orc_file.schema
   one: double
   two: string
   three: bool
   >>> orc_file.nrows
   3

See the :class:`~pyarrow.orc.ORCFile` docstring for more details.

As you can learn more in the `Apache ORC format
<https://orc.apache.org/specification/>`_, an ORC file consists of
multiple stripes. ``read_table`` will read all of the stripes and
concatenate them into a single table. You can read individual stripes with
``read_stripe``::

   >>> orc_file.nstripes
   1
   >>> orc_file.read_stripe(0)
   pyarrow.RecordBatch
   one: double
   two: string
   three: bool

We can write an ORC file using ``ORCWriter``::

   >>> with orc.ORCWriter('example2.orc') as writer:
   ...     writer.write(table)

Compression
---------------------------------------------

The data pages within a column in a row group can be compressed after the
encoding passes (dictionary, RLE encoding). In PyArrow we don't use compression
by default, but Snappy, ZSTD, Gzip/Zlib, and LZ4 are also supported::

   >>> orc.write_table(table, where, compression='uncompressed')
   >>> orc.write_table(table, where, compression='gzip')
   >>> orc.write_table(table, where, compression='zstd')
   >>> orc.write_table(table, where, compression='snappy')

Snappy generally results in better performance, while Gzip may yield smaller
files.

Reading from cloud storage
--------------------------

In addition to local files, pyarrow supports other filesystems, such as cloud
filesystems, through the ``filesystem`` keyword::

   >>> from pyarrow import fs

   >>> s3  = fs.S3FileSystem(region="us-east-2")
   >>> table = orc.read_table("bucket/object/key/prefix", filesystem=s3)

.. seealso::
   :ref:`Documentation for filesystems <filesystem>`.
