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

Apache Arrow is an ideal in-memory transport layer for data that is being read
or written with ORC files.

Obtaining pyarrow with ORC Support
--------------------------------------

If you installed ``pyarrow`` with pip or conda, it should be built with ORC
support bundled:

.. ipython:: python

   import pyarrow.orc as po

If you are building ``pyarrow`` from source, you must use
``-DARROW_ORC=ON`` when compiling the C++ libraries and enable the ORC
extensions when building ``pyarrow``. See the :ref:`Python Development
<python-development>` page for more details.

Reading and Writing Single Files
--------------------------------

The functions :func:`~.orc.read_table` and :func:`~.orc.write_table`
read and write the :ref:`pyarrow.Table <data.table>` object, respectively.

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

We write this to ORC format with ``write_table``:

.. ipython:: python

   import pyarrow.orc as po
   po.write_table(table, 'example.orc')

This creates a single ORC file. In practice, an ORC dataset may consist
of many files in many directories. We can read a single file back with
``read_table``:

.. ipython:: python

   table2 = po.read_table('example.orc')
   table2.to_pandas()

You can pass a subset of columns to read, which can be much faster than reading
the whole file (due to the columnar layout):

.. ipython:: python

   po.read_table('example.orc', columns=['one', 'three'])

We need not use a string to specify the origin of the file. It can be any of:

* A file path as a string
* A :ref:`NativeFile <io.native_file>` from PyArrow
* A Python file object

In general, a Python file object will have the worst read performance, while a
string file path or an instance of :class:`~.NativeFile` (especially memory
maps) will perform the best.

ORC file writing options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:func:`~pyarrow.orc.write_table()` has a number of options to
control various settings when writing an ORC file.

* ``file_version``, the ORC format version to use.  ``'0.11'`` ensures
  compatibility with older readers, while ``'0.12'`` is the newer one.
* ``stripe_size``, to control the approximate size of data within a column 
  stripe. This currently defaults to 64MB.

See the :func:`~pyarrow.orc.write_table()` docstring for more details.

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

   po.write_table(table, 'example_noindex.orc')
   t = po.read_table('example_noindex.orc')
   t.to_pandas()

Here you see the index did not survive the round trip.

Finer-grained Reading and Writing
---------------------------------

``read_table`` uses the :class:`~.ORCFile` class, which has other features:

.. ipython:: python

   orc_file = po.ORCFile('example.orc')
   orc_file.metadata
   orc_file.schema
   orc_file.nrows

See the :class:`~pyarrow.orc.ORCFile()` docstring for more details.

As you can learn more in the `Apache ORC format
<https://orc.apache.org/specification/>`_, an ORC file consists of
multiple stripes. ``read_table`` will read all of the stripes and
concatenate them into a single table. You can read individual stripes with
``read_stripe``:

.. ipython:: python

   orc_file.nstripes
   orc_file.read_stripe(0)

We can write an ORC file using ``ORCWriter``:

.. ipython:: python

   with po.ORCWriter('example2.orc') as writer:
      writer.write_table(table)

Compression
---------------------------------------------

The data pages within a column in a row group can be compressed after the
encoding passes (dictionary, RLE encoding). In PyArrow we use uncompressed
compression by default, but Snappy, ZSTD, Gzip/Zlib, and LZ4 are also supported:

.. code-block:: python

   po.write_table(table, where, compression='uncompressed')
   po.write_table(table, where, compression='gzip')
   po.write_table(table, where, compression='zstd')
   po.write_table(table, where, compression='snappy')

Snappy generally results in better performance, while Gzip may yield smaller
files.

Partitioned Datasets (Multiple Files)
------------------------------------------------

Multiple ORC files constitute a ORC *dataset*. These may present in a
number of ways:

* A list of ORC absolute file paths
* A directory name containing nested directories defining a partitioned dataset

A dataset partitioned by year and month may look like on disk:

.. code-block:: text

   dataset_name/
     year=2021/
       month=01/
          0.orc
          1.orc
          ...
       month=02/
          0.orc
          1.orc
          ...
       month=03/
       ...
     year=2022/
       month=01/
       ...
     ...


Reading from cloud storage
--------------------------

In addition to local files, pyarrow supports other filesystems, such as cloud
filesystems, through the ``filesystem`` keyword:

.. code-block:: python

    from pyarrow import fs

    s3  = fs.S3FileSystem(region="us-east-2")
    table = po.read_table("bucket/object/key/prefix", filesystem=s3)

Currently, :class:`HDFS <pyarrow.fs.HadoopFileSystem>` and
:class:`Amazon S3-compatible storage <pyarrow.fs.S3FileSystem>` are
supported. See the :ref:`filesystem` docs for more details. For those
built-in filesystems, the filesystem can also be inferred from the file path,
if specified as a URI:

.. code-block:: python

    table = po.read_table("s3://bucket/object/key/prefix")

Other filesystems can still be supported if there is an
`fsspec <https://filesystem-spec.readthedocs.io/en/latest/>`__-compatible
implementation available. See :ref:`filesystem-fsspec` for more details.
One example is Azure Blob storage, which can be interfaced through the
`adlfs <https://github.com/dask/adlfs>`__ package.

.. code-block:: python

    from adlfs import AzureBlobFileSystem

    abfs = AzureBlobFileSystem(account_name="XXXX", account_key="XXXX", container_name="XXXX")
    table = po.read_table("file.orc", filesystem=abfs)
