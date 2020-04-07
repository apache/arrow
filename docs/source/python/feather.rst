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

.. _feather:

Feather File Format
===================

Feather is a portable file format for storing Arrow tables or data frames (from
languages like Python or R) that utilizes the :ref:`Arrow IPC format <ipc>`
internally. Feather was created early in the Arrow project as a proof of
concept for fast, language-agnostic data frame storage for Python (pandas) and
R. There are two file format versions for Feather:

* Version 2 (V2), the default version, which is exactly represented as the
  Arrow IPC file format on disk. V2 files support storing all Arrow data types
  as well as compression with LZ4 or ZSTD. V2 was first made available in
  Apache Arrow 0.17.0.
* Version 1 (V1), a legacy version available starting in 2016, replaced by
  V2. V1 files are distinct from Arrow IPC files and lack many features, such
  as the ability to store all Arrow data types. V1 files also lack compression
  support. We intend to maintain read support for V1 for the foreseeable
  future.

The ``pyarrow.feather`` module contains the read and write functions for the
format. :func:`~pyarrow.feather.write_feather` accepts either a
:class:`~pyarrow.Table` or ``pandas.DataFrame`` object:

.. code-block:: python

   import pyarrow.feather as feather
   feather.write_feather(df, '/path/to/file')

:func:`~pyarrow.feather.read_feather` reads a Feather file as a
``pandas.DataFrame``. :func:`~pyarrow.feather.read_table` reads a Feather file
as a :class:`~pyarrow.Table`. Internally, :func:`~pyarrow.feather.read_feather`
simply calls :func:`~pyarrow.feather.read_table` and the result is converted to
pandas:

.. code-block:: python

   # Result is pandas.DataFrame
   read_df = feather.read_feather('/path/to/file')

   # Result is pyarrow.Table
   read_arrow = feather.read_table('/path/to/file')

These functions can read and write with file-paths or file-like objects. For
example:

.. code-block:: python

   with open('/path/to/file', 'wb') as f:
       feather.write_feather(df, f)

   with open('/path/to/file', 'rb') as f:
       read_df = feather.read_feather(f)

A file input to ``read_feather`` must support seeking.

Using Compression
-----------------

As of Apache Arrow version 0.17.0, Feather V2 files (the default version)
support two fast compression libraries, LZ4 (using the frame format) and
ZSTD. LZ4 is used by default if it is available (which it should be if you
obtained pyarrow through a normal package manager):

.. code-block:: python

   # Uses LZ4 by default
   feather.write_feather(df, file_path)

   # Use LZ4 explicitly
   feather.write_feather(df, file_path, compression='lz4')

   # Use ZSTD
   feather.write_feather(df, file_path, compression='zstd')

   # Do not compress
   feather.write_feather(df, file_path, compression='uncompressed')

Note that the default LZ4 compression generally yields much smaller files
without sacrificing much read or write performance. In some instances,
LZ4-compressed files may be faster to read and write than uncompressed due to
reduced disk IO requirements.

Writing Version 1 (V1) Files
----------------------------

For compatibility with libraries without support for Version 2 files, you can
write the version 1 format by passing ``version=1`` to ``write_feather``. We
intend to maintain read support for V1 for the foreseeable future.
