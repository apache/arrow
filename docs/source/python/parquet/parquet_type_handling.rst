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
.. _parquet_type_handling:


Data Type Handling
==================

Reading types as DictionaryArray
--------------------------------

The ``read_dictionary`` option in ``read_table`` and ``ParquetDataset`` will
cause columns to be read as ``DictionaryArray``, which will become
``pandas.Categorical`` when converted to pandas. This option is only valid for
string and binary column types, and it can yield significantly lower memory use
and improved performance for columns with many repeated string values.

.. code-block:: python

   >>> import pyarrow as pa
   >>> import pyarrow.parquet as pq

   >>> table = pa.table({'one': [-1, None, 2.5],
   ...                   'two': ['foo', 'bar', 'baz'],
   ...                   'three': [True, False, True]})
   ...
   >>> pq.write_table(table, 'example.parquet')

   >>> pq.read_table('example.parquet', read_dictionary=['two'])
   pyarrow.Table
   one: double
   two: dictionary<values=string, indices=int32, ordered=0>
   three: bool
   ----
   one: [[-1,null,2.5]]
   two: [  -- dictionary:
   ["foo","bar","baz"]  -- indices:
   [0,1,2]]
   three: [[true,false,true]]

Reading binary and list columns
-------------------------------

By default, Parquet ``BYTE_ARRAY`` columns are read as Arrow ``binary`` type
and Parquet ``LIST`` columns are read as Arrow ``list`` type, both of which use
32-bit offsets. For very large datasets this can overflow. The ``binary_type``
and ``list_type`` parameters let you choose a different Arrow type on read.

``binary_type`` accepts ``pa.binary()`` (default), ``pa.large_binary()``, or
``pa.binary_view()``:

.. code-block:: python

   >>> table = pa.table({'data': pa.array([b'hello', b'world'], pa.binary())})
   >>> pq.write_table(table, 'binary.parquet', store_schema=False)
   >>> pq.read_table('binary.parquet', binary_type=pa.large_binary()).schema
   data: large_binary

``list_type`` accepts ``pa.ListType`` or ``pa.LargeListType``:

.. code-block:: python

   >>> table = pa.table({'lists': pa.array([[1, 2], [3]], pa.list_(pa.int32()))})
   >>> pq.write_table(table, 'lists.parquet', store_schema=False)
   >>> pq.read_table('lists.parquet', list_type=pa.LargeListType).schema
   lists: large_list<element: int32>
     child 0, element: int32

.. note::
   Both settings are ignored when a serialized Arrow schema is present in the
   Parquet file metadata (i.e. when the parquet file was written with
   ``store_schema=True``).

Read in Arrow Extension Types
-----------------------------

Certain Parquet logical types (JSON, UUID, Geometry, Geography) are supported
and read as Arrow extension types by default (``arrow.json``, ``arrow.uuid``
and ``geoarrow.wkb`` respectively). This support is enabled via the
``arrow_extensions_enabled`` parameter and is used in :func:`~pyarrow.parquet.read_table`,
:class:`~pyarrow.parquet.ParquetFile`, :class:`~pyarrow.parquet.ParquetDataset`, and
:func:`~pyarrow.parquet.read_schema`.

To read these Parquet logical types as storage types (default behavior
until PyArrow version ``21.0.0``), set ``arrow_extensions_enabled=False``.

.. note::
   Reading GEOMETRY/GEOGRAPHY columns as ``geoarrow.wkb`` additionally
   requires the ``geoarrow.wkb`` extension type to be registered. For that
   you can install Python bindings for GeoArrow
   (`geoarrow-pyarrow <https://geoarrow.org/geoarrow-python/main/index.html#installation>`_)
   and import ``geoarrow.pyarrow`` module.

Storing timestamps
------------------

Some Parquet readers may only support timestamps stored in millisecond
(``'ms'``) or microsecond (``'us'``) resolution. Since pandas uses nanoseconds
to represent timestamps, this can occasionally be a nuisance. By default
(when writing version 1.0 Parquet files), the nanoseconds will be cast to
microseconds (``'us'``).

In addition, we provide the ``coerce_timestamps`` option to allow you to select
the desired resolution:

.. code-block:: python

   >>> pq.write_table(table, 'example.parquet', coerce_timestamps='ms')

If a cast to a lower resolution value may result in a loss of data, by default
an exception will be raised. This can be suppressed by passing
``allow_truncated_timestamps=True``:

.. code-block:: python

   >>> pq.write_table(table, 'example.parquet', coerce_timestamps='ms',
   ...                allow_truncated_timestamps=True)

Timestamps with nanoseconds can be stored without casting when using the
more recent Parquet format version 2.6:

.. code-block:: python

   >>> pq.write_table(table, 'example.parquet', version='2.6')

However, many Parquet readers do not yet support this newer format version, and
therefore the default is to write version 1.0 files. When compatibility across
different processing frameworks is required, it is recommended to use the
default version 1.0.

Older Parquet implementations use ``INT96`` based storage of
timestamps, but this is now deprecated. This includes some older
versions of Apache Impala and Apache Spark. To write timestamps in
this format, set the ``use_deprecated_int96_timestamps`` option to
``True`` in ``write_table``.

.. code-block:: python

   >>> pq.write_table(table, 'example.parquet', use_deprecated_int96_timestamps=True)
