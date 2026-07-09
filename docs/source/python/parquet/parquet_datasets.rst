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
.. _parquet_datasets:


Partitioned Datasets (Multiple Files)
=====================================

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

   >>> import pyarrow as pa
   >>> import pyarrow.parquet as pq

   >>> table = pa.table({'one': [-1, None, 2.5],
   ...                   'two': ['foo', 'bar', 'baz'],
   ...                   'three': [True, False, True]})
   ...

   >>> # Local dataset write
   >>> pq.write_to_dataset(table, root_path='dataset_name',
   ...                     partition_cols=['one', 'two'])

The root path in this case specifies the parent directory to which data will be
saved. The partition columns are the column names by which to partition the
dataset. Columns are partitioned in the order they are given. The partition
splits are determined by the unique values in the partition columns.

To use another filesystem you only need to add the filesystem parameter, the
individual table writes are wrapped using ``with`` statements so the
``pq.write_to_dataset`` function does not need to be.

.. code-block:: python

   >>> # Remote file-system example
   >>> from pyarrow.fs import HadoopFileSystem  # doctest: +SKIP
   >>> fs = HadoopFileSystem(host, port, user=user, kerb_ticket=ticket_cache_path)  # doctest: +SKIP
   >>> pq.write_to_dataset(table, root_path='dataset_name',  # doctest: +SKIP
   ...                     partition_cols=['one', 'two'], filesystem=fs)

Compatibility Note: if using ``pq.write_to_dataset`` to create a table that
will then be used by HIVE then partition column values must be compatible with
the allowed character set of the HIVE version you are running.

Writing ``_metadata`` and ``_common_metadata`` files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some processing frameworks such as Spark or Dask (optionally) use ``_metadata``
and ``_common_metadata`` files with partitioned datasets.

Those files include information about the schema of the full dataset (for
``_common_metadata``) and potentially all row group metadata of all files in the
partitioned dataset as well (for ``_metadata``). The actual files are
metadata-only Parquet files. Note this is not a Parquet standard, but a
convention set in practice by those frameworks.

Using those files can give a more efficient creation of a parquet Dataset,
since it can use the stored schema and file paths of all row groups,
instead of inferring the schema and crawling the directories for all Parquet
files (this is especially the case for filesystems where accessing files
is expensive).

The :func:`~pyarrow.parquet.write_to_dataset` function does not automatically
write such metadata files, but you can use it to gather the metadata and
combine and write them manually:

.. code-block:: python

   >>> # Write a dataset and collect metadata information of all written files
   >>> metadata_collector = []
   >>> root_path = "dataset_name_1"
   >>> pq.write_to_dataset(table, root_path, metadata_collector=metadata_collector)

   >>> # Write the ``_common_metadata`` parquet file without row groups statistics
   >>> pq.write_metadata(table.schema, root_path + '/_common_metadata')

   >>> # Write the ``_metadata`` parquet file with row groups statistics of all files
   >>> pq.write_metadata(
   ...     table.schema, root_path + '/_metadata',
   ...     metadata_collector=metadata_collector
   ... )

When not using the :func:`~pyarrow.parquet.write_to_dataset` function, but
writing the individual files of the partitioned dataset using
:func:`~pyarrow.parquet.write_table` or :class:`~pyarrow.parquet.ParquetWriter`,
the ``metadata_collector`` keyword can also be used to collect the FileMetaData
of the written files. In this case, you need to ensure to set the file path
contained in the row group metadata yourself before combining the metadata, and
the schemas of all different files and collected FileMetaData objects should be
the same:

.. code-block:: python

   >>> import os
   >>> os.mkdir("year=2017")

   >>> metadata_collector = []
   >>> pq.write_table(
   ...     table, "year=2017/data1.parquet",
   ...     metadata_collector=metadata_collector
   ... )

   >>> # set the file path relative to the root of the partitioned dataset
   >>> metadata_collector[-1].set_file_path("year=2017/data1.parquet")

   >>> # combine and write the metadata
   >>> metadata = metadata_collector[0]
   >>> for _meta in metadata_collector[1:]:
   ...     metadata.append_row_groups(_meta)
   >>> metadata.write_metadata_file("_metadata")

   >>> # or use pq.write_metadata to combine and write in a single step
   >>> pq.write_metadata(
   ...     table.schema, "_metadata",
   ...     metadata_collector=metadata_collector
   ... )

   >>> pq.read_metadata("_metadata")
   <pyarrow._parquet.FileMetaData object at ...>
     created_by: parquet-cpp-arrow version ...
     num_columns: 3
     num_rows: 3
     num_row_groups: 1
     format_version: 2.6
     serialized_size: ...

Reading from Partitioned Datasets
---------------------------------

The :class:`~.ParquetDataset` class accepts either a directory name or a list
of file paths, and can discover and infer some common partition structures,
such as those produced by Hive:

.. code-block:: python

   >>> dataset = pq.ParquetDataset('dataset_name/')
   >>> table = dataset.read()
   >>> table
   pyarrow.Table
   three: bool
   one: dictionary<values=string, indices=int32, ordered=0>
   two: dictionary<values=string, indices=int32, ordered=0>
   ----
   three: [[true],[true],[false]]
   one: [  -- dictionary:
   ["-1","2.5"]  -- indices:
   [0],  -- dictionary:
   ["-1","2.5"]  -- indices:
   [1],  -- dictionary:
   [null]  -- indices:
   [0]]
   two: [  -- dictionary:
   ["foo","baz","bar"]  -- indices:
   [0],  -- dictionary:
   ["foo","baz","bar"]  -- indices:
   [1],  -- dictionary:
   ["foo","baz","bar"]  -- indices:
   [2]]

You can also use the convenience function ``read_table`` exposed by
``pyarrow.parquet`` that avoids the need for an additional Dataset object
creation step.

.. code-block:: python

   >>> table = pq.read_table('dataset_name')

Note: the partition columns in the original table will have their types
converted to Arrow dictionary types (pandas categorical) on load. Ordering of
partition columns is not preserved through the save/load process. If reading
from a remote filesystem into a pandas dataframe you may need to run
``sort_index`` to maintain row ordering (as long as the ``preserve_index``
option was enabled on write).

Other features:

- Filtering on all columns (using row group statistics) instead of only on
  the partition keys.
- Fine-grained partitioning: support for a directory partitioning scheme
  in addition to the Hive-like partitioning (e.g. "/2019/11/15/" instead of
  "/year=2019/month=11/day=15/"), and the ability to specify a schema for
  the partition keys.

.. note::
    The partition keys need to be explicitly included in the ``columns``
    keyword when you want to include them in the result while reading a
    subset of the columns.

.. note::

   When passing a single file path to :func:`~pyarrow.parquet.read_table`
   or :class:`~pyarrow.parquet.ParquetDataset`, partition columns are not
   inferred from the file path, even if the path contains Hive-like segments.
   To get partition columns, pass the parent directory instead:

   .. code-block:: python

      >>> # Doesn't include 'year' as a column
      >>> pq.read_table('dataset_name/year=2017/data1.parquet')  # doctest: +SKIP

      >>> # Includes 'year' as a partition column
      >>> pq.read_table('dataset_name/')  # doctest: +SKIP
