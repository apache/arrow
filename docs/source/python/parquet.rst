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
Impala <http://impala.apache.org>`_, and `Apache Spark
<http://spark.apache.org>`_ adopting it as a shared standard for high
performance data IO.

Apache Arrow is an ideal in-memory transport layer for data that is being read
or written with Parquet files. We have been concurrently developing the `C++
implementation of
Apache Parquet <https://github.com/apache/arrow/tree/main/cpp/tools/parquet>`_,
which includes a native, multithreaded C++ adapter to and from in-memory Arrow
data. PyArrow includes Python bindings to this code, which thus enables reading
and writing Parquet files with pandas as well.

Obtaining pyarrow with Parquet Support
--------------------------------------

If you installed ``pyarrow`` with pip or conda, it should be built with Parquet
support bundled:

.. ipython:: python

   import pyarrow.parquet as pq

If you are building ``pyarrow`` from source, you must use ``-DARROW_PARQUET=ON``
when compiling the C++ libraries and enable the Parquet extensions when
building ``pyarrow``. If you want to use Parquet Encryption, then you must
use ``-DPARQUET_REQUIRE_ENCRYPTION=ON`` too when compiling the C++ libraries.
See the :ref:`Python Development <python-development>` page for more details.

Reading and Writing Single Files
--------------------------------

The functions :func:`~.parquet.read_table` and :func:`~.parquet.write_table`
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

We do not need to use a string to specify the origin of the file. It can be any of:

* A file path as a string
* A :ref:`NativeFile <io.native_file>` from PyArrow
* A Python file object

In general, a Python file object will have the worst read performance, while a
string file path or an instance of :class:`~.NativeFile` (especially memory
maps) will perform the best.

.. _parquet_mmap:

Reading Parquet and Memory Mapping
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Because Parquet data needs to be decoded from the Parquet format
and compression, it can't be directly mapped from disk.
Thus the ``memory_map`` option might perform better on some systems
but won't help much with resident memory consumption.

.. code-block:: python

      >>> pq_array = pa.parquet.read_table("area1.parquet", memory_map=True)
      >>> print("RSS: {}MB".format(pa.total_allocated_bytes() >> 20))
      RSS: 4299MB

      >>> pq_array = pa.parquet.read_table("area1.parquet", memory_map=False)
      >>> print("RSS: {}MB".format(pa.total_allocated_bytes() >> 20))
      RSS: 4299MB

If you need to deal with Parquet data bigger than memory,
the :ref:`dataset` and partitioning is probably what you are looking for.

Parquet file writing options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:func:`~pyarrow.parquet.write_table()` has a number of options to
control various settings when writing a Parquet file.

* ``version``, the Parquet format version to use.  ``'1.0'`` ensures
  compatibility with older readers, while ``'2.4'`` and greater values
  enable more Parquet types and encodings.
* ``data_page_size``, to control the approximate size of encoded data
  pages within a column chunk. This currently defaults to 1MB.
* ``flavor``, to set compatibility options particular to a Parquet
  consumer like ``'spark'`` for Apache Spark.

See the :func:`~pyarrow.parquet.write_table()` docstring for more details.

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

   with pq.ParquetWriter('example2.parquet', table.schema) as writer:
      for i in range(3):
         writer.write_table(table)

   pf2 = pq.ParquetFile('example2.parquet')
   pf2.num_row_groups

Inspecting the Parquet File Metadata
------------------------------------

The ``FileMetaData`` of a Parquet file can be accessed through
:class:`~.ParquetFile` as shown above:

.. ipython:: python

   parquet_file = pq.ParquetFile('example.parquet')
   metadata = parquet_file.metadata

or can also be read directly using :func:`~parquet.read_metadata`:

.. ipython:: python

   metadata = pq.read_metadata('example.parquet')
   metadata

The returned ``FileMetaData`` object allows to inspect the
`Parquet file metadata <https://github.com/apache/parquet-format#metadata>`__,
such as the row groups and column chunk metadata and statistics:

.. ipython:: python

   metadata.row_group(0)
   metadata.row_group(0).column(0)

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
to represent timestamps, this can occasionally be a nuisance. By default
(when writing version 1.0 Parquet files), the nanoseconds will be cast to
microseconds ('us').

In addition, We provide the ``coerce_timestamps`` option to allow you to select
the desired resolution:

.. code-block:: python

   pq.write_table(table, where, coerce_timestamps='ms')

If a cast to a lower resolution value may result in a loss of data, by default
an exception will be raised. This can be suppressed by passing
``allow_truncated_timestamps=True``:

.. code-block:: python

   pq.write_table(table, where, coerce_timestamps='ms',
                  allow_truncated_timestamps=True)

Timestamps with nanoseconds can be stored without casting when using the
more recent Parquet format version 2.6:

.. code-block:: python

   pq.write_table(table, where, version='2.6')

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
compression by default, but Brotli, Gzip, ZSTD, LZ4, and uncompressed are
also supported:

.. code-block:: python

   pq.write_table(table, where, compression='snappy')
   pq.write_table(table, where, compression='gzip')
   pq.write_table(table, where, compression='brotli')
   pq.write_table(table, where, compression='zstd')
   pq.write_table(table, where, compression='lz4')
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
   from pyarrow.fs import HadoopFileSystem
   fs = HadoopFileSystem(host, port, user=user, kerb_ticket=ticket_cache_path)
   pq.write_to_dataset(table, root_path='dataset_name',
                       partition_cols=['one', 'two'], filesystem=fs)

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

   # Write a dataset and collect metadata information of all written files
   metadata_collector = []
   pq.write_to_dataset(table, root_path, metadata_collector=metadata_collector)

   # Write the ``_common_metadata`` parquet file without row groups statistics
   pq.write_metadata(table.schema, root_path / '_common_metadata')

   # Write the ``_metadata`` parquet file with row groups statistics of all files
   pq.write_metadata(
       table.schema, root_path / '_metadata',
       metadata_collector=metadata_collector
   )

When not using the :func:`~pyarrow.parquet.write_to_dataset` function, but
writing the individual files of the partitioned dataset using
:func:`~pyarrow.parquet.write_table` or :class:`~pyarrow.parquet.ParquetWriter`,
the ``metadata_collector`` keyword can also be used to collect the FileMetaData
of the written files. In this case, you need to ensure to set the file path
contained in the row group metadata yourself before combining the metadata, and
the schemas of all different files and collected FileMetaData objects should be
the same:

.. code-block:: python

   metadata_collector = []
   pq.write_table(
       table1, root_path / "year=2017/data1.parquet",
       metadata_collector=metadata_collector
   )

   # set the file path relative to the root of the partitioned dataset
   metadata_collector[-1].set_file_path("year=2017/data1.parquet")

   # combine and write the metadata
   metadata = metadata_collector[0]
   for _meta in metadata_collector[1:]:
       metadata.append_row_groups(_meta)
   metadata.write_metadata_file(root_path / "_metadata")

   # or use pq.write_metadata to combine and write in a single step
   pq.write_metadata(
       table1.schema, root_path / "_metadata",
       metadata_collector=metadata_collector
   )

Reading from Partitioned Datasets
------------------------------------------------

The :class:`~.ParquetDataset` class accepts either a directory name or a list
of file paths, and can discover and infer some common partition structures,
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

Other features:

- Filtering on all columns (using row group statistics) instead of only on
  the partition keys.
- Fine-grained partitioning: support for a directory partitioning scheme
  in addition to the Hive-like partitioning (e.g. "/2019/11/15/" instead of
  "/year=2019/month=11/day=15/"), and the ability to specify a schema for
  the partition keys.

Note:

- The partition keys need to be explicitly included in the ``columns``
  keyword when you want to include them in the result while reading a
  subset of the columns


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

Reading from cloud storage
--------------------------

In addition to local files, pyarrow supports other filesystems, such as cloud
filesystems, through the ``filesystem`` keyword:

.. code-block:: python

    from pyarrow import fs

    s3  = fs.S3FileSystem(region="us-east-2")
    table = pq.read_table("bucket/object/key/prefix", filesystem=s3)

Currently, :class:`HDFS <pyarrow.fs.HadoopFileSystem>` and
:class:`Amazon S3-compatible storage <pyarrow.fs.S3FileSystem>` are
supported. See the :ref:`filesystem` docs for more details. For those
built-in filesystems, the filesystem can also be inferred from the file path,
if specified as a URI:

.. code-block:: python

    table = pq.read_table("s3://bucket/object/key/prefix")

Other filesystems can still be supported if there is an
`fsspec <https://filesystem-spec.readthedocs.io/en/latest/>`__-compatible
implementation available. See :ref:`filesystem-fsspec` for more details.
One example is Azure Blob storage, which can be interfaced through the
`adlfs <https://github.com/dask/adlfs>`__ package.

.. code-block:: python

    from adlfs import AzureBlobFileSystem

    abfs = AzureBlobFileSystem(account_name="XXXX", account_key="XXXX", container_name="XXXX")
    table = pq.read_table("file.parquet", filesystem=abfs)

Parquet Modular Encryption (Columnar Encryption)
------------------------------------------------

Columnar encryption is supported for Parquet files in C++ starting from
Apache Arrow 4.0.0 and in PyArrow starting from Apache Arrow 6.0.0.

Parquet uses the envelope encryption practice, where file parts are encrypted
with "data encryption keys" (DEKs), and the DEKs are encrypted with "master
encryption keys" (MEKs). The DEKs are randomly generated by Parquet for each
encrypted file/column. The MEKs are generated, stored and managed in a Key
Management Service (KMS) of user’s choice.

Reading and writing encrypted Parquet files involves passing file encryption
and decryption properties to :class:`~pyarrow.parquet.ParquetWriter` and to
:class:`~.ParquetFile`, respectively.

Writing an encrypted Parquet file:

.. code-block:: python

   encryption_properties = crypto_factory.file_encryption_properties(
                                    kms_connection_config, encryption_config)
   with pq.ParquetWriter(filename, schema,
                        encryption_properties=encryption_properties) as writer:
      writer.write_table(table)

Reading an encrypted Parquet file:

.. code-block:: python

   decryption_properties = crypto_factory.file_decryption_properties(
                                                    kms_connection_config)
   parquet_file = pq.ParquetFile(filename,
                                 decryption_properties=decryption_properties)


In order to create the encryption and decryption properties, a
:class:`pyarrow.parquet.encryption.CryptoFactory` should be created and
initialized with KMS Client details, as described below.


KMS Client
~~~~~~~~~~

The master encryption keys should be kept and managed in a production-grade
Key Management System (KMS), deployed in the user's organization. Using Parquet
encryption requires implementation of a client class for the KMS server.
Any KmsClient implementation should implement the informal interface
defined by :class:`pyarrow.parquet.encryption.KmsClient` as following:

.. code-block:: python

   import pyarrow.parquet.encryption as pe

   class MyKmsClient(pe.KmsClient):

      """An example KmsClient implementation skeleton"""
      def __init__(self, kms_connection_configuration):
         pe.KmsClient.__init__(self)
         # Any KMS-specific initialization based on
         # kms_connection_configuration comes here

      def wrap_key(self, key_bytes, master_key_identifier):
         wrapped_key = ... # call KMS to wrap key_bytes with key specified by
                           # master_key_identifier
         return wrapped_key

      def unwrap_key(self, wrapped_key, master_key_identifier):
         key_bytes = ... # call KMS to unwrap wrapped_key with key specified by
                         # master_key_identifier
         return key_bytes

The concrete implementation will be loaded at runtime by a factory function
provided by the user. This factory function will be used to initialize the
:class:`pyarrow.parquet.encryption.CryptoFactory` for creating file encryption
and decryption properties.

For example, in order to use the ``MyKmsClient`` defined above:

.. code-block:: python

   def kms_client_factory(kms_connection_configuration):
      return MyKmsClient(kms_connection_configuration)

   crypto_factory = CryptoFactory(kms_client_factory)

An :download:`example <../../../python/examples/parquet_encryption/sample_vault_kms_client.py>`
of such a class for an open source
`KMS <https://www.vaultproject.io/api/secret/transit>`_ can be found in the Apache
Arrow GitHub repository. The production KMS client should be designed in
cooperation with an organization's security administrators, and built by
developers with experience in access control management. Once such a class is
created, it can be passed to applications via a factory method and leveraged
by general PyArrow users as shown in the encrypted parquet write/read sample
above.

KMS connection configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Configuration of connection to KMS (:class:`pyarrow.parquet.encryption.KmsConnectionConfig`
used when creating file encryption and decryption properties) includes the
following options:

* ``kms_instance_url``, URL of the KMS instance.
* ``kms_instance_id``, ID of the KMS instance that will be used for encryption
  (if multiple KMS instances are available).
* ``key_access_token``, authorization token that will be passed to KMS.
* ``custom_kms_conf``, a string dictionary with KMS-type-specific configuration.

Encryption configuration
~~~~~~~~~~~~~~~~~~~~~~~~

:class:`pyarrow.parquet.encryption.EncryptionConfiguration` (used when
creating file encryption properties) includes the following options:

* ``footer_key``, the ID of the master key for footer encryption/signing.
* ``column_keys``, which columns to encrypt with which key. Dictionary with
  master key IDs as the keys, and column name lists as the values,
  e.g. ``{key1: [col1, col2], key2: [col3]}`` .
* ``encryption_algorithm``, the Parquet encryption algorithm.
  Can be ``AES_GCM_V1`` (default) or ``AES_GCM_CTR_V1``.
* ``plaintext_footer``, whether to write the file footer in plain text (otherwise it is encrypted).
* ``double_wrapping``, whether to use double wrapping - where data encryption keys (DEKs)
  are encrypted with key encryption keys (KEKs), which in turn are encrypted
  with master encryption keys (MEKs). If set to ``false``, single wrapping is
  used - where DEKs are encrypted directly with MEKs.
* ``cache_lifetime``, the lifetime of cached entities (key encryption keys,
  local wrapping keys, KMS client objects) represented as a ``datetime.timedelta``.
* ``internal_key_material``, whether to store key material inside Parquet file footers;
  this mode doesn’t produce additional files. If set to ``false``, key material is
  stored in separate files in the same folder, which enables key rotation for
  immutable Parquet files.
* ``data_key_length_bits``, the length of data encryption keys (DEKs), randomly
  generated by Parquet key management tools. Can be 128, 192 or 256 bits.

.. note::
   When ``double_wrapping`` is true, Parquet implements a "double envelope
   encryption" mode that minimizes the interaction of the program with a KMS
   server. In this mode, the DEKs are encrypted with "key encryption keys"
   (KEKs, randomly generated by Parquet). The KEKs are encrypted with "master
   encryption keys" (MEKs) in the KMS; the result and the KEK itself are
   cached in the process memory.

An example encryption configuration:

.. code-block:: python

   encryption_config = pq.EncryptionConfiguration(
      footer_key="footer_key_name",
      column_keys={
         "column_key_name": ["Column1", "Column2"],
      },
   )

Decryption configuration
~~~~~~~~~~~~~~~~~~~~~~~~

:class:`pyarrow.parquet.encryption.DecryptionConfiguration` (used when creating
file decryption properties) is optional and it includes the following options:

* ``cache_lifetime``, the lifetime of cached entities (key encryption keys, local
  wrapping keys, KMS client objects) represented as a ``datetime.timedelta``.
