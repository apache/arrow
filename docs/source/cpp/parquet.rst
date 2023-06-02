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

.. default-domain:: cpp
.. highlight:: cpp

.. cpp:namespace:: parquet

=================================
Reading and writing Parquet files
=================================

.. seealso::
   :ref:`Parquet reader and writer API reference <cpp-api-parquet>`.

The `Parquet format <https://parquet.apache.org/docs/>`__
is a space-efficient columnar storage format for complex data.  The Parquet
C++ implementation is part of the Apache Arrow project and benefits
from tight integration with the Arrow C++ classes and facilities.

Reading Parquet files
=====================

The :class:`arrow::FileReader` class reads data into Arrow Tables and Record
Batches.

The :class:`StreamReader` class allows for data to be read using a C++ input
stream approach to read fields column by column and row by row.  This approach
is offered for ease of use and type-safety.  It is of course also useful when
data must be streamed as files are read and written incrementally.

Please note that the performance of the :class:`StreamReader` will not
be as good due to the type checking and the fact that column values
are processed one at a time.

FileReader
----------

To read Parquet data into Arrow structures, use :class:`arrow::FileReader`.
To construct, it requires a :class:`::arrow::io::RandomAccessFile` instance 
representing the input file. To read the whole file at once, 
use :func:`arrow::FileReader::ReadTable`:

.. literalinclude:: ../../../cpp/examples/arrow/parquet_read_write.cc
   :language: cpp
   :start-after: arrow::Status ReadFullFile(
   :end-before: return arrow::Status::OK();
   :emphasize-lines: 9-10,14
   :dedent: 2

Finer-grained options are available through the
:class:`arrow::FileReaderBuilder` helper class, which accepts the :class:`ReaderProperties`
and :class:`ArrowReaderProperties` classes.

For reading as a stream of batches, use the :func:`arrow::FileReader::GetRecordBatchReader`
method to retrieve a :class:`arrow::RecordBatchReader`. It will use the batch 
size set in :class:`ArrowReaderProperties`.

.. literalinclude:: ../../../cpp/examples/arrow/parquet_read_write.cc
   :language: cpp
   :start-after: arrow::Status ReadInBatches(
   :end-before: return arrow::Status::OK();
   :emphasize-lines: 25
   :dedent: 2

.. seealso::

   For reading multi-file datasets or pushing down filters to prune row groups,
   see :ref:`Tabular Datasets<cpp-dataset>`.

Performance and Memory Efficiency
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For remote filesystems, use read coalescing (pre-buffering) to reduce number of API calls:

.. code-block:: cpp

   auto arrow_reader_props = parquet::ArrowReaderProperties();
   reader_properties.set_prebuffer(true);

The defaults are generally tuned towards good performance, but parallel column
decoding is off by default. Enable it in the constructor of :class:`ArrowReaderProperties`:

.. code-block:: cpp

   auto arrow_reader_props = parquet::ArrowReaderProperties(/*use_threads=*/true);

If memory efficiency is more important than performance, then:

#. Do *not* turn on read coalescing (pre-buffering) in :class:`parquet::ArrowReaderProperties`.
#. Read data in batches using :func:`arrow::FileReader::GetRecordBatchReader`.
#. Turn on ``enable_buffered_stream`` in :class:`parquet::ReaderProperties`.

In addition, if you know certain columns contain many repeated values, you can
read them as :term:`dictionary encoded<dictionary-encoding>` columns. This is 
enabled with the ``set_read_dictionary`` setting on :class:`ArrowReaderProperties`. 
If the files were written with Arrow C++ and the ``store_schema`` was activated,
then the original Arrow schema will be automatically read and will override this
setting.

StreamReader
------------

The :class:`StreamReader` allows for Parquet files to be read using
standard C++ input operators which ensures type-safety.

Please note that types must match the schema exactly i.e. if the
schema field is an unsigned 16-bit integer then you must supply a
``uint16_t`` type.

Exceptions are used to signal errors.  A :class:`ParquetException` is
thrown in the following circumstances:

* Attempt to read field by supplying the incorrect type.

* Attempt to read beyond end of row.

* Attempt to read beyond end of file.

.. code-block:: cpp

   #include "arrow/io/file.h"
   #include "parquet/stream_reader.h"

   {
      std::shared_ptr<arrow::io::ReadableFile> infile;

      PARQUET_ASSIGN_OR_THROW(
         infile,
         arrow::io::ReadableFile::Open("test.parquet"));

      parquet::StreamReader stream{parquet::ParquetFileReader::Open(infile)};

      std::string article;
      float price;
      uint32_t quantity;

      while ( !stream.eof() )
      {
         stream >> article >> price >> quantity >> parquet::EndRow;
         // ...
      }
   }

Writing Parquet files
=====================

WriteTable
----------

The :func:`arrow::WriteTable` function writes an entire
:class:`::arrow::Table` to an output file.

.. literalinclude:: ../../../cpp/examples/arrow/parquet_read_write.cc
   :language: cpp
   :start-after: arrow::Status WriteFullFile(
   :end-before: return arrow::Status::OK();
   :emphasize-lines: 19-21
   :dedent: 2

.. note::

   Column compression is off by default in C++. See :ref:`below <parquet-writer-properties>` 
   for how to choose a compression codec in the writer properties.

To write out data batch-by-batch, use :class:`arrow::FileWriter`.

.. literalinclude:: ../../../cpp/examples/arrow/parquet_read_write.cc
   :language: cpp
   :start-after: arrow::Status WriteInBatches(
   :end-before: return arrow::Status::OK();
   :emphasize-lines: 23-25,32,36
   :dedent: 2

StreamWriter
------------

The :class:`StreamWriter` allows for Parquet files to be written using
standard C++ output operators, similar to reading with the :class:`StreamReader`
class. This type-safe approach also ensures that rows are written without 
omitting fields and allows for new row groups to be created automatically 
(after certain volume of data) or explicitly by using the :type:`EndRowGroup` 
stream modifier.

Exceptions are used to signal errors.  A :class:`ParquetException` is
thrown in the following circumstances:

* Attempt to write a field using an incorrect type.

* Attempt to write too many fields in a row.

* Attempt to skip a required field.

.. code-block:: cpp

   #include "arrow/io/file.h"
   #include "parquet/stream_writer.h"

   {
      std::shared_ptr<arrow::io::FileOutputStream> outfile;

      PARQUET_ASSIGN_OR_THROW(
         outfile,
         arrow::io::FileOutputStream::Open("test.parquet"));

      parquet::WriterProperties::Builder builder;
      std::shared_ptr<parquet::schema::GroupNode> schema;

      // Set up builder with required compression type etc.
      // Define schema.
      // ...

      parquet::StreamWriter os{
         parquet::ParquetFileWriter::Open(outfile, schema, builder.build())};

      // Loop over some data structure which provides the required
      // fields to be written and write each row.
      for (const auto& a : getArticles())
      {
         os << a.name() << a.price() << a.quantity() << parquet::EndRow;
      }
   }

.. _parquet-writer-properties:

Writer properties
-----------------

To configure how Parquet files are written, use the :class:`WriterProperties::Builder`:

.. code-block:: cpp

   #include "parquet/arrow/writer.h"
   #include "arrow/util/type_fwd.h"

   using parquet::WriterProperties;
   using parquet::ParquetVersion;
   using parquet::ParquetDataPageVersion;
   using arrow::Compression;

   std::shared_ptr<WriterProperties> props = WriterProperties::Builder()
      .max_row_group_length(64 * 1024)
      .created_by("My Application")
      .version(ParquetVersion::PARQUET_2_6)
      .data_page_version(ParquetDataPageVersion::V2)
      .compression(Compression::SNAPPY)
      .build();

The ``max_row_group_length`` sets an upper bound on the number of rows per row
group that takes precedent over the ``chunk_size`` passed in the write methods.

You can set the version of Parquet to write with ``version``, which determines
which logical types are available. In addition, you can set the data page version
with ``data_page_version``. It's V1 by default; setting to V2 will allow more
optimal compression (skipping compressing pages where there isn't a space 
benefit), but not all readers support this data page version.

Compression is off by default, but to get the most out of Parquet, you should 
also choose a compression codec. You can choose one for the whole file or 
choose one for individual columns. If you choose a mix, the file-level option
will apply to columns that don't have a specific compression codec. See 
:class:`::arrow::Compression` for options.

Column data encodings can likewise be applied at the file-level or at the 
column level. By default, the writer will attempt to dictionary encode all 
supported columns, unless the dictionary grows too large. This behavior can
be changed at file-level or at the column level with ``disable_dictionary()``.
When not using dictionary encoding, it will fallback to the encoding set for 
the column or the overall file; by default ``Encoding::PLAIN``, but this can
be changed with ``encoding()``.

.. code-block:: cpp

   #include "parquet/arrow/writer.h"
   #include "arrow/util/type_fwd.h"

   using parquet::WriterProperties;
   using arrow::Compression;
   using parquet::Encoding;

   std::shared_ptr<WriterProperties> props = WriterProperties::Builder()
     .compression(Compression::SNAPPY)        // Fallback
     ->compression("colA", Compression::ZSTD) // Only applies to column "colA"
     ->encoding(Encoding::BIT_PACKED)         // Fallback
     ->encoding("colB", Encoding::RLE)        // Only applies to column "colB"
     ->disable_dictionary("colB")             // Never dictionary-encode column "colB"
     ->build();

Statistics are enabled by default for all columns. You can disable statistics for
all columns or specific columns using ``disable_statistics`` on the builder.
There is a ``max_statistics_size`` which limits the maximum number of bytes that
may be used for min and max values, useful for types like strings or binary blobs.

There are also Arrow-specific settings that can be configured with
:class:`parquet::ArrowWriterProperties`:

.. code-block:: cpp

   #include "parquet/arrow/writer.h"

   using parquet::ArrowWriterProperties;

   std::shared_ptr<ArrowWriterProperties> arrow_props = ArrowWriterProperties::Builder()
      .enable_deprecated_int96_timestamps() // default False
      ->store_schema() // default False
      ->build();

These options mostly dictate how Arrow types are converted to Parquet types.
Turning on ``store_schema`` will cause the writer to store the serialized Arrow
schema within the file metadata. Since there is no bijection between Parquet
schemas and Arrow schemas, storing the Arrow schema allows the Arrow reader
to more faithfully recreate the original data. This mapping from Parquet types
back to original Arrow types includes:

* Reading timestamps with original timezone information (Parquet does not
  support time zones);
* Reading Arrow types from their storage types (such as Duration from int64
  columns);
* Reading string and binary columns back into large variants with 64-bit offsets;
* Reading back columns as dictionary encoded (whether an Arrow column and
  the serialized Parquet version are dictionary encoded are independent).

Supported Parquet features
==========================

The Parquet format has many features, and Parquet C++ supports a subset of them.

Page types
----------

+-------------------+---------+
| Page type         | Notes   |
+===================+=========+
| DATA_PAGE         |         |
+-------------------+---------+
| DATA_PAGE_V2      |         |
+-------------------+---------+
| DICTIONARY_PAGE   |         |
+-------------------+---------+

*Unsupported page type:* INDEX_PAGE. When reading a Parquet file, pages of
this type are ignored.

Compression
-----------

+-------------------+---------+
| Compression codec | Notes   |
+===================+=========+
| SNAPPY            |         |
+-------------------+---------+
| GZIP              |         |
+-------------------+---------+
| BROTLI            |         |
+-------------------+---------+
| LZ4               | \(1)    |
+-------------------+---------+
| ZSTD              |         |
+-------------------+---------+

* \(1) On the read side, Parquet C++ is able to decompress both the regular
  LZ4 block format and the ad-hoc Hadoop LZ4 format used by the
  `reference Parquet implementation <https://github.com/apache/parquet-mr>`__.
  On the write side, Parquet C++ always generates the ad-hoc Hadoop LZ4 format.

*Unsupported compression codec:* LZO.

Encodings
---------

+--------------------------+----------+----------+---------+
| Encoding                 | Reading  | Writing  | Notes   |
+==========================+==========+==========+=========+
| PLAIN                    | ✓        | ✓        |         |
+--------------------------+----------+----------+---------+
| PLAIN_DICTIONARY         | ✓        | ✓        |         |
+--------------------------+----------+----------+---------+
| BIT_PACKED               | ✓        | ✓        | \(1)    |
+--------------------------+----------+----------+---------+
| RLE                      | ✓        | ✓        | \(1)    |
+--------------------------+----------+----------+---------+
| RLE_DICTIONARY           | ✓        | ✓        | \(2)    |
+--------------------------+----------+----------+---------+
| BYTE_STREAM_SPLIT        | ✓        | ✓        |         |
+--------------------------+----------+----------+---------+
| DELTA_BINARY_PACKED      | ✓        | ✓        |         |
+--------------------------+----------+----------+---------+
| DELTA_BYTE_ARRAY         | ✓        |          |         |
+--------------------------+----------+----------+---------+
| DELTA_LENGTH_BYTE_ARRAY  | ✓        | ✓        |         |
+--------------------------+----------+----------+---------+

* \(1) Only supported for encoding definition and repetition levels,
  and boolean values.

* \(2) On the write path, RLE_DICTIONARY is only enabled if Parquet format version
  2.4 or greater is selected in :func:`WriterProperties::version`.

Types
-----

Physical types
~~~~~~~~~~~~~~

+--------------------------+-------------------------+------------+
| Physical type            | Mapped Arrow type       | Notes      |
+==========================+=========================+============+
| BOOLEAN                  | Boolean                 |            |
+--------------------------+-------------------------+------------+
| INT32                    | Int32 / other           | \(1)       |
+--------------------------+-------------------------+------------+
| INT64                    | Int64 / other           | \(1)       |
+--------------------------+-------------------------+------------+
| INT96                    | Timestamp (nanoseconds) | \(2)       |
+--------------------------+-------------------------+------------+
| FLOAT                    | Float32                 |            |
+--------------------------+-------------------------+------------+
| DOUBLE                   | Float64                 |            |
+--------------------------+-------------------------+------------+
| BYTE_ARRAY               | Binary / other          | \(1) \(3)  |
+--------------------------+-------------------------+------------+
| FIXED_LENGTH_BYTE_ARRAY  | FixedSizeBinary / other | \(1)       |
+--------------------------+-------------------------+------------+

* \(1) Can be mapped to other Arrow types, depending on the logical type
  (see below).

* \(2) On the write side, :func:`ArrowWriterProperties::support_deprecated_int96_timestamps`
  must be enabled.

* \(3) On the write side, an Arrow LargeBinary can also mapped to BYTE_ARRAY.

Logical types
~~~~~~~~~~~~~

Specific logical types can override the default Arrow type mapping for a given
physical type.

+-------------------+-----------------------------+----------------------------+---------+
| Logical type      | Physical type               | Mapped Arrow type          | Notes   |
+===================+=============================+============================+=========+
| NULL              | Any                         | Null                       | \(1)    |
+-------------------+-----------------------------+----------------------------+---------+
| INT               | INT32                       | Int8 / UInt8 / Int16 /     |         |
|                   |                             | UInt16 / Int32 / UInt32    |         |
+-------------------+-----------------------------+----------------------------+---------+
| INT               | INT64                       | Int64 / UInt64             |         |
+-------------------+-----------------------------+----------------------------+---------+
| DECIMAL           | INT32 / INT64 / BYTE_ARRAY  | Decimal128 / Decimal256    | \(2)    |
|                   | / FIXED_LENGTH_BYTE_ARRAY   |                            |         |
+-------------------+-----------------------------+----------------------------+---------+
| DATE              | INT32                       | Date32                     | \(3)    |
+-------------------+-----------------------------+----------------------------+---------+
| TIME              | INT32                       | Time32 (milliseconds)      |         |
+-------------------+-----------------------------+----------------------------+---------+
| TIME              | INT64                       | Time64 (micro- or          |         |
|                   |                             | nanoseconds)               |         |
+-------------------+-----------------------------+----------------------------+---------+
| TIMESTAMP         | INT64                       | Timestamp (milli-, micro-  |         |
|                   |                             | or nanoseconds)            |         |
+-------------------+-----------------------------+----------------------------+---------+
| STRING            | BYTE_ARRAY                  | Utf8                       | \(4)    |
+-------------------+-----------------------------+----------------------------+---------+
| LIST              | Any                         | List                       | \(5)    |
+-------------------+-----------------------------+----------------------------+---------+
| MAP               | Any                         | Map                        | \(6)    |
+-------------------+-----------------------------+----------------------------+---------+

* \(1) On the write side, the Parquet physical type INT32 is generated.

* \(2) On the write side, a FIXED_LENGTH_BYTE_ARRAY is always emitted.

* \(3) On the write side, an Arrow Date64 is also mapped to a Parquet DATE INT32.

* \(4) On the write side, an Arrow LargeUtf8 is also mapped to a Parquet STRING.

* \(5) On the write side, an Arrow LargeList or FixedSizedList is also mapped to
  a Parquet LIST.

* \(6) On the read side, a key with multiple values does not get deduplicated,
  in contradiction with the
  `Parquet specification <https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps>`__.

*Unsupported logical types:* JSON, BSON, UUID.  If such a type is encountered
when reading a Parquet file, the default physical type mapping is used (for
example, a Parquet JSON column may be read as Arrow Binary or FixedSizeBinary).

Converted types
~~~~~~~~~~~~~~~

While converted types are deprecated in the Parquet format (they are superceded
by logical types), they are recognized and emitted by the Parquet C++
implementation so as to maximize compatibility with other Parquet
implementations.

Special cases
~~~~~~~~~~~~~

An Arrow Extension type is written out as its storage type.  It can still
be recreated at read time using Parquet metadata (see "Roundtripping Arrow
types" below).

An Arrow Dictionary type is written out as its value type.  It can still
be recreated at read time using Parquet metadata (see "Roundtripping Arrow
types" below).

Roundtripping Arrow types
~~~~~~~~~~~~~~~~~~~~~~~~~

While there is no bijection between Arrow types and Parquet types, it is
possible to serialize the Arrow schema as part of the Parquet file metadata.
This is enabled using :func:`ArrowWriterProperties::store_schema`.

On the read path, the serialized schema will be automatically recognized
and will recreate the original Arrow data, converting the Parquet data as
required (for example, a LargeList will be recreated from the Parquet LIST
type).

As an example, when serializing an Arrow LargeList to Parquet:

* The data is written out as a Parquet LIST

* When read back, the Parquet LIST data is decoded as an Arrow LargeList if
  :func:`ArrowWriterProperties::store_schema` was enabled when writing the file;
  otherwise, it is decoded as an Arrow List.

Serialization details
"""""""""""""""""""""

The Arrow schema is serialized as a :ref:`Arrow IPC <format-ipc>` schema message,
then base64-encoded and stored under the ``ARROW:schema`` metadata key in
the Parquet file metadata.

Limitations
~~~~~~~~~~~

Writing or reading back FixedSizedList data with null entries is not supported.

Encryption
----------

Parquet C++ implements all features specified in the
`encryption specification <https://github.com/apache/parquet-format/blob/master/Encryption.md>`__,
except for encryption of column index and bloom filter modules. 

More specifically, Parquet C++ supports:

* AES_GCM_V1 and AES_GCM_CTR_V1 encryption algorithms.
* AAD suffix for Footer, ColumnMetaData, Data Page, Dictionary Page,
  Data PageHeader, Dictionary PageHeader module types. Other module types
  (ColumnIndex, OffsetIndex, BloomFilter Header, BloomFilter Bitset) are not
  supported.
* EncryptionWithFooterKey and EncryptionWithColumnKey modes.
* Encrypted Footer and Plaintext Footer modes.

Miscellaneous
-------------

+--------------------------+----------+----------+---------+
| Feature                  | Reading  | Writing  | Notes   |
+==========================+==========+==========+=========+
| Column Index             | ✓        |          | \(1)    |
+--------------------------+----------+----------+---------+
| Offset Index             | ✓        |          | \(1)    |
+--------------------------+----------+----------+---------+
| Bloom Filter             | ✓        | ✓        | \(2)    |
+--------------------------+----------+----------+---------+
| CRC checksums            | ✓        | ✓        | \(3)    |
+--------------------------+----------+----------+---------+

* \(1) Access to the Column and Offset Index structures is provided, but
  data read APIs do not currently make any use of them.

* \(2) APIs are provided for creating, serializing and deserializing Bloom
  Filters, but they are not integrated into data read APIs.

* \(3) For now, only the checksums of V1 Data Pages and Dictionary Pages
  are computed.
