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

The `Parquet format <https://parquet.apache.org/documentation/latest/>`__
is a space-efficient columnar storage format for complex data.  The Parquet
C++ implementation is part of the Apache Arrow project and benefits
from tight integration with the Arrow C++ classes and facilities.

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
| DELTA_BINARY_PACKED      | ✓        |          |         |
+--------------------------+----------+----------+---------+
| DELTA_BYTE_ARRAY         | ✓        |          |         |
+--------------------------+----------+----------+---------+

* \(1) Only supported for encoding definition and repetition levels, not values.

* \(2) On the write path, RLE_DICTIONARY is only enabled if Parquet format version
  2.4 or greater is selected in :func:`WriterProperties::version`.

*Unsupported encoding:* DELTA_LENGTH_BYTE_ARRAY.

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


Reading Parquet files
=====================

The :class:`arrow::FileReader` class reads data for an entire
file or row group into an :class:`::arrow::Table`.

The :class:`StreamReader` and :class:`StreamWriter` classes allow for
data to be written using a C++ input/output streams approach to
read/write fields column by column and row by row.  This approach is
offered for ease of use and type-safety.  It is of course also useful
when data must be streamed as files are read and written
incrementally.

Please note that the performance of the :class:`StreamReader` and
:class:`StreamWriter` classes will not be as good due to the type
checking and the fact that column values are processed one at a time.

FileReader
----------

The Parquet :class:`arrow::FileReader` requires a
:class:`::arrow::io::RandomAccessFile` instance representing the input
file.

.. code-block:: cpp

   #include "arrow/parquet/arrow/reader.h"

   {
      // ...
      arrow::Status st;
      arrow::MemoryPool* pool = default_memory_pool();
      std::shared_ptr<arrow::io::RandomAccessFile> input = ...;

      // Open Parquet file reader
      std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
      st = parquet::arrow::OpenFile(input, pool, &arrow_reader);
      if (!st.ok()) {
         // Handle error instantiating file reader...
      }

      // Read entire file as a single Arrow table
      std::shared_ptr<arrow::Table> table;
      st = arrow_reader->ReadTable(&table);
      if (!st.ok()) {
         // Handle error reading Parquet data...
      }
   }

Finer-grained options are available through the
:class:`arrow::FileReaderBuilder` helper class.

.. TODO write section about performance and memory efficiency

StreamReader
------------

The :class:`StreamReader` allows for Parquet files to be read using
standard C++ input operators which ensures type-safety.

Please note that types must match the schema exactly i.e. if the
schema field is an unsigned 16-bit integer then you must supply a
uint16_t type.

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

      parquet::StreamReader os{parquet::ParquetFileReader::Open(infile)};

      std::string article;
      float price;
      uint32_t quantity;

      while ( !os.eof() )
      {
         os >> article >> price >> quantity >> parquet::EndRow;
         // ...
      }
   }

Writing Parquet files
=====================

WriteTable
----------

The :func:`arrow::WriteTable` function writes an entire
:class:`::arrow::Table` to an output file.

.. code-block:: cpp

   #include "parquet/arrow/writer.h"

   {
      std::shared_ptr<arrow::io::FileOutputStream> outfile;
      PARQUET_ASSIGN_OR_THROW(
         outfile,
         arrow::io::FileOutputStream::Open("test.parquet"));

      PARQUET_THROW_NOT_OK(
         parquet::arrow::WriteTable(table, arrow::default_memory_pool(), outfile, 3));
   }

StreamWriter
------------

The :class:`StreamWriter` allows for Parquet files to be written using
standard C++ output operators.  This type-safe approach also ensures
that rows are written without omitting fields and allows for new row
groups to be created automatically (after certain volume of data) or
explicitly by using the :type:`EndRowGroup` stream modifier.

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
