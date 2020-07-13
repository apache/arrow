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

The Parquet C++ library is part of the Apache Arrow project and benefits
from tight integration with Arrow C++.

The :class:`arrow::FileReader` class reads data for an entire
file or row group into an :class:`::arrow::Table`.

The :func:`arrow::WriteTable` function writes an entire
:class:`::arrow::Table` to an output file.

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
==========

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

WriteTable
==========

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

StreamReader
============

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

StreamWriter
============

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
