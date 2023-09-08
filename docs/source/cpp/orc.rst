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

.. cpp:namespace:: arrow::adapters::orc

=============================
Reading and Writing ORC files
=============================

The `Apache ORC <http://orc.apache.org/>`_ project provides a
standardized open-source columnar storage format for use in data analysis
systems. It was created originally for use in `Apache Hadoop
<http://hadoop.apache.org/>`_ with systems like `Apache Drill
<http://drill.apache.org>`_, `Apache Hive <http://hive.apache.org>`_, `Apache
Impala <http://impala.apache.org>`_, and `Apache Spark
<http://spark.apache.org>`_ adopting it as a shared standard for high
performance data IO.

Apache Arrow is an ideal in-memory representation layer for data that is being read
or written with ORC files.

.. seealso::
   :ref:`ORC reader/writer API reference <cpp-api-orc>`.

Supported ORC features
==========================

The ORC format has many features, and we support a subset of them.

Data types
----------
Here are a list of ORC types and mapped Arrow types.

+-------------------+-----------------------------------+-----------+
| Logical type      | Mapped Arrow type                 | Notes     |
+===================+===================================+===========+
| BOOLEAN           | Boolean                           |           |
+-------------------+-----------------------------------+-----------+
| BYTE              | Int8                              |           |
+-------------------+-----------------------------------+-----------+
| SHORT             | Int16                             |           |
+-------------------+-----------------------------------+-----------+
| INT               | Int32                             |           |
+-------------------+-----------------------------------+-----------+
| LONG              | Int64                             |           |
+-------------------+-----------------------------------+-----------+
| FLOAT             | Float32                           |           |
+-------------------+-----------------------------------+-----------+
| DOUBLE            | Float64                           |           |
+-------------------+-----------------------------------+-----------+
| STRING            | String/LargeString                | \(1)      |
+-------------------+-----------------------------------+-----------+
| BINARY            | Binary/LargeBinary/FixedSizeBinary| \(1)      |
+-------------------+-----------------------------------+-----------+
| TIMESTAMP         | Timestamp/Date64                  | \(1) \(2) |
+-------------------+-----------------------------------+-----------+
| TIMESTAMP_INSTANT | Timestamp                         | \(2)      |
+-------------------+-----------------------------------+-----------+
| LIST              | List/LargeList/FixedSizeList      | \(1)      |
+-------------------+-----------------------------------+-----------+
| MAP               | Map                               |           |
+-------------------+-----------------------------------+-----------+
| STRUCT            | Struct                            |           |
+-------------------+-----------------------------------+-----------+
| UNION             | SparseUnion/DenseUnion            | \(1)      |
+-------------------+-----------------------------------+-----------+
| DECIMAL           | Decimal128/Decimal256             | \(1)      |
+-------------------+-----------------------------------+-----------+
| DATE              | Date32                            |           |
+-------------------+-----------------------------------+-----------+
| VARCHAR           | String                            | \(3)      |
+-------------------+-----------------------------------+-----------+
| CHAR              | String                            | \(3)      |
+-------------------+-----------------------------------+-----------+

* \(1) On the read side the ORC type is read as the first corresponding Arrow type in the table.

* \(2) On the write side the ORC TIMESTAMP_INSTANT is used when timezone is provided, otherwise
  ORC TIMESTAMP is used. On the read side both ORC TIMESTAMP and TIMESTAMP_INSTANT types are read
  as the Arrow Timestamp type with :cpp:enumerator:`arrow::TimeUnit::NANO` and timezone is set to
  UTC for ORC TIMESTAMP_INSTANT type only.

* \(3) On the read side both ORC CHAR and VARCHAR types are read as the Arrow String type. ORC CHAR
  and VARCHAR types are not supported on the write side.

Compression
-----------

+-------------------+
| Compression codec |
+===================+
| SNAPPY            |
+-------------------+
| GZIP/ZLIB         |
+-------------------+
| LZ4               |
+-------------------+
| ZSTD              |
+-------------------+

*Unsupported compression codec:* LZO.


Reading ORC Files
=================

The :class:`ORCFileReader` class reads data for an entire
file or stripe into an :class:`::arrow::Table`.

ORCFileReader
-------------

The :class:`ORCFileReader` class requires a
:class:`::arrow::io::RandomAccessFile` instance representing the input
file.

.. code-block:: cpp

    #include <arrow/adapters/orc/adapter.h>

    {
        // ...
        arrow::Status st;
        arrow::MemoryPool* pool = default_memory_pool();
        std::shared_ptr<arrow::io::RandomAccessFile> input = ...;

        // Open ORC file reader
        auto maybe_reader = arrow::adapters::orc::ORCFileReader::Open(input, pool);
        if (!maybe_reader.ok()) {
            // Handle error instantiating file reader...
        }
        std::unique_ptr<arrow::adapters::orc::ORCFileReader> reader = maybe_reader.ValueOrDie();

        // Read entire file as a single Arrow table
        auto maybe_table = reader->Read();
        if (!maybe_table.ok()) {
            // Handle error reading ORC data...
        }
        std::shared_ptr<arrow::Table> table = maybe_table.ValueOrDie();
    }


Writing ORC Files
=================

ORCFileWriter
-------------

An ORC file is written to a :class:`~arrow::io::OutputStream`.

.. code-block:: cpp

    #include <arrow/adapters/orc/adapter.h>
    {
        // Oneshot write
        // ...
        std::shared_ptr<arrow::io::OutputStream> output = ...;
        auto writer_options = WriterOptions();
        auto maybe_writer = arrow::adapters::orc::ORCFileWriter::Open(output.get(), writer_options);
        if (!maybe_writer.ok()) {
           // Handle error instantiating file writer...
        }
        std::unique_ptr<arrow::adapters::orc::ORCFileWriter> writer = maybe_writer.ValueOrDie();
        if (!(writer->Write(*input_table)).ok()) {
            // Handle write error...
        }
        if (!(writer->Close()).ok()) {
            // Handle close error...
        }
    }
