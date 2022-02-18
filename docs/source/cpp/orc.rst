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

The `ORC format <https://orc.apache.org/docs/>`__
is a space-efficient columnar storage format for complex data. 
Arrow provides a fast ORC reader and a fast ORC writer allowing
ingestion of external data as Arrow tables.

.. seealso::
   :ref:`ORC reader/writer API reference <cpp-api-orc>`.

Supported ORC features
==========================

The ORC format has many features, and we support a subset of them.

Compression
-----------

+-------------------+---------+
| Compression codec | Notes   |
+===================+=========+
| SNAPPY            |         |
+-------------------+---------+
| GZIP/ZLIB         |         |
+-------------------+---------+
| LZ4               |         |
+-------------------+---------+
| ZSTD              |         |
+-------------------+---------+

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

.. note:: The writer does not yet support all Arrow types.
