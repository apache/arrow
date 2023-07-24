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

============
File Formats
============

.. _cpp-api-csv:

CSV reader
==========

.. doxygenstruct:: arrow::csv::ConvertOptions
   :members:

.. doxygenstruct:: arrow::csv::ParseOptions
   :members:

.. doxygenstruct:: arrow::csv::ReadOptions
   :members:

.. doxygenclass:: arrow::csv::TableReader
   :members:

.. doxygenclass:: arrow::csv::StreamingReader
   :members:

CSV writer
==========

.. doxygenstruct:: arrow::csv::WriteOptions
   :members:

.. doxygengroup:: csv-write-functions
   :content-only:

.. doxygengroup:: csv-writer-factories
   :content-only:

.. _cpp-api-json:

Line-separated JSON
===================

.. doxygenenum:: arrow::json::UnexpectedFieldBehavior

.. doxygenstruct:: arrow::json::ReadOptions
   :members:

.. doxygenstruct:: arrow::json::ParseOptions
   :members:

.. doxygenclass:: arrow::json::TableReader
   :members:

.. doxygenclass:: arrow::json::StreamingReader
   :members:

.. _cpp-api-parquet:

Parquet reader
==============

.. doxygenclass:: parquet::ReaderProperties
   :members:

.. doxygenclass:: parquet::ArrowReaderProperties
   :members:

.. doxygenclass:: parquet::ParquetFileReader
   :members:

.. doxygenclass:: parquet::arrow::FileReader
   :members:

.. doxygenclass:: parquet::arrow::FileReaderBuilder
   :members:

.. doxygengroup:: parquet-arrow-reader-factories
   :content-only:

.. doxygenclass:: parquet::StreamReader
   :members:

Parquet writer
==============

.. doxygenclass:: parquet::WriterProperties
   :members:

.. doxygenclass:: parquet::ArrowWriterProperties
   :members:

.. doxygenclass:: parquet::arrow::FileWriter
   :members:

.. doxygenfunction:: parquet::arrow::WriteTable

.. doxygenclass:: parquet::StreamWriter
   :members:

.. _cpp-api-orc:

ORC
===

.. doxygenclass:: arrow::adapters::orc::ORCFileReader
   :members:

.. doxygenstruct:: arrow::adapters::orc::WriteOptions
   :members:

.. doxygenclass:: arrow::adapters::orc::ORCFileWriter
   :members:
