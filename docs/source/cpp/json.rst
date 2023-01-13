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

.. cpp:namespace:: arrow::json

==================
Reading JSON files
==================

Line-separated JSON files can either be read as a single Arrow Table
with a :class:`~TableReader` or streamed as RecordBatches with a
:class:`~StreamingReader`.

Both of these readers require an :class:`arrow::io::InputStream` instance
representing the input file. Their behavior can be customized using a
combination of :class:`~ReadOptions`, :class:`~ParseOptions`, and
other parameters.

.. seealso::
   :ref:`JSON reader API reference <cpp-api-json>`.

TableReader
===========

:class:`~TableReader` reads an entire file in one shot as a :class:`~arrow::Table`. Each
independent JSON object in the input file is converted to a row in
the output table.

.. code-block:: cpp

   #include "arrow/json/api.h"

   {
      // ...
      arrow::Status st;
      arrow::MemoryPool* pool = default_memory_pool();
      std::shared_ptr<arrow::io::InputStream> input = ...;

      auto read_options = arrow::json::ReadOptions::Defaults();
      auto parse_options = arrow::json::ParseOptions::Defaults();

      // Instantiate TableReader from input stream and options
      std::shared_ptr<arrow::json::TableReader> reader;
      st = arrow::json::TableReader::Make(pool, input, read_options,
                                          parse_options, &reader);
      if (!st.ok()) {
         // Handle TableReader instantiation error...
      }

      std::shared_ptr<arrow::Table> table;
      // Read table from JSON file
      st = reader->Read(&table);
      if (!st.ok()) {
         // Handle JSON read error
         // (for example a JSON syntax error or failed type conversion)
      }
   }

StreamingReader
===============

:class:`~StreamingReader` reads a file incrementally from blocks of a roughly equal byte size, each yielding a
:class:`~arrow::RecordBatch`. Each independent JSON object in a block
is converted to a row in the output batch.

All batches adhere to a consistent :class:`~arrow::Schema`, which is
derived from the first loaded batch. Alternatively, an explicit schema
may be passed via :class:`~ParseOptions`.

.. code-block:: cpp

   #include "arrow/json/api.h"

   {
      // ...
      auto read_options = arrow::json::ReadOptions::Defaults();
      auto parse_options = arrow::json::ParseOptions::Defaults();

      std::shared_ptr<arrow::io::InputStream> stream;
      auto result = arrow::json::StreamingReader::Make(stream,
                                                       read_options,
                                                       parse_options);
      if (!result.ok()) {
         // Handle instantiation error
      }
      std::shared_ptr<arrow::json::StreamingReader> reader = *result;

      for (arrow::Result<std::shared_ptr<arrow::RecordBatch>> maybe_batch : *reader) {
         if (!maybe_batch.ok()) {
            // Handle read/parse error
         }
         std::shared_ptr<arrow::RecordBatch> batch = *maybe_batch;
         // Operate on each batch...
      }
   }

Data types
==========

Since JSON values are typed, the possible Arrow data types on output
depend on the input value types.  Top-level JSON values should always be
objects.  The fields of top-level objects are taken to represent columns
in the Arrow data.  For each name/value pair in a JSON object, there are
two possible modes of deciding the output data type:

* if the name is in :member:`ParseOptions::explicit_schema`,
  conversion of the JSON value to the corresponding Arrow data type is
  attempted;

* otherwise, the Arrow data type is determined via type inference on
  the JSON value, trying out a number of Arrow data types in order.

The following tables show the possible combinations for each of those
two modes.

.. table:: Explicit conversions from JSON to Arrow
   :align: center

   +-----------------+----------------------------------------------------+
   | JSON value type | Allowed Arrow data types                           |
   +=================+====================================================+
   | Null            | Any (including Null)                               |
   +-----------------+----------------------------------------------------+
   | Number          | All Integer types, Float32, Float64,               |
   |                 | Date32, Date64, Time32, Time64                     |
   +-----------------+----------------------------------------------------+
   | Boolean         | Boolean                                            |
   +-----------------+----------------------------------------------------+
   | String          | Binary, LargeBinary, String, LargeString,          |
   |                 | Timestamp                                          |
   +-----------------+----------------------------------------------------+
   | Array           | List                                               |
   +-----------------+----------------------------------------------------+
   | Object (nested) | Struct                                             |
   +-----------------+----------------------------------------------------+

.. table:: Implicit type inference from JSON to Arrow
   :align: center

   +-----------------+----------------------------------------------------+
   | JSON value type | Inferred Arrow data types (in order)               |
   +=================+====================================================+
   | Null            | Null, any other                                    |
   +-----------------+----------------------------------------------------+
   | Number          | Int64, Float64                                     |
   |                 |                                                    |
   +-----------------+----------------------------------------------------+
   | Boolean         | Boolean                                            |
   +-----------------+----------------------------------------------------+
   | String          | Timestamp (with seconds unit), String              |
   |                 |                                                    |
   +-----------------+----------------------------------------------------+
   | Array           | List                                               |
   +-----------------+----------------------------------------------------+
   | Object (nested) | Struct                                             |
   +-----------------+----------------------------------------------------+
