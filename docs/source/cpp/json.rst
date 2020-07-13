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

Arrow allows reading line-separated JSON files as Arrow tables.  Each
independent JSON object in the input file is converted to a row in
the target Arrow table.

.. seealso::
   :ref:`JSON reader API reference <cpp-api-json>`.

Basic usage
===========

A JSON file is read from a :class:`~arrow::io::InputStream`.

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

Data types
==========

Since JSON values are typed, the possible Arrow data types on output
depend on the input value types.  Top-level JSON values should always be
objects.  The fields of top-level objects are taken to represent columns
in the Arrow data.  For each name/value pair in a JSON object, there are
two possible modes of deciding the output data type:

* if the name is in :class:`ConvertOptions::explicit_schema`,
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
