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

Row to columnar conversion
==========================

Fixed Schemas
-------------

The following example converts an array of structs to a :class:`arrow::Table`
instance, and then converts it back to the original array of structs.

.. literalinclude:: ../../../../cpp/examples/arrow/row_wise_conversion_example.cc


Dynamic Schemas
---------------

In many cases, we need to convert to and from row data that does not have a
schema known at compile time. To help implement these conversions, this library
provides several utilities:

* :class:`arrow::RecordBatchBuilder`: creates and manages array builders for
  a full record batch.
* :func:`arrow::VisitTypeInline`: dispatch to functions specialized for the given
  array type.
* :ref:`type-traits` (such as ``arrow::enable_if_primitive_ctype``): narrow template
  functions to specific Arrow types, useful in conjunction with
  the :ref:`cpp-visitor-pattern`.
* :class:`arrow::TableBatchReader`: read a table in a batch at a time, with each
  batch being a zero-copy slice.

The following example shows how to implement conversion between JSON rows
and Arrow objects. You can read the full code example at
https://github.com/apache/arrow/blob/main/cpp/examples/arrow/simdjson_row_converter.cc

Writing conversions to Arrow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To convert rows to Arrow record batches, we'll setup Array builders for all the columns
and then for each row iterate through its fields and append the values to the builders.
We assume that we already know the target schema, which may have
been provided by another system or was inferred in another function. Inferring
the schema *during* conversion is a challenging proposition; many systems will
check the first N rows to infer a schema if there is none already available.

At the top level, we define a function ``ConvertToRecordBatch``:

.. literalinclude:: ../../../../cpp/examples/arrow/simdjson_row_converter.cc
   :language: cpp
   :start-at: arrow::Result<std::shared_ptr<arrow::RecordBatch>> ConvertToRecordBatch(
   :end-at: }  // ConvertToRecordBatch
   :linenos:
   :lineno-match:

First we use :class:`arrow::RecordBatchBuilder`, which conveniently creates builders
for each field in the schema. We parse each JSON row into its own simdjson DOM document
using a single parser, keeping the documents alive throughout conversion. The
``GetAllChildValues`` helper collects the values for all fields in one pass over the rows.
A ``JsonValueConverter`` then uses ``arrow::VisitTypeInline`` to append each column's
values to its builder, recursively handling nested structs and lists. At the end, we call
``batch->ValidateFull()`` to check the integrity of the resulting record batch.

The ``JsonValueConverter`` class is responsible for appending JSON values to Arrow array
builders according to their Arrow data types. It supports integers, floating-point numbers,
strings, booleans, structs, and lists. The ``Unwrap`` helper converts simdjson results into
``arrow::Result`` values, while ``GetField`` retrieves object fields and represents missing
fields as ``std::nullopt``.

.. literalinclude:: ../../../../cpp/examples/arrow/simdjson_row_converter.cc
   :language: cpp
   :start-at: // Collect the values of the given fields from each JSON object, one vector per
   :end-at: }  // GetAllChildValues
   :linenos:
   :lineno-match:

Writing conversions from Arrow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To convert Arrow data into JSON rows, we process the table in smaller batches,
converting each row of a batch into a JSON object.

The ``ConvertToVector`` function converts a record batch into JSON rows. It uses
``JsonColumnWriter`` to convert each column's values to JSON, then combines the columns
into one JSON object per row. The ``ArrowToJsonConverter`` class uses
:class:`arrow::TableBatchReader` to read the table in batches and returns an
:class:`arrow::Iterator` of JSON rows. Processing smaller batches can be more efficient than
converting the entire table at once.

.. literalinclude:: ../../../../cpp/examples/arrow/simdjson_row_converter.cc
   :language: cpp
   :start-at: // Convert a single row of an Arrow record batch into a JSON object.
   :end-at: };  // ArrowToJsonConverter
   :linenos:
   :lineno-match:

The ``JsonColumnWriter`` class converts Arrow arrays into JSON text using
``arrow::VisitArrayInline``. It handles primitive types, strings, structs, and lists,
recursively converting nested arrays. The ``AppendJsonString`` helper escapes strings,
``FormatPrimitive`` formats numeric and boolean values, and ``ObjectToJson`` combines
converted column values into a JSON object.

.. literalinclude:: ../../../../cpp/examples/arrow/simdjson_row_converter.cc
   :language: cpp
   :start-at: /// \brief Visitor that writes the elements of one Arrow array as JSON.
   :end-at: };  // JsonColumnWriter
   :linenos:
   :lineno-match:
