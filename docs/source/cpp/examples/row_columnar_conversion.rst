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

The following example shows how to implement conversion between ``rapidjson::Document``
and Arrow objects. You can read the full code example at
https://github.com/apache/arrow/blob/main/cpp/examples/arrow/rapidjson_row_converter.cc

Writing conversions to Arrow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To convert rows to Arrow record batches, we'll setup Array builders for all the columns
and then for each field iterate through row values and append to the builders.
We assume that we already know the target schema, which may have
been provided by another system or was inferred in another function. Inferring
the schema *during* conversion is a challenging proposition; many systems will
check the first N rows to infer a schema if there is none already available.

At the top level, we define a function ``ConvertToRecordBatch``:

.. literalinclude:: ../../../../cpp/examples/arrow/rapidjson_row_converter.cc
   :language: cpp
   :start-at: arrow::Result<std::shared_ptr<arrow::RecordBatch>> ConvertToRecordBatch(
   :end-at: }  // ConvertToRecordBatch
   :linenos:
   :lineno-match:

First we use :class:`arrow::RecordBatchBuilder`, which conveniently creates builders
for each field in the schema. Then we iterate over the fields of the schema, get
the builder, and call ``Convert()`` on our ``JsonValueConverter`` (to be discussed
next). At the end, we call ``batch->ValidateFull()``, which checks the integrity
of our arrays to make sure the conversion was performed correctly, which is useful
for debugging new conversion implementations.

One level down, the ``JsonValueConverter`` is responsible for appending row values
for the provided field to a provided array builder. In order to specialize logic
for each data type, it implements ``Visit`` methods and calls :func:`arrow::VisitTypeInline`.
(See more about type visitors in :ref:`cpp-visitor-pattern`.)

At the end of that class is the private method ``FieldValues()``, which returns
an iterator of the column values for the current field across the rows. In
row-based structures that are flat (such as a vector of values) this may be
trivial to implement. But if the schema is nested, as in the case of JSON documents,
a special iterator is needed to navigate the levels of nesting. See the
`full example <https://github.com/apache/arrow/blob/main/cpp/examples/arrow/rapidjson_row_converter.cc>`_
for the implementation details of ``DocValuesIterator``.

.. literalinclude:: ../../../../cpp/examples/arrow/rapidjson_row_converter.cc
   :language: cpp
   :start-at: class JsonValueConverter
   :end-at: };  // JsonValueConverter
   :linenos:
   :lineno-match:

Writing conversions from Arrow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To convert into rows *from* Arrow record batches, we'll process the table in
smaller batches, visiting each field of the batch and filling the output rows
column-by-column.

At the top-level, we define ``ArrowToDocumentConverter`` that provides the API
for converting Arrow batches and tables to rows. In many cases, it's more optimal
to perform conversions to rows in smaller batches, rather than doing the entire
table at once. So we define one ``ConvertToVector`` method to convert a single
batch, then in the other conversion method we use :class:`arrow::TableBatchReader`
to iterate over slices of a table. This returns Arrow's iterator type
(:class:`arrow::Iterator`) so rows could then be processed either one-at-a-time
or be collected into a container.

.. literalinclude:: ../../../../cpp/examples/arrow/rapidjson_row_converter.cc
   :language: cpp
   :start-at: class ArrowToDocumentConverter
   :end-at: };  // ArrowToDocumentConverter
   :linenos:
   :lineno-match:

One level down, the output rows are filled in by ``RowBatchBuilder``.
The ``RowBatchBuilder`` implements ``Visit()`` methods, but to save on code we
write a template method for array types that have primitive C equivalents
(booleans, integers, and floats) using ``arrow::enable_if_primitive_ctype``.
See :ref:`type-traits` for other type predicates.

.. literalinclude:: ../../../../cpp/examples/arrow/rapidjson_row_converter.cc
   :language: cpp
   :start-at: class RowBatchBuilder
   :end-at: };  // RowBatchBuilder
   :linenos:
   :lineno-match:
