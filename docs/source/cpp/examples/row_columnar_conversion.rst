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

For schemas not known at compile time, implement an :class:`arrow::ToRowConverter`
or an :class:`arrow::FromRowConverter`.
The following example shows how to implement conversion between `rapidjson::Document`
and Arrow objects. You can read the full example at 
https://github.com/apache/arrow/blob/master/cpp/examples/arrow/rapidjson_row_converter.cc

Writing conversions to Arrow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To handle conversions into Arrow, define an :class:`arrow::FromRowConverter` specialized
for your row data structure. In this example, we use :class:`arrow::RecordBatchBuilder`,
which conveniently creates builders for each field in the schemas. Another class ``JsonValueConverter``,
which is introduced next, will handle getting the values from the rows and appending
to the appropriate builder, given a field.

.. literalinclude:: ../../../../cpp/examples/arrow/rapidjson_row_converter.cc
   :language: cpp
   :start-at: class DocumentToArrowConverter
   :end-at: };  // DocumentToArrowConverter
   :linenos:
   :lineno-match:

The ``JsonValueConverter`` implements ``Visit()`` methods for each relevant array
type that append to the current builder. Then the ``Convert()`` method simply
sets the current field, builder, and then calls :func:`arrow::VisitTypeInline`.

.. literalinclude:: ../../../../cpp/examples/arrow/rapidjson_row_converter.cc
   :language: cpp
   :start-at: class JsonValueConverter
   :end-at: };  // JsonValueConverter
   :linenos:
   :lineno-match:

With those defined, we can convert rows into a table using an instance of the
converter:

.. literalinclude:: ../../../../cpp/examples/arrow/rapidjson_row_converter.cc
   :language: cpp
   :start-after: (Doc section: Convert to Arrow)
   :end-before: (Doc section: Convert to Arrow)
   :linenos:
   :lineno-match:


Writing conversions from Arrow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To convert into rows *from* Arrow, define an :class:`arrow::ToRowConverter` specialized
for your row data structure. In our example, we define a ``RowBatchBuilder`` class
to handle converting each column and assigning field values to each row. Because
the builder implements ``Visit()`` methods for each Arrow array type, it can be
passed to :func:`arrow::VisitArrayInline` to dispatch to the appropriate logic for
each array type.

.. literalinclude:: ../../../../cpp/examples/arrow/rapidjson_row_converter.cc
   :language: cpp
   :start-at: class ArrowToDocumentConverter
   :end-at: };  // ArrowToDocumentConverter
   :linenos:
   :lineno-match:

The ``RowBatchBuilder`` implements ``Visit()`` methods, but to save on code is 
able to write a template method for array types that have primitive C equivalents
(booleans, integers, and floats). It uses the ``arrow::enable_if_primitive_ctype``
to narrow the template to only apply to relevant array types. See :ref:`type-traits`
for other type predicates.

.. literalinclude:: ../../../../cpp/examples/arrow/rapidjson_row_converter.cc
   :language: cpp
   :start-at: class RowBatchBuilder
   :end-at: };  // RowBatchBuilder
   :linenos:
   :lineno-match:

Finally, we can use the converter iterate over rows in the batch as rapidjson
Documents:

.. literalinclude:: ../../../../cpp/examples/arrow/rapidjson_row_converter.cc
   :language: cpp
   :start-after: (Doc section: Convert to Rows)
   :end-before: (Doc section: Convert to Rows)
   :linenos:
   :lineno-match: