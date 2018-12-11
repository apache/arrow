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

==========
Data Types
==========

.. doxygenenum:: arrow::Type::type

.. doxygenclass:: arrow::DataType
   :members:

.. _api-type-factories:

Factory functions
=================

These functions are recommended for creating data types.  They may return
new objects or existing singletons, depending on the type requested.

.. doxygengroup:: type-factories
   :project: arrow_cpp
   :content-only:

Concrete type subclasses
========================

Primitive
---------

.. doxygenclass:: arrow::NullType
   :members:

.. doxygenclass:: arrow::BooleanType
   :members:

.. doxygenclass:: arrow::Int8Type
   :members:

.. doxygenclass:: arrow::Int16Type
   :members:

.. doxygenclass:: arrow::Int32Type
   :members:

.. doxygenclass:: arrow::Int64Type
   :members:

.. doxygenclass:: arrow::UInt8Type
   :members:

.. doxygenclass:: arrow::UInt16Type
   :members:

.. doxygenclass:: arrow::UInt32Type
   :members:

.. doxygenclass:: arrow::UInt64Type
   :members:

.. doxygenclass:: arrow::HalfFloatType
   :members:

.. doxygenclass:: arrow::FloatType
   :members:

.. doxygenclass:: arrow::DoubleType
   :members:

Time-related
------------

.. doxygenenum:: arrow::TimeUnit::type

.. doxygenclass:: arrow::Date32Type
   :members:

.. doxygenclass:: arrow::Date64Type
   :members:

.. doxygenclass:: arrow::Time32Type
   :members:

.. doxygenclass:: arrow::Time64Type
   :members:

.. doxygenclass:: arrow::TimestampType
   :members:

Binary-like
-----------

.. doxygenclass:: arrow::BinaryType
   :members:

.. doxygenclass:: arrow::StringType
   :members:

.. doxygenclass:: arrow::FixedSizeBinaryType
   :members:

.. doxygenclass:: arrow::Decimal128Type
   :members:

Nested
------

.. doxygenclass:: arrow::ListType
   :members:

.. doxygenclass:: arrow::StructType
   :members:

.. doxygenclass:: arrow::UnionType
   :members:

Dictionary-encoded
------------------

.. doxygenclass:: arrow::DictionaryType
   :members:
