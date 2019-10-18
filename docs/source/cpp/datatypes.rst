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

Data Types
==========

Data types govern how physical data is interpreted.  Their :ref:`specification
<format_columnar>` allows binary interoperability between different Arrow
implementations, including from different programming languages and runtimes
(for example it is possible to access the same data, without copying, from
both Python and Java using the :py:mod:`pyarrow.jvm` bridge module).

Information about a data type in C++ can be represented in three ways:

1. Using a :class:`arrow::DataType` instance (e.g. as a function argument)
2. Using a :class:`arrow::DataType` concrete subclass (e.g. as a template
   parameter)
3. Using a :type:`arrow::Type::type` enum value (e.g. as the condition of
   a switch statement)

The first form (using a :class:`arrow::DataType` instance) is the most idiomatic
and flexible.  Runtime-parametric types can only be fully represented with
a DataType instance.  For example, a :class:`arrow::TimestampType` needs to be
constructed at runtime with a :type:`arrow::TimeUnit::type` parameter; a
:class:`arrow::Decimal128Type` with *scale* and *precision* parameters;
a :class:`arrow::ListType` with a full child type (itself a
:class:`arrow::DataType` instance).

The two other forms can be used where performance is critical, in order to
avoid paying the price of dynamic typing and polymorphism.  However, some
amount of runtime switching can still be required for parametric types.
It is not possible to reify all possible types at compile time, since Arrow
data types allows arbitrary nesting.

Creating data types
-------------------

To instantiate data types, it is recommended to call the provided
:ref:`factory functions <api-type-factories>`::

   std::shared_ptr<arrow::DataType> type;

   // A 16-bit integer type
   type = arrow::int16();
   // A 64-bit timestamp type (with microsecond granularity)
   type = arrow::timestamp(arrow::TimeUnit::MICRO);
   // A list type of single-precision floating-point values
   type = arrow::list(arrow::float32());
