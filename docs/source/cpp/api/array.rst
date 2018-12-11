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

======
Arrays
======

.. doxygenclass:: arrow::Array
   :project: arrow_cpp
   :members:

Concrete array subclasses
=========================

.. doxygenclass:: arrow::DictionaryArray
   :project: arrow_cpp
   :members:

Non-nested
----------

.. doxygenclass:: arrow::FlatArray
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::NullArray
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::BinaryArray
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::StringArray
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::PrimitiveArray
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::BooleanArray
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::FixedSizeBinaryArray
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::Decimal128Array
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::NumericArray
   :project: arrow_cpp
   :members:

Nested
------

.. doxygenclass:: arrow::UnionArray
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::ListArray
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::StructArray
   :project: arrow_cpp
   :members:

Chunked Arrays
==============

.. doxygenclass:: arrow::ChunkedArray
   :project: arrow_cpp
   :members:
