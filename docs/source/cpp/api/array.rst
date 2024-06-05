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

.. doxygenclass:: arrow::ArrayData
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::Array
   :project: arrow_cpp
   :members:

Factory functions
=================

.. doxygengroup:: array-factories
   :content-only:

Concrete array subclasses
=========================

Primitive and temporal
----------------------

.. doxygenclass:: arrow::NullArray
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::BooleanArray
   :project: arrow_cpp
   :members:

.. doxygengroup:: numeric-arrays
   :content-only:
   :members:

Binary-like
-----------

.. doxygengroup:: binary-arrays
   :content-only:
   :members:

Nested
------

.. doxygengroup:: nested-arrays
   :content-only:
   :members:

Dictionary-encoded
------------------

.. doxygenclass:: arrow::DictionaryArray
   :members:

Extension arrays
----------------

.. doxygenclass:: arrow::ExtensionArray
   :members:


Chunked Arrays
==============

.. doxygenclass:: arrow::ChunkedArray
   :project: arrow_cpp
   :members:


Utilities
=========

.. doxygenclass:: arrow::ArrayVisitor
   :project: arrow_cpp
   :members:
   :undoc-members:
