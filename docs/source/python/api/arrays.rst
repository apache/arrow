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

.. _api.array:
.. currentmodule:: pyarrow

Arrays and Scalars
==================

Factory Function
----------------

This function is the main entry point to create an Arrow array from Python.

.. autosummary::
   :toctree: ../generated/

   array

Array Types
-----------

An array's Python class depends on its data type.  Concrete array classes
may expose data type-specific methods or properties.

.. autosummary::
   :toctree: ../generated/

   Array
   BooleanArray
   FloatingPointArray
   IntegerArray
   Int8Array
   Int16Array
   Int32Array
   Int64Array
   NullArray
   NumericArray
   UInt8Array
   UInt16Array
   UInt32Array
   UInt64Array
   BinaryArray
   StringArray
   FixedSizeBinaryArray
   LargeBinaryArray
   LargeStringArray
   Time32Array
   Time64Array
   Date32Array
   Date64Array
   TimestampArray
   Decimal128Array
   DictionaryArray
   ListArray
   LargeListArray
   StructArray
   UnionArray

.. _api.scalar:

Array Scalars
-------------

Indexing an array wraps the represented value in a scalar object whose
concrete type depends on the array data type.  You shouldn't instantiate
any of those classes directly.

.. autosummary::
   :toctree: ../generated/

   NA
   Scalar
   ArrayValue
   BooleanValue
   Int8Value
   Int16Value
   Int32Value
   Int64Value
   UInt8Value
   UInt16Value
   UInt32Value
   UInt64Value
   FloatValue
   DoubleValue
   BinaryValue
   StringValue
   FixedSizeBinaryValue
   LargeBinaryValue
   LargeStringValue
   Time32Value
   Time64Value
   Date32Value
   Date64Value
   TimestampValue
   DecimalValue
   DictionaryValue
   ListValue
   LargeListValue
   StructValue
   UnionValue
