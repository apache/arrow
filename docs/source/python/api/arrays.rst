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

Factory Functions
-----------------

These functions create new Arrow arrays:

.. autosummary::
   :toctree: ../generated/

   array
   nulls

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
   HalfFloatArray
   FloatArray
   DoubleArray
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
   DurationArray
   MonthDayNanoIntervalArray
   Decimal128Array
   DictionaryArray
   ListArray
   FixedSizeListArray
   LargeListArray
   MapArray
   RunEndEncodedArray
   StructArray
   UnionArray
   ExtensionArray
   FixedShapeTensorArray

.. _api.scalar:

Scalars
-------

This function constructs a new Arrow scalar:

.. autosummary::
   :toctree: ../generated/

   scalar

A scalar's python class depends on its data type.  Concrete scalar
classes may expose data type-specific methods or properties.

.. autosummary::
   :toctree: ../generated/

   NA
   Scalar
   BooleanScalar
   Int8Scalar
   Int16Scalar
   Int32Scalar
   Int64Scalar
   UInt8Scalar
   UInt16Scalar
   UInt32Scalar
   UInt64Scalar
   HalfFloatScalar
   FloatScalar
   DoubleScalar
   BinaryScalar
   StringScalar
   FixedSizeBinaryScalar
   LargeBinaryScalar
   LargeStringScalar
   Time32Scalar
   Time64Scalar
   Date32Scalar
   Date64Scalar
   TimestampScalar
   DurationScalar
   MonthDayNanoIntervalScalar
   Decimal128Scalar
   DictionaryScalar
   RunEndEncodedScalar
   ListScalar
   LargeListScalar
   MapScalar
   StructScalar
   UnionScalar
   ExtensionScalar
