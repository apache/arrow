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

.. currentmodule:: pyarrow
.. _data:

In-Memory Data Model
====================

Apache Arrow defines columnar array data structures by composing type metadata
with memory buffers, like the ones described previously. These are exposed in
Python through a series of interrelated classes:

* **Type Metadata**: Instances of ``pyarrow.DataType``, which describe a logical
  array type
* **Schemas**: Instances of ``pyarrow.Schema``, which describe a named
  collection of types. These can be thought of as the column types in a
  table-like object.
* **Arrays**: Instances of ``pyarrow.Array``, which are atomic, contiguous
  columnar data structures composed from Arrow Buffer objects
* **Record Batches**: Instances of ``pyarrow.RecordBatch``, which are a
  collection of Array objects with a particular Schema
* **Tables**: Instances of ``pyarrow.Table``, a logical table data structure in
  which each column consists of one or more ``pyarrow.Array`` objects of the
  same type.

We will examine these in the sections below in a series of examples.

Type Metadata
-------------

Each logical data type in Arrow has a corresponding factory function for
creating an instance of that type object in Python:

.. ipython:: python

   import pyarrow as pa
   t1 = pa.int32()
   t2 = pa.string()
   t3 = pa.binary()
   t4 = pa.binary(10)
   t5 = pa.timestamp('ms')

   t1
   print(t1)
   print(t4)
   print(t5)

These objects are `metadata`; they are used for describing the data in arrays,
schemas, and record batches. In Python, they can be used in functions where the
input data (e.g. Python objects) may be coerced to more than one Arrow type.

The :class:`~pyarrow.Field` type is a type plus a name and optional
user-defined metadata:

.. ipython:: python

   f0 = pa.field('int32_field', t1)
   f0
   f0.name
   f0.type

Arrow supports **nested value types** like list, struct, and union. When
creating these, you must pass types or fields to indicate the data types of the
types' children. For example, we can define a list of int32 values with:

.. ipython:: python

   t6 = pa.list_(t1)
   t6

A `struct` is a collection of named fields:

.. ipython:: python

   fields = [
       pa.field('s0', t1),
       pa.field('s1', t2),
       pa.field('s2', t4),
       pa.field('s3', t6)
   ]

   t7 = pa.struct(fields)
   print(t7)

See :ref:`Data Types API <api.types>` for a full listing of data type
functions.

Schemas
-------

Arrays
------

Record Batches
--------------

Tables
------

Dictionary Arrays
-----------------

Custom Schema and Field Metadata
--------------------------------

TODO
