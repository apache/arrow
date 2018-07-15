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
with memory buffers, like the ones explained in the documentation on
:ref:`Memory and IO <io>`. These data structures are exposed in Python through
a series of interrelated classes:

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

.. _data.types:

Type Metadata
-------------

Apache Arrow defines language agnostic column-oriented data structures for
array data. These include:

* **Fixed-length primitive types**: numbers, booleans, date and times, fixed
  size binary, decimals, and other values that fit into a given number
* **Variable-length primitive types**: binary, string
* **Nested types**: list, struct, and union
* **Dictionary type**: An encoded categorical type (more on this later)

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

We use the name **logical type** because the **physical** storage may be the
same for one or more types. For example, ``int64``, ``float64``, and
``timestamp[ms]`` all occupy 64 bits per value.

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

.. _data.schema:

Schemas
-------

The :class:`~pyarrow.Schema` type is similar to the ``struct`` array type; it
defines the column names and types in a record batch or table data
structure. The ``pyarrow.schema`` factory function makes new Schema objects in
Python:

.. ipython:: python

   fields = [
       pa.field('s0', t1),
       pa.field('s1', t2),
       pa.field('s2', t4),
       pa.field('s3', t6)
   ]

   my_schema = pa.schema(fields)
   my_schema

In some applications, you may not create schemas directly, only using the ones
that are embedded in :ref:`IPC messages <ipc>`.

.. _data.array:

Arrays
------

For each data type, there is an accompanying array data structure for holding
memory buffers that define a single contiguous chunk of columnar array
data. When you are using PyArrow, this data may come from IPC tools, though it
can also be created from various types of Python sequences (lists, NumPy
arrays, pandas data).

A simple way to create arrays is with ``pyarrow.array``, which is similar to
the ``numpy.array`` function.  By default PyArrow will infer the data type
for you:

.. ipython:: python

   arr = pa.array([1, 2, None, 3])
   arr

But you may also pass a specific data type to override type inference:

.. ipython:: python

   pa.array([1, 2], type=pa.uint16())

The array's ``type`` attribute is the corresponding piece of type metadata:

.. ipython:: python

   arr.type

Each in-memory array has a known length and null count (which will be 0 if
there are no null values):

.. ipython:: python

   len(arr)
   arr.null_count

Scalar values can be selected with normal indexing.  ``pyarrow.array`` converts
``None`` values to Arrow nulls; we return the special ``pyarrow.NA`` value for
nulls:

.. ipython:: python

   arr[0]
   arr[2]

Arrow data is immutable, so values can be selected but not assigned.

Arrays can be sliced without copying:

.. ipython:: python

   arr[1:3]

None values and NAN handling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As mentioned in the above section, the Python object ``None`` is always
converted to an Arrow null element on the conversion to ``pyarrow.Array``. For
the float NaN value which is either represented by the Python object
``float('nan')`` or ``numpy.nan`` we normally convert it to a *valid* float
value during the conversion. If an integer input is supplied to
``pyarrow.array`` that contains ``np.nan``, ``ValueError`` is raised.

To handle better compability with Pandas, we support interpreting NaN values as
null elements. This is enabled automatically on all ``from_pandas`` function and
can be enable on the other conversion functions by passing ``from_pandas=True``
as a function parameter.

List arrays
~~~~~~~~~~~

``pyarrow.array`` is able to infer the type of simple nested data structures
like lists:

.. ipython:: python

   nested_arr = pa.array([[], None, [1, 2], [None, 1]])
   print(nested_arr.type)

Struct arrays
~~~~~~~~~~~~~

For other kinds of nested arrays, such as struct arrays, you currently need
to pass the type explicitly.  Struct arrays can be initialized from a
sequence of Python dicts or tuples:

.. ipython:: python

   ty = pa.struct([
       pa.field('x', pa.int8()),
       pa.field('y', pa.bool_()),
   ])
   pa.array([{'x': 1, 'y': True}, {'x': 2, 'y': False}], type=ty)
   pa.array([(3, True), (4, False)], type=ty)

When initializing a struct array, nulls are allowed both at the struct
level and at the individual field level.  If initializing from a sequence
of Python dicts, a missing dict key is handled as a null value:

.. ipython:: python

   pa.array([{'x': 1}, None, {'y': None}], type=ty)

You can also construct a struct array from existing arrays for each of the
struct's components.  In this case, data storage will be shared with the
individual arrays, and no copy is involved:

.. ipython:: python

   xs = pa.array([5, 6, 7], type=pa.int16())
   ys = pa.array([False, True, True])
   arr = pa.StructArray.from_arrays((xs, ys), names=('x', 'y'))
   arr.type
   arr

Union arrays
~~~~~~~~~~~~

The union type represents a nested array type where each value can be one
(and only one) of a set of possible types.  There are two possible
storage types for union arrays: sparse and dense.

In a sparse union array, each of the child arrays has the same length
as the resulting union array.  They are adjuncted with a ``int8`` "types"
array that tells, for each value, from which child array it must be
selected:

.. ipython:: python

   xs = pa.array([5, 6, 7])
   ys = pa.array([False, False, True])
   types = pa.array([0, 1, 1], type=pa.int8())
   union_arr = pa.UnionArray.from_sparse(types, [xs, ys])
   union_arr.type
   union_arr

In a dense union array, you also pass, in addition to the ``int8`` "types"
array, a ``int32`` "offsets" array that tells, for each value, at
each offset in the selected child array it can be found:

.. ipython:: python

   xs = pa.array([5, 6, 7])
   ys = pa.array([False, True])
   types = pa.array([0, 1, 1, 0, 0], type=pa.int8())
   offsets = pa.array([0, 0, 1, 1, 2], type=pa.int32())
   union_arr = pa.UnionArray.from_dense(types, offsets, [xs, ys])
   union_arr.type
   union_arr


Dictionary Arrays
~~~~~~~~~~~~~~~~~

The **Dictionary** type in PyArrow is a special array type that is similar to a
factor in R or a ``pandas.Categorical``. It enables one or more record batches
in a file or stream to transmit integer *indices* referencing a shared
**dictionary** containing the distinct values in the logical array. This is
particularly often used with strings to save memory and improve performance.

The way that dictionaries are handled in the Apache Arrow format and the way
they appear in C++ and Python is slightly different. We define a special
:class:`~.DictionaryArray` type with a corresponding dictionary type. Let's
consider an example:

.. ipython:: python

   indices = pa.array([0, 1, 0, 1, 2, 0, None, 2])
   dictionary = pa.array(['foo', 'bar', 'baz'])

   dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)
   dict_array

Here we have:

.. ipython:: python

   print(dict_array.type)
   dict_array.indices
   dict_array.dictionary

When using :class:`~.DictionaryArray` with pandas, the analogue is
``pandas.Categorical`` (more on this later):

.. ipython:: python

   dict_array.to_pandas()

.. _data.record_batch:

Record Batches
--------------

A **Record Batch** in Apache Arrow is a collection of equal-length array
instances. Let's consider a collection of arrays:

.. ipython:: python

   data = [
       pa.array([1, 2, 3, 4]),
       pa.array(['foo', 'bar', 'baz', None]),
       pa.array([True, None, False, True])
   ]

A record batch can be created from this list of arrays using
``RecordBatch.from_arrays``:

.. ipython:: python

   batch = pa.RecordBatch.from_arrays(data, ['f0', 'f1', 'f2'])
   batch.num_columns
   batch.num_rows
   batch.schema

   batch[1]

A record batch can be sliced without copying memory like an array:

.. ipython:: python

   batch2 = batch.slice(1, 3)
   batch2[1]

.. _data.table:

Tables
------

The PyArrow :class:`~.Table` type is not part of the Apache Arrow
specification, but is rather a tool to help with wrangling multiple record
batches and array pieces as a single logical dataset. As a relevant example, we
may receive multiple small record batches in a socket stream, then need to
concatenate them into contiguous memory for use in NumPy or pandas. The Table
object makes this efficient without requiring additional memory copying.

Considering the record batch we created above, we can create a Table containing
one or more copies of the batch using ``Table.from_batches``:

.. ipython:: python

   batches = [batch] * 5
   table = pa.Table.from_batches(batches)
   table
   table.num_rows

The table's columns are instances of :class:`~.Column`, which is a container
for one or more arrays of the same type.

.. ipython:: python

   c = table[0]
   c
   c.data
   c.data.num_chunks
   c.data.chunk(0)

As you'll see in the :ref:`pandas section <pandas>`, we can convert these
objects to contiguous NumPy arrays for use in pandas:

.. ipython:: python

   c.to_pandas()

Multiple tables can also be concatenated together to form a single table using
``pyarrow.concat_tables``, if the schemas are equal:

.. ipython:: python

   tables = [table] * 2
   table_all = pa.concat_tables(tables)
   table_all.num_rows
   c = table_all[0]
   c.data.num_chunks

This is similar to ``Table.from_batches``, but uses tables as input instead of
record batches. Record batches can be made into tables, but not the other way
around, so if your data is already in table form, then use
``pyarrow.concat_tables``.

Custom Schema and Field Metadata
--------------------------------

TODO
