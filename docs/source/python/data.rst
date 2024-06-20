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

Data Types and In-Memory Data Model
===================================

Apache Arrow defines columnar array data structures by composing type metadata
with memory buffers, like the ones explained in the documentation on
:ref:`Memory and IO <io>`. These data structures are exposed in Python through
a series of interrelated classes:

* **Type Metadata**: Instances of ``pyarrow.DataType``, which describe the
  type of an array and govern how its values are interpreted
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
* **Nested types**: list, map, struct, and union
* **Dictionary type**: An encoded categorical type (more on this later)

Each data type in Arrow has a corresponding factory function for creating
an instance of that type object in Python:

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

.. note::
   Different data types might use a given physical storage. For example,
   ``int64``, ``float64``, and ``timestamp[ms]`` all occupy 64 bits per value.

These objects are ``metadata``; they are used for describing the data in arrays,
schemas, and record batches. In Python, they can be used in functions where the
input data (e.g. Python objects) may be coerced to more than one Arrow type.

The :class:`~pyarrow.Field` type is a type plus a name and optional
user-defined metadata:

.. ipython:: python

   f0 = pa.field('int32_field', t1)
   f0
   f0.name
   f0.type

Arrow supports **nested value types** like list, map, struct, and union. When
creating these, you must pass types or fields to indicate the data types of the
types' children. For example, we can define a list of int32 values with:

.. ipython:: python

   t6 = pa.list_(t1)
   t6

A ``struct`` is a collection of named fields:

.. ipython:: python

   fields = [
       pa.field('s0', t1),
       pa.field('s1', t2),
       pa.field('s2', t4),
       pa.field('s3', t6),
   ]

   t7 = pa.struct(fields)
   print(t7)

For convenience, you can pass ``(name, type)`` tuples directly instead of
:class:`~pyarrow.Field` instances:

.. ipython:: python

   t8 = pa.struct([('s0', t1), ('s1', t2), ('s2', t4), ('s3', t6)])
   print(t8)
   t8 == t7


See :ref:`Data Types API <api.types>` for a full listing of data type
functions.

.. _data.schema:

Schemas
-------

The :class:`~pyarrow.Schema` type is similar to the ``struct`` array type; it
defines the column names and types in a record batch or table data
structure. The :func:`pyarrow.schema` factory function makes new Schema objects in
Python:

.. ipython:: python

   my_schema = pa.schema([('field0', t1),
                          ('field1', t2),
                          ('field2', t4),
                          ('field3', t6)])
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

To handle better compatibility with Pandas, we support interpreting NaN values as
null elements. This is enabled automatically on all ``from_pandas`` function and
can be enabled on the other conversion functions by passing ``from_pandas=True``
as a function parameter.

List arrays
~~~~~~~~~~~

``pyarrow.array`` is able to infer the type of simple nested data structures
like lists:

.. ipython:: python

   nested_arr = pa.array([[], None, [1, 2], [None, 1]])
   print(nested_arr.type)

ListView arrays
~~~~~~~~~~~~~~~

``pyarrow.array`` can create an alternate list type called ListView:

.. ipython:: python

   nested_arr = pa.array([[], None, [1, 2], [None, 1]], type=pa.list_view(pa.int64()))
   print(nested_arr.type)

ListView arrays have a different set of buffers than List arrays. The ListView array
has both an offsets and sizes buffer, while a List array only has an offsets buffer.
This allows for ListView arrays to specify out-of-order offsets:

.. ipython:: python

   values = [1, 2, 3, 4, 5, 6]
   offsets = [4, 2, 0]
   sizes = [2, 2, 2]
   arr = pa.ListViewArray.from_arrays(offsets, sizes, values)
   arr

See the format specification for more details on :ref:`listview-layout`.

Struct arrays
~~~~~~~~~~~~~

``pyarrow.array`` is able to infer the schema of a struct type from arrays of
dictionaries:

.. ipython:: python

   pa.array([{'x': 1, 'y': True}, {'z': 3.4, 'x': 4}])

Struct arrays can be initialized from a sequence of Python dicts or tuples. For tuples,
you must explicitly pass the type:

.. ipython:: python

   ty = pa.struct([('x', pa.int8()),
                   ('y', pa.bool_())])
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

Map arrays
~~~~~~~~~~

Map arrays can be constructed from lists of lists of tuples (key-item pairs), but only if
the type is explicitly passed into :meth:`array`:

.. ipython:: python

   data = [[('x', 1), ('y', 0)], [('a', 2), ('b', 45)]]
   ty = pa.map_(pa.string(), pa.int64())
   pa.array(data, type=ty)

MapArrays can also be constructed from offset, key, and item arrays. Offsets represent the
starting position of each map. Note that the :attr:`MapArray.keys` and :attr:`MapArray.items`
properties give the *flattened* keys and items. To keep the keys and items associated to
their row, use the :meth:`ListArray.from_arrays` constructor with the
:attr:`MapArray.offsets` property.

.. ipython:: python

   arr = pa.MapArray.from_arrays([0, 2, 3], ['x', 'y', 'z'], [4, 5, 6])
   arr.keys
   arr.items
   pa.ListArray.from_arrays(arr.offsets, arr.keys)
   pa.ListArray.from_arrays(arr.offsets, arr.items)

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

.. _data.dictionary:

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

The table's columns are instances of :class:`~.ChunkedArray`, which is a
container for one or more arrays of the same type.

.. ipython:: python

   c = table[0]
   c
   c.num_chunks
   c.chunk(0)

As you'll see in the :ref:`pandas section <pandas_interop>`, we can convert
these objects to contiguous NumPy arrays for use in pandas:

.. ipython:: python

   c.to_pandas()

Multiple tables can also be concatenated together to form a single table using
``pyarrow.concat_tables``, if the schemas are equal:

.. ipython:: python

   tables = [table] * 2
   table_all = pa.concat_tables(tables)
   table_all.num_rows
   c = table_all[0]
   c.num_chunks

This is similar to ``Table.from_batches``, but uses tables as input instead of
record batches. Record batches can be made into tables, but not the other way
around, so if your data is already in table form, then use
``pyarrow.concat_tables``.

Custom Schema and Field Metadata
--------------------------------

Arrow supports both schema-level and field-level custom key-value metadata
allowing for systems to insert their own application defined metadata to
customize behavior.

Custom metadata can be accessed at :attr:`Schema.metadata` for the schema-level
and :attr:`Field.metadata` for the field-level.

Note that this metadata is preserved in :ref:`ipc` processes.

To customize the schema metadata of an existing table you can use
:meth:`Table.replace_schema_metadata`:

.. ipython:: python

   table.schema.metadata # empty
   table = table.replace_schema_metadata({"f0": "First dose"})
   table.schema.metadata

To customize the metadata of the field from the table schema you can use
:meth:`Field.with_metadata`:

.. ipython:: python

   field_f1 = table.schema.field("f1")
   field_f1.metadata # empty
   field_f1 = field_f1.with_metadata({"f1": "Second dose"})
   field_f1.metadata

Both options create a shallow copy of the data and do not in fact change the
Schema which is immutable. To change the metadata in the schema of the table
we created a new object when calling :meth:`Table.replace_schema_metadata`.

To change the metadata of the field in the schema we would need to define
a new schema and cast the data to this schema:

.. ipython:: python

   my_schema2 = pa.schema([
      pa.field('f0', pa.int64(), metadata={"name": "First dose"}),
      pa.field('f1', pa.string(), metadata={"name": "Second dose"}),
      pa.field('f2', pa.bool_())],
      metadata={"f2": "booster"})
   t2 = table.cast(my_schema2)
   t2.schema.field("f0").metadata
   t2.schema.field("f1").metadata
   t2.schema.metadata

Metadata key and value pairs are ``std::string`` objects in the C++ implementation
and so they are bytes objects (``b'...'``) in Python.

Record Batch Readers
--------------------

Many functions in PyArrow either return or take as an argument a :class:`RecordBatchReader`.
It can be used like any iterable of record batches, but also provides their common
schema without having to get any of the batches.::

   >>> schema = pa.schema([('x', pa.int64())])
   >>> def iter_record_batches():
   ...    for i in range(2):
   ...       yield pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], schema=schema)
   >>> reader = pa.RecordBatchReader.from_batches(schema, iter_record_batches())
   >>> print(reader.schema)
   pyarrow.Schema
   x: int64
   >>> for batch in reader:
   ...    print(batch)
   pyarrow.RecordBatch
   x: int64
   pyarrow.RecordBatch
   x: int64

It can also be sent between languages using the :ref:`C stream interface <c-stream-interface>`.

Conversion of RecordBatch to Tensor
-----------------------------------

Each array of the ``RecordBatch`` has it's own contiguous memory that is not necessarily
adjacent to other arrays. A different memory structure that is used in machine learning
libraries is a two dimensional array (also called a 2-dim tensor or a matrix) which takes
only one contiguous block of memory.

For this reason there is a function ``pyarrow.RecordBatch.to_tensor()`` available
to efficiently convert tabular columnar data into a tensor.

Data types supported in this conversion are unsigned, signed integer and float
types. Currently only column-major conversion is supported.

   >>>  import pyarrow as pa
   >>>  arr1 = [1, 2, 3, 4, 5]
   >>>  arr2 = [10, 20, 30, 40, 50]
   >>>  batch = pa.RecordBatch.from_arrays(
   ...      [
   ...          pa.array(arr1, type=pa.uint16()),
   ...          pa.array(arr2, type=pa.int16()),
   ...      ], ["a", "b"]
   ...  )
   >>>  batch.to_tensor()
   <pyarrow.Tensor>
   type: int32
   shape: (9, 2)
   strides: (4, 36)
   >>>  batch.to_tensor().to_numpy()
   array([[ 1, 10],
         [ 2, 20],
         [ 3, 30],
         [ 4, 40],
         [ 5, 50]], dtype=int32)

With ``null_to_nan`` set to ``True`` one can also convert data with
nulls. They will be converted to ``NaN``:

   >>> import pyarrow as pa
   >>> batch = pa.record_batch(
   ...     [
   ...         pa.array([1, 2, 3, 4, None], type=pa.int32()),
   ...         pa.array([10, 20, 30, 40, None], type=pa.float32()),
   ...     ], names = ["a", "b"]
   ... )
   >>> batch.to_tensor(null_to_nan=True).to_numpy()
   array([[ 1., 10.],
         [ 2., 20.],
         [ 3., 30.],
         [ 4., 40.],
         [nan, nan]])
