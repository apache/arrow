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

======
Arrays
======

.. seealso::
   :doc:`Array API reference <api/array>`

The central type in Arrow is the class :class:`arrow::Array`.   An array
represents a known-length sequence of values all having the same type.
Internally, those values are represented by one or several buffers, the
number and meaning of which depend on the array's data type, as documented
in :ref:`the Arrow data layout specification <format_layout>`.

Those buffers consist of the value data itself and an optional bitmap buffer
that indicates which array entries are null values.  The bitmap buffer
can be entirely omitted if the array is known to have zero null values.

There are concrete subclasses of :class:`arrow::Array` for each data type,
that help you access individual values of the array.

Building an array
=================

Available strategies
--------------------

As Arrow objects are immutable, they cannot be populated directly like for
example a ``std::vector``.  Instead, several strategies can be used:

* if the data already exists in memory with the right layout, you can wrap
  said memory inside :class:`arrow::Buffer` instances and then construct
  a :class:`arrow::ArrayData` describing the array;

  .. seealso:: :ref:`cpp_memory_management`

* otherwise, the :class:`arrow::ArrayBuilder` base class and its concrete
  subclasses help building up array data incrementally, without having to
  deal with details of the Arrow format yourself.

Using ArrayBuilder and its subclasses
-------------------------------------

To build an ``Int64`` Arrow array, we can use the :class:`arrow::Int64Builder`
class. In the following example, we build an array of the range 1 to 8 where
the element that should hold the value 4 is nulled::

   arrow::Int64Builder builder;
   builder.Append(1);
   builder.Append(2);
   builder.Append(3);
   builder.AppendNull();
   builder.Append(5);
   builder.Append(6);
   builder.Append(7);
   builder.Append(8);

   auto maybe_array = builder.Finish();
   if (!maybe_array.ok()) {
      // ... do something on array building failure
   }
   std::shared_ptr<arrow::Array> array = *maybe_array;

The resulting Array (which can be casted to the concrete :class:`arrow::Int64Array`
subclass if you want to access its values) then consists of two
:class:`arrow::Buffer`\s.
The first buffer holds the null bitmap, which consists here of a single byte with
the bits ``1|1|1|1|0|1|1|1``. As we use  `least-significant bit (LSB) numbering`_,
this indicates that the fourth entry in the array is null. The second
buffer is simply an ``int64_t`` array containing all the above values.
As the fourth entry is null, the value at that position in the buffer is
undefined.

Here is how you could access the concrete array's contents::

   // Cast the Array to its actual type to access its data
   auto int64_array = std::static_pointer_cast<arrow::Int64Array>(array);

   // Get the pointer to the null bitmap
   const uint8_t* null_bitmap = int64_array->null_bitmap_data();

   // Get the pointer to the actual data
   const int64_t* data = int64_array->raw_values();

   // Alternatively, given an array index, query its null bit and value directly
   int64_t index = 2;
   if (!int64_array->IsNull(index)) {
      int64_t value = int64_array->Value(index);
   }

.. note::
   :class:`arrow::Int64Array` (respectively :class:`arrow::Int64Builder`) is
   just a ``typedef``, provided for convenience, of ``arrow::NumericArray<Int64Type>``
   (respectively ``arrow::NumericBuilder<Int64Type>``).

.. _least-significant bit (LSB) numbering: https://en.wikipedia.org/wiki/Bit_numbering

Performance
-----------

While it is possible to build an array value-by-value as in the example above,
to attain highest performance it is recommended to use the bulk appending
methods (usually named ``AppendValues``) in the concrete :class:`arrow::ArrayBuilder`
subclasses.

If you know the number of elements in advance, it is also recommended to
presize the working area by calling the :func:`~arrow::ArrayBuilder::Resize`
or :func:`~arrow::ArrayBuilder::Reserve` methods.

Here is how one could rewrite the above example to take advantage of those
APIs::

   arrow::Int64Builder builder;
   // Make place for 8 values in total
   builder.Reserve(8);
   // Bulk append the given values (with a null in 4th place as indicated by the
   // validity vector)
   std::vector<bool> validity = {true, true, true, false, true, true, true, true};
   std::vector<int64_t> values = {1, 2, 3, 0, 5, 6, 7, 8};
   builder.AppendValues(values, validity);

   auto maybe_array = builder.Finish();

If you still must append values one by one, some concrete builder subclasses
have methods marked "Unsafe" that assume the working area has been correctly
presized, and offer higher performance in exchange::

   arrow::Int64Builder builder;
   // Make place for 8 values in total
   builder.Reserve(8);
   builder.UnsafeAppend(1);
   builder.UnsafeAppend(2);
   builder.UnsafeAppend(3);
   builder.UnsafeAppendNull();
   builder.UnsafeAppend(5);
   builder.UnsafeAppend(6);
   builder.UnsafeAppend(7);
   builder.UnsafeAppend(8);

   auto maybe_array = builder.Finish();

Size Limitations and Recommendations
====================================

Some array types are structurally limited to 32-bit sizes.  This is the case
for list arrays (which can hold up to 2^31 elements), string arrays and binary
arrays (which can hold up to 2GB of binary data), at least.  Some other array
types can hold up to 2^63 elements in the C++ implementation, but other Arrow
implementations can have a 32-bit size limitation for those array types as well.

For these reasons, it is recommended that huge data be chunked in subsets of
more reasonable size.

Chunked Arrays
==============

A :class:`arrow::ChunkedArray` is, like an array, a logical sequence of values;
but unlike a simple array, a chunked array does not require the entire sequence
to be physically contiguous in memory.  Also, the constituents of a chunked array
need not have the same size, but they must all have the same data type.

A chunked array is constructed by aggregating any number of arrays.  Here we'll
build a chunked array with the same logical values as in the example above,
but in two separate chunks::

   std::vector<std::shared_ptr<arrow::Array>> chunks;
   std::shared_ptr<arrow::Array> array;

   // Build first chunk
   arrow::Int64Builder builder;
   builder.Append(1);
   builder.Append(2);
   builder.Append(3);
   if (!builder.Finish(&array).ok()) {
      // ... do something on array building failure
   }
   chunks.push_back(std::move(array));

   // Build second chunk
   builder.Reset();
   builder.AppendNull();
   builder.Append(5);
   builder.Append(6);
   builder.Append(7);
   builder.Append(8);
   if (!builder.Finish(&array).ok()) {
      // ... do something on array building failure
   }
   chunks.push_back(std::move(array));

   auto chunked_array = std::make_shared<arrow::ChunkedArray>(std::move(chunks));

   assert(chunked_array->num_chunks() == 2);
   // Logical length in number of values
   assert(chunked_array->length() == 8);
   assert(chunked_array->null_count() == 1);

Slicing
=======

Like for physical memory buffers, it is possible to make zero-copy slices
of arrays and chunked arrays, to obtain an array or chunked array referring
to some logical subsequence of the data.  This is done by calling the
:func:`arrow::Array::Slice` and :func:`arrow::ChunkedArray::Slice` methods,
respectively.

