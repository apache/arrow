Apache Arrow C++ API documentation      {#index}
==================================

<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

Apache Arrow is a columnar in-memory analytics layer designed to accelerate
big data. It houses a set of canonical in-memory representations of flat and
hierarchical data along with multiple language-bindings for structure
manipulation. It also provides IPC and common algorithm implementations.

This is the documentation of the C++ API of Apache Arrow. For more details
on the format and other language bindings see
the [main page for Arrow](https://arrow.apache.org/). Here will we only detail
the usage of the C++ API for Arrow and the leaf libraries that add additional
functionality such as using [jemalloc](http://jemalloc.net/) as an allocator
for Arrow structures.

Table of Contents
-----------------

 * Instructions on how to build Arrow C++ on [Windows](Windows.md)
 * How to access [HDFS](HDFS.md)

Getting Started
---------------

The most basic structure in Arrow is an `arrow::Array`. It holds a sequence
of values with known length all having the same type. It consists of the data
itself and an additional bitmap that indicates if the corresponding entry of
array is a null-value. Note that for array with zero null entries, we can omit
this bitmap.

As Arrow objects are immutable, there are classes provided that should help you
build these objects. To build an array of `int64_t` elements, we can use the
`arrow::Int64Builder`. In the following example, we build an array of the range
1 to 8 where the element that should hold the number 4 is nulled.

    Int64Builder builder(arrow::default_memory_pool(), arrow::int64());
    builder.Append(1);
    builder.Append(2);
    builder.Append(3);
    builder.AppendNull();
    builder.Append(5);
    builder.Append(6);
    builder.Append(7);
    builder.Append(8);

    std::shared_ptr<Array> array;
    builder.Finish(&array);

The resulting Array (which can be casted to `arrow::Int64Array` if you want
to access its values) then consists of two `arrow::Buffer`. The first one is
the null bitmap holding a single byte with the bits `0|0|0|0|1|0|0|0`.
As we use [least-significant bit (LSB) numbering](https://en.wikipedia.org/wiki/Bit_numbering)
this indicates that the fourth entry in the array is null. The second
buffer is simply an `int64_t` array containing all the above values.
As the fourth entry is null, the value at that position in the buffer is
undefined.

    // Cast the Array to its actual type to access its data
    std::shared_ptr<Int64Array> int64_array = std::static_pointer_cast<Int64Array>(array);

    // Get the pointer to the null bitmap.
    const uint8_t* null_bitmap = int64_array->null_bitmap_data();

    // Get the pointer to the actual data
    const int64_t* data = int64_array->raw_data();

In the above example, we have yet skipped explaining two things in the code.
On constructing the builder, we have passed `arrow::int64()` to it. This is
the type information with which the resulting array will be annotated. In
this simple form, it is solely a `std::shared_ptr<arrow::Int64Type>`
instantiation.

Furthermore, we have passed `arrow::default_memory_pool()` to the constructor.
This `arrow::MemoryPool` is used for the allocations of heap memory. Besides
tracking the amount of memory allocated, the allocator also ensures that the
allocated memory regions are 64-byte aligned (as required by the Arrow
specification).
