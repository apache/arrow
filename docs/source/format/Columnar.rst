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

.. _format_columnar:

*********************
Arrow Columnar Format
*********************

The "Arrow Columnar Format" includes a language-agnostic in-memory
data structure specification, metadata serialization, and a protocol
for serialization and generic data transport.

This document is intended to provide adequate detail to create a new
implementation of the columnar format without the aid of an existing
implementation. We utilize Google's `Flatbuffers`_ project for
metadata serialization, so it will be necessary to refer to the
project's `Flatbuffers protocol definition files <FlatbuffersFiles>`_
while reading this document.

The columnar format has some key features:

* Data adjacency for sequential access (scans)
* O(1) (constant-time) random access
* SIMD and vectorization-friendly
* Relocatable without "pointer swizzling", allowing for true zero-copy
  access in shared memory

Some things to keep in mind:

* Some data types can be mutated in-place in memory, but others
  cannot. The community has made deliberate trade-offs regarding
  in-place mutability to provide analytical performance and data
  locality guarantees

Terminology
===========

Since different projects have used different words to describe various
concepts, here is a small glossary to help disambiguate.

* **Array** or **Vector**: a sequence of values with known length all
  having the same type. These terms are used interchangeably in
  different Arrow implementations, but we use "array" in this
  document.
* **Slot**: a single logical value in an array of some particular data type
* **Buffer** or **Contiguous memory region**: a sequential virtual
  address space with a given length. Any byte can be reached via a
  single pointer offset less than the region's length.
* **Physical Layout type**: The underlying memory layout for an array
  without taking into account any value semantics. For example, a
  32-bit signed integer array and 32-bit floating point array have the
  same layout type.
* **Logical type**: An application-facing semantic value type that is
  implemented using some physical layout type. For example, Decimal
  values are stored as 16 bytes in a fixed-size binary
  layout. Similarly, strings can be stored as ``List<1-byte>``. A
  timestamp may be stored as 64-bit fixed-size layout.
* **Nested** or **parametric type**: a data type whose full structure
  depends on one or more other child types. Two fully-specified nested
  types are equal if and only if their child types are equal. For
  example, ``List<U>`` is distinct from ``List<V>`` iff U and V are
  different types.
* **Parent** and **child arrays**: names to express relationships
  between physical value arrays in a nested type structure. For
  example, a ``List<T>``-type parent array has a T-type array as its
  child (see more on lists below).
* **Leaf node** or **leaf**: A primitive value array that may or may
  not be a child array of some array with a nested type.

Physical Memory Layout
======================

Arrays are defined by a few pieces of metadata and data:

* A logical data type
* A sequence of buffers
* A length as a 64-bit signed integer. Implementations are permitted
  to be limited to 32-bit lengths, see more on this below
* A null count as a 64-bit signed integer
* An optional **dictionary**, for dictionary-encoded arrays

Nested arrays additionally have a sequence of one or more sets of
these items, called the **child arrays**.

Each logical data type has a well-defined physical layout. Here are
the different physical layouts defined by Arrow:

* **Primitive (fixed-size)**: a sequence of values each having the
  same byte or bit width
* **Variable-size Binary**: a sequence of values each having a variable
  byte length. Two variants of this layout are supported using 32-bit
  and 64-bit length encoding.
* **Fixed-size List**: a nested layout each each value has the same
  number of elements taken from a child data type.
* **Variable-size List**: a nested layout type where each value is a
  variable-length sequence of values taken from a child data type. Two
  variants of this layout are supported using 32-bit and 64-bit length
  encoding.
* **Struct**: a nested layout type consisting of a collection of child
  **fields** each having the same length
* **Sparse** and **Dense Union**: a nested layout type representing a
  sequence of values, each of which can have type chosen from a
  collection of child array types.
* **Null**: a sequence of all null values, having null logical type

Buffer Alignment and Padding
----------------------------

Implementations are recommended allocate memory on aligned (8- or
64-byte boundaries) and pad (overallocate) to a length that is a
multiple of 8 or 64 bytes. When serializing Arrow data for
interprocess communication, these alignment and padding requirements
are enforced. If possible, we suggest that you prefer using 64-byte
alignment and padding. Unless otherwise noted, padded bytes do not
need to have a specific value.

The alignment requirement follows best practices for optimized memory
access:

* Elements in numeric arrays will be guaranteed to be retrieved via aligned access.
* On some architectures alignment can help limit partially used cache lines.

The recommendation for 64 byte alignment comes from the `Intel
performance guide`_ that recommends alignment of memory to match SIMD
register width.  The specific padding length was chosen because it
matches the largest known SIMD instruction registers available as of
April 2016 (Intel AVX-512).

The recommended padding of 64 bytes allows for using `SIMD`_
instructions consistently in loops without additional conditional
checks.  This should allow for simpler, efficient and CPU
cache-friendly code.  In other words, we can load the entire 64-byte
buffer into a 512-bit wide SIMD register and get data-level
parallelism on all the columnar values packed into the 64-byte
buffer. Guaranteed padding can also allow certain compilers to
generate more optimized code directly (e.g. One can safely use Intel's
``-qopt-assume-safe-padding``).

Byte Order (`Endianness`_)
---------------------------

The Arrow format is little endian by default.

Serialized Schema metadata has an endianness field indicating
endianness of RecordBatches. Typically this is the endianness of the
system where the RecordBatch was generated. The main use case is
exchanging RecordBatches between systems with the same Endianness.  At
first we will return an error when trying to read a Schema with an
endianness that does not match the underlying system. The reference
implementation is focused on Little Endian and provides tests for
it. Eventually we may provide automatic conversion via byte swapping.

Array lengths
-------------

Array lengths are represented in the Arrow metadata as a 64-bit signed
integer. An implementation of Arrow is considered valid even if it only
supports lengths up to the maximum 32-bit signed integer, though. If using
Arrow in a multi-language environment, we recommend limiting lengths to
2 :sup:`31` - 1 elements or less. Larger data sets can be represented using
multiple array chunks.

Null count
----------

The number of null value slots is a property of the physical array and
considered part of the data structure. The null count is represented
in the Arrow metadata as a 64-bit signed integer, as it may be as
large as the array length.

Validity bitmaps
----------------

Any type can have null value slots, whether primitive or nested type.

An array with nulls must have a contiguous memory buffer, known as the
validity (or "null") bitmap, large enough to have at least 1 bit for
each array slot.

Whether any array slot is valid (non-null) is encoded in the respective bits of
this bitmap. A 1 (set bit) for index ``j`` indicates that the value is not null,
while a 0 (bit not set) indicates that it is null. Bitmaps are to be
initialized to be all unset at allocation time (this includes padding).::

    is_valid[j] -> bitmap[j / 8] & (1 << (j % 8))

We use `least-significant bit (LSB) numbering`_ (also known as
bit-endianness). This means that within a group of 8 bits, we read
right-to-left: ::

    values = [0, 1, null, 2, null, 3]

    bitmap
    j mod 8   7  6  5  4  3  2  1  0
              0  0  1  0  1  0  1  1

Arrays having a 0 null count may choose to not allocate the null
bitmap. Implementations may choose to always allocate one anyway as a matter of
convenience, but this should be noted when memory is being shared.

Nested type arrays have their own null bitmap and null count regardless of
the null count and null bits of their child arrays.

Primitive
---------

A primitive value array represents a fixed-length array of values each having
the same physical slot width typically measured in bytes, though the spec also
provides for bit-packed types (e.g. boolean values encoded in bits).

Internally, the array contains a contiguous memory buffer whose total size is
equal to the slot width multiplied by the array length. For bit-packed types,
the size is rounded up to the nearest byte.

The associated null bitmap is contiguously allocated (as described above) but
does not need to be adjacent in memory to the values buffer.

**Example Layout: Int32 Array**

For example a primitive array of int32s: ::

    [1, null, 2, 4, 8]

Would look like: ::

    * Length: 5, Null count: 1
    * Null bitmap buffer:

      |Byte 0 (validity bitmap) | Bytes 1-63            |
      |-------------------------|-----------------------|
      | 00011101                | 0 (padding)           |

    * Value Buffer:

      |Bytes 0-3   | Bytes 4-7   | Bytes 8-11  | Bytes 12-15 | Bytes 16-19 | Bytes 20-63 |
      |------------|-------------|-------------|-------------|-------------|-------------|
      | 1          | unspecified | 2           | 4           | 8           | unspecified |

**Example Layout: Non-null int32 Array**

``[1, 2, 3, 4, 8]`` has two possible layouts: ::

    * Length: 5, Null count: 0
    * Null bitmap buffer:

      | Byte 0 (validity bitmap) | Bytes 1-63            |
      |--------------------------|-----------------------|
      | 00011111                 | 0 (padding)           |

    * Value Buffer:

      |Bytes 0-3   | Bytes 4-7   | Bytes 8-11  | bytes 12-15 | bytes 16-19 | Bytes 20-63 |
      |------------|-------------|-------------|-------------|-------------|-------------|
      | 1          | 2           | 3           | 4           | 8           | unspecified |

or with the bitmap elided: ::

    * Length 5, Null count: 0
    * Null bitmap buffer: Not required
    * Value Buffer:

      |Bytes 0-3   | Bytes 4-7   | Bytes 8-11  | bytes 12-15 | bytes 16-19 | Bytes 20-63 |
      |------------|-------------|-------------|-------------|-------------|-------------|
      | 1          | 2           | 3           | 4           | 8           | unspecified |

Variable-size
-------------

Each value in this layout type consists of 0 or more bytes. While
primitive arrays have a single values buffer, variable-size binary
have an **offsets** buffer and **data** buffer

The offsets buffer contains `length + 1` signed integers (either
32-bit or 64-bit), which encode the start position of each element in
the data buffer. The length of the value in each slot is computed
using the first difference with the next element in the offsets
array. For example, the position and length of slot j is computed as:

::

    slot_position = offsets[j]
    slot_length = offsets[j + 1] - offsets[j]  // (for 0 <= j < length)

Generally the first value in the offsets array is 0, and the last
element is the length of the values array.

Variable-size List
------------------

List is a nested type which is semantically similar to variable-size
binary. It is defined by two buffers: a validity bitmap and an offsets
buffer. The offsets are the same as in the variable-size binary case,
and both 32-bit and 64-bit signed integer offsets are
supported. Rather than referencing an additional data buffer, instead
these offsets reference a child array having any type.

A list type is specified like ``List<T>``, where ``T`` is any type
(primitive or nested).

**Example Layout: ``List<Char>`` Array**

We illustrate ``List<Int8>``.

For an array of length 4 with respective values: ::

    [[12, -7, 25], null, [0, -127, 127, 50], []]

will have the following representation: ::

    * Length: 4, Null count: 1
    * Null bitmap buffer:

      | Byte 0 (validity bitmap) | Bytes 1-63            |
      |--------------------------|-----------------------|
      | 00001101                 | 0 (padding)           |

    * Offsets buffer (int32)

      | Bytes 0-3  | Bytes 4-7   | Bytes 8-11  | Bytes 12-15 | Bytes 16-19 | Bytes 20-63 |
      |------------|-------------|-------------|-------------|-------------|-------------|
      | 0          | 3           | 3           | 7           | 7           | unspecified |

    * Values array (Int8array):
      * Length: 7,  Null count: 0
      * Null bitmap buffer: Not required

        | Bytes 0-6                   | Bytes 7-63  |
        |-----------------------------|-------------|
        | 12, 7, 25, 0, -127, 127, 50 | unspecified |

**Example Layout: ``List<List<Int8>>``**

``[[[1, 2], [3, 4]], [[5, 6, 7], null, [8]], [[9, 10]]]``

will be represented as follows: ::

    * Length 3
    * Nulls count: 0
    * Null bitmap buffer: Not required
    * Offsets buffer (int32)

      | Bytes 0-3  | Bytes 4-7  | Bytes 8-11 | Bytes 12-15 | Bytes 16-63 |
      |------------|------------|------------|-------------|-------------|
      | 0          |  2         |  5         |  6          | unspecified |

    * Values array (`List<Int8>`)
      * Length: 6, Null count: 1
      * Null bitmap buffer:

        | Byte 0 (validity bitmap) | Bytes 1-63  |
        |--------------------------|-------------|
        | 00110111                 | 0 (padding) |

      * Offsets buffer (int32)

        | Bytes 0-27           | Bytes 28-63 |
        |----------------------|-------------|
        | 0, 2, 4, 7, 7, 8, 10 | unspecified |

      * Values array (Int8):
        * Length: 10, Null count: 0
        * Null bitmap buffer: Not required

          | Bytes 0-9                     | Bytes 10-63 |
          |-------------------------------|-------------|
          | 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 | unspecified |

Note that while the inner offsets buffer encodes the start position in
the inner values array, the outer offsets buffer encodes the start
position of corresponding outer element in the inner offsets buffer.

Fixed-Size List
---------------

Fixed-Size List is a nested type in which each array slot contains a
fixed-size sequence of values all having the same type (heterogeneity
can be achieved through unions, described later).

A fixed size list type is specified like ``FixedSizeList<T>[N]``,
where ``T`` is any type (primitive or nested) and ``N`` is a 32-bit
signed integer representing the length of the lists.

A fixed size list array is represented by a values array, which is a
child array of type T. T may also be a nested type. The value in slot
``j`` of a fixed size list array is stored in an ``N``-long slice of
the values array, starting at an offset of ``j * N``.

**Example Layout: ``FixedSizeList<byte>[4]`` Array**

Here we illustrate ``FixedSizeList<byte>[4]``.

For an array of length 4 with respective values: ::

    [[192, 168, 0, 12], null, [192, 168, 0, 25], [192, 168, 0, 1]]

will have the following representation: ::

    * Length: 4, Null count: 1
    * Null bitmap buffer:

      | Byte 0 (validity bitmap) | Bytes 1-63            |
      |--------------------------|-----------------------|
      | 00001101                 | 0 (padding)           |

    * Values array (byte array):
      * Length: 16,  Null count: 0
      * Null bitmap buffer: Not required

        | Bytes 0-3       | Bytes 4-7   | Bytes 8-15                      |
        |-----------------|-------------|---------------------------------|
        | 192, 168, 0, 12 | unspecified | 192, 168, 0, 25, 192, 168, 0, 1 |


Struct
------

A struct is a nested type parameterized by an ordered sequence of
types (which can all be distinct), called its fields. Typically the
fields have names, but the names and their types are part of the type
metadata, not the physical memory layout.

A struct array does not have any additional allocated physical storage
for its values.  A struct array must still have an allocated null
bitmap, if it has one or more null values.

Physically, a struct type has one child array for each field. The
child arrays are independent and need not be adjacent to each other in
memory.

For example, the struct (field names shown here as strings for illustration
purposes)::

    Struct <
      name: String (= List<char>),
      age: Int32
    >

has two child arrays, one ``List<char>`` array (layout as above) and one 4-byte
primitive value array having ``Int32`` logical type.

**Example Layout: ``Struct<List<char>, Int32>``**

The layout for ``[{'joe', 1}, {null, 2}, null, {'mark', 4}]`` would be: ::

    * Length: 4, Null count: 1
    * Null bitmap buffer:

      |Byte 0 (validity bitmap) | Bytes 1-63            |
      |-------------------------|-----------------------|
      | 00001011                | 0 (padding)           |

    * Children arrays:
      * field-0 array (`List<char>`):
        * Length: 4, Null count: 2
        * Null bitmap buffer:

          | Byte 0 (validity bitmap) | Bytes 1-63            |
          |--------------------------|-----------------------|
          | 00001001                 | 0 (padding)           |

        * Offsets buffer:

          | Bytes 0-19     |
          |----------------|
          | 0, 3, 3, 3, 7  |

         * Values array:
            * Length: 7, Null count: 0
            * Null bitmap buffer: Not required

            * Value buffer:

              | Bytes 0-6      |
              |----------------|
              | joemark        |

      * field-1 array (int32 array):
        * Length: 4, Null count: 1
        * Null bitmap buffer:

          | Byte 0 (validity bitmap) | Bytes 1-63            |
          |--------------------------|-----------------------|
          | 00001011                 | 0 (padding)           |

        * Value Buffer:

          |Bytes 0-3   | Bytes 4-7   | Bytes 8-11  | Bytes 12-15 | Bytes 16-63 |
          |------------|-------------|-------------|-------------|-------------|
          | 1          | 2           | unspecified | 4           | unspecified |

While a struct does not have physical storage for each of its semantic
slots (i.e. each scalar C-like struct), an entire struct slot can be
set to null via the null bitmap. Any of the child field arrays can
have null values according to their respective independent null
bitmaps. This implies that for a particular struct slot the null
bitmap for the struct array might indicate a null slot when one or
more of its child arrays has a non-null value in their corresponding
slot.  When reading the struct array the parent null bitmap is
authoritative.  This is illustrated in the example above, the child
arrays have valid entries for the null struct but are 'hidden' from
the consumer by the parent array's null bitmap.  However, when treated
independently corresponding values of the children array will be
non-null.

Dense Union
-----------

A dense union is semantically similar to a struct, and contains an
ordered sequence of types. While a struct contains multiple arrays, a
union is semantically a single array in which each slot can have a
different type.

The union types may be named, but like structs this will be a matter
of the metadata and will not affect the physical memory layout.

We define two distinct union types that are optimized for different use
cases. This first, the dense union, represents a mixed-type array with 5 bytes
of overhead for each value. Its physical layout is as follows:

* One child array for each type
* Types buffer: A buffer of 8-bit signed integers, enumerated from 0 corresponding
  to each type.  A union with more then 127 possible types can be modeled as a
  union of unions.
* Offsets buffer: A buffer of signed int32 values indicating the relative offset
  into the respective child array for the type in a given slot. The respective
  offsets for each child value array must be in order / increasing.

Critically, the dense union allows for minimal overhead in the ubiquitous
union-of-structs with non-overlapping-fields use case (``Union<s1: Struct1, s2:
Struct2, s3: Struct3, ...>``)

**Example Layout: Dense union**

An example layout for logical union of: ``Union<f: float, i: int32>``
having the values: ``[{f=1.2}, null, {f=3.4}, {i=5}]``

::

    * Length: 4, Null count: 1
    * Null bitmap buffer:
      |Byte 0 (validity bitmap) | Bytes 1-63            |
      |-------------------------|-----------------------|
      |00001101                 | 0 (padding)           |

    * Types buffer:

      |Byte 0   | Byte 1      | Byte 2   | Byte 3   | Bytes 4-63  |
      |---------|-------------|----------|----------|-------------|
      | 0       | unspecified | 0        | 1        | unspecified |

    * Offset buffer:

      |Byte 0-3 | Byte 4-7    | Byte 8-11 | Byte 12-15 | Bytes 16-63 |
      |---------|-------------|-----------|------------|-------------|
      | 0       | unspecified | 1         | 0          | unspecified |

    * Children arrays:
      * Field-0 array (f: float):
        * Length: 2, nulls: 0
        * Null bitmap buffer: Not required

        * Value Buffer:

          | Bytes 0-7 | Bytes 8-63  |
          |-----------|-------------|
          | 1.2, 3.4  | unspecified |


      * Field-1 array (i: int32):
        * Length: 1, nulls: 0
        * Null bitmap buffer: Not required

        * Value Buffer:

          | Bytes 0-3 | Bytes 4-63  |
          |-----------|-------------|
          | 5         | unspecified |

Sparse Union
------------

A sparse union has the same structure as a dense union, with the omission of
the offsets array. In this case, the child arrays are each equal in length to
the length of the union.

While a sparse union may use significantly more space compared with a
dense union, it has some advantages that may be desirable in certain
use cases:

* A sparse union is more amenable to vectorized expression evaluation in some use cases.
* Equal-length arrays can be interpreted as a union by only defining the types array.

**Example layout: ``SparseUnion<u0: Int32, u1: Float, u2: List<Char>>``**

For the union array: ::

    [{u0=5}, {u1=1.2}, {u2='joe'}, {u1=3.4}, {u0=4}, {u2='mark'}]

will have the following layout: ::

    * Length: 6, Null count: 0
    * Null bitmap buffer: Not required

    * Types buffer:

     | Byte 0     | Byte 1      | Byte 2      | Byte 3      | Byte 4      | Byte 5       | Bytes  6-63           |
     |------------|-------------|-------------|-------------|-------------|--------------|-----------------------|
     | 0          | 1           | 2           | 1           | 0           | 2            | unspecified (padding) |

    * Children arrays:

      * u0 (Int32):
        * Length: 6, Null count: 4
        * Null bitmap buffer:

          |Byte 0 (validity bitmap) | Bytes 1-63            |
          |-------------------------|-----------------------|
          |00010001                 | 0 (padding)           |

        * Value buffer:

          |Bytes 0-3   | Bytes 4-7   | Bytes 8-11  | Bytes 12-15 | Bytes 16-19 | Bytes 20-23  | Bytes 24-63           |
          |------------|-------------|-------------|-------------|-------------|--------------|-----------------------|
          | 5          | unspecified | unspecified | unspecified | 4           |  unspecified | unspecified (padding) |

      * u1 (float):
        * Length: 6, Null count: 4
        * Null bitmap buffer:

          |Byte 0 (validity bitmap) | Bytes 1-63            |
          |-------------------------|-----------------------|
          | 00001010                | 0 (padding)           |

        * Value buffer:

          |Bytes 0-3    | Bytes 4-7   | Bytes 8-11  | Bytes 12-15 | Bytes 16-19 | Bytes 20-23  | Bytes 24-63           |
          |-------------|-------------|-------------|-------------|-------------|--------------|-----------------------|
          | unspecified |  1.2        | unspecified | 3.4         | unspecified |  unspecified | unspecified (padding) |

      * u2 (`List<char>`)
        * Length: 6, Null count: 4
        * Null bitmap buffer:

          | Byte 0 (validity bitmap) | Bytes 1-63            |
          |--------------------------|-----------------------|
          | 00100100                 | 0 (padding)           |

        * Offsets buffer (int32)

          | Bytes 0-3  | Bytes 4-7   | Bytes 8-11  | Bytes 12-15 | Bytes 16-19 | Bytes 20-23 | Bytes 24-27 | Bytes 28-63 |
          |------------|-------------|-------------|-------------|-------------|-------------|-------------|-------------|
          | 0          | 0           | 0           | 3           | 3           | 3           | 7           | unspecified |

        * Values array (char array):
          * Length: 7,  Null count: 0
          * Null bitmap buffer: Not required

            | Bytes 0-6  | Bytes 7-63            |
            |------------|-----------------------|
            | joemark    | unspecified (padding) |

Note that nested types in a sparse union must be internally consistent
(e.g. see the List in the diagram), i.e. random access at any index j
on any child array will not cause an error.  In other words, the array
for the nested type must be valid if it is reinterpreted as a
non-nested array.

Similar to structs, a particular child array may have a non-null slot
even if the null bitmap of the parent union array indicates the slot
is null.  Additionally, a child array may have a non-null slot even if
the types array indicates that a slot contains a different type at the
index.

Null
----

We provide a simplified memory-efficient layout for the Null data type
where all values are null. In this case no memory buffers are
allocated.

Dictionary encoding
-------------------

Dictionary encoding is a data representation technique to represent
values by integers referencing a **dictionary** usually consisting of
unique values. It can be effective when you have data with many
repeated values.

Any array can be dictionary-encoded. The dictionary is stored as an
optional property of an array. When a field is dictionary encoded, the
values are represented by an array of signed integers representing the
index of the value in the dictionary.

As an example, you could have the following data: ::

    type: String

    ['foo', 'bar', 'foo', 'bar', null, 'baz']

In dictionary-encoded form, this could appear as:

::

    data String (dictionary-encoded)
       index_type: Int32
       values: [0, 1, 0, 1, null, 2]

    dictionary
       type: String
       values: ['foo', 'bar', 'baz']

We discuss dictionary encoding as it relates to serialization further
below.

Buffer Layout Listing
---------------------

For the avoidance of ambiguity, we provide listing the order and type
of memory buffers for each layout type.

.. csv-table:: Buffer Layouts
   :header: "Layout Type", "Buffer 0", "Buffer 1", "Buffer 2"
   :widths: 30, 20, 20, 20

   "Primitive",validity,data,
   "Variable Binary",validity,offsets,data
   "List",validity,offsets,
   "Fixed-size List",validity,,
   "Struct",validity,,
   "Sparse Union",validity,type ids,
   "Dense Union",validity,type ids,offsets
   "Null",placeholder (length 0),,

Logical Types
=============

The `Schema.fbs`_ defines built-in logical types supported by Apache
Arrow. Each logical type uses one of the above physical
layouts. Parametric or nested logical types may have different
physical layouts depending on the particular realization of the type.

We do not go into detail about the logical types definitions in this
document as we consider `Schema.fbs`_ to be the ultimate point of
truth.

Serialization and Interprocess Communication
============================================

The primitive unit of serialized data in Apache Arrow is the "record
batch". Semantically, a record batch is a collection of independent
arrays each having the same length as one another but potentially
different data types

In this section we define a protocol for serializing record batches
into a stream of binary payloads and reconstructing record batches
from these payloads without need for memory copying.

Metadata serialization
----------------------

The Flatbuffers files `Schema.fbs`_ contains the definitions for all
built-in logical data types and the ``Schema`` metadata type which
represents the schema of a given record batch. A schema consists of
names, logical types, and child fields for each top-level schema
field.

The ``Schema`` message contains no data whatsoever. We provide both
schema-level and field-level ``custom_metadata`` attributes allowing
for systems to insert their own application defined metadata to
customize behavior.

The ``Field`` Flatbuffers type contains the metadata for a single
array. This includes:

* The field's name
* The field's logical type
* Whether the field is semantically nullable. Note that this has no
  bearing on the array's layout
* A collection of child ``Field`` values, for nested types
* A ``dictionary`` property indicating whether the field is
  dictionary-encoded or not

RecordBatch serialization
-------------------------

A record batch can be considered to be the realization of a
schema. The serialized form of the record batch is the following:

* The ``data header``, defined as the ``RecordBatch`` type in
  `Message.fbs`_.
* The ``body``, a flat sequence of memory buffers written end-to-end
  with appropriate padding to ensure a minimum of 8-byte alignment

The data header contains the following:

* The length and null count for each flattened field in the record
  batch
* The memory offset and length of each constituent ``Buffer`` in the
  record batch's body

Fields and buffers are flattened by a pre-order depth-first traversal
of the fields in the record batch. For example, let's consider the
schema ::

    col1: Struct<a: Int32, b: List<item: Int64>, c: Float64>
    col2: Utf8

The flattened version of this is: ::

    FieldNode 0: Struct name='col1'
    FieldNode 1: Int32 name=a'
    FieldNode 2: List name='b'
    FieldNode 3: Int64 name='item'
    FieldNode 4: Float64 name='c'
    FieldNode 5: Utf8 name='col2'

For the buffers produced, we would have the following (refer to the
table above): ::

    buffer 0: field 0 validity
    buffer 1: field 1 validity
    buffer 2: field 1 values
    buffer 3: field 2 validity
    buffer 4: field 2 offsets
    buffer 5: field 3 validity
    buffer 6: field 3 values
    buffer 7: field 4 validity
    buffer 8: field 4 values
    buffer 9: field 5 validity
    buffer 10: field 5 offsets
    buffer 11: field 5 data

The ``Buffer`` Flatbuffers value describes the location and size of a
piece of memory. Generally these are interpreted relative to the
**encapsulated message format** defined next.

Encapsulated message format
---------------------------

For simple streaming and file-based serialization, we define a
so-called "encapsulated" message format for interprocess
communication. Such messages can be "deserialized" into in-memory
Arrow array objects by examining only the message metadata without any
need to copy or move any of the actual data.

The encapsulated binary message format is as follows:

* A 32-bit continuation indicator. The value ``0xFFFFFFFF`` indicates
  a valid message. This component was introduced in version 0.15.0 in
  part to address the 8-byte alignment requirement of Flatbuffers
* A 32-bit little-endian length prefix indicating the metadata size
* The message metadata as a Flatbuffer
* Padding bytes to an 8-byte boundary
* The message body, which must be a multiple of 8 bytes

Schematically, we have: ::

    <continuation: 0xFFFFFFFF>
    <metadata_size: int32>
    <metadata_flatbuffer: bytes>
    <padding>
    <message body>

The complete serialized message must be a multiple of 8 bytes so that messages
can be relocated between streams. Otherwise the amount of padding between the
metadata and the message body could be non-deterministic.

The ``metadata_size`` includes the size of the flatbuffer plus padding. The
``Message`` flatbuffer includes a version number, the particular message (as a
flatbuffer union), and the size of the message body:

Currently, we support 4 types of messages:

* Schema
* RecordBatch
* DictionaryBatch
* Tensor

Streaming format
----------------

We provide a streaming format for record batches. It is presented as a sequence
of encapsulated messages, each of which follows the format above. The schema
comes first in the stream, and it is the same for all of the record batches
that follow. If any fields in the schema are dictionary-encoded, one or more
``DictionaryBatch`` messages will be included. ``DictionaryBatch`` and
``RecordBatch`` messages may be interleaved, but before any dictionary key is used
in a ``RecordBatch`` it should be defined in a ``DictionaryBatch``. ::

    <SCHEMA>
    <DICTIONARY 0>
    ...
    <DICTIONARY k - 1>
    <RECORD BATCH 0>
    ...
    <DICTIONARY x DELTA>
    ...
    <DICTIONARY y DELTA>
    ...
    <RECORD BATCH n - 1>
    <EOS [optional]: 0x0000000000000000>

When a stream reader implementation is reading a stream, after each
message, it may read the next 8 bytes to determine both if the stream
continues and the size of the message metadata that follows. Once the
message flatbuffer is read, you can then read the message body.

The stream writer can signal end-of-stream (EOS) either by writing 8
zero (`0x00`) bytes or closing the stream interface.

File format
-----------

We define a "file format" supporting random access in a very similar format to
the streaming format. The file starts and ends with a magic string ``ARROW1``
(plus padding). What follows in the file is identical to the stream format. At
the end of the file, we write a *footer* containing a redundant copy of the
schema (which is a part of the streaming format) plus memory offsets and sizes
for each of the data blocks in the file. This enables random access any record
batch in the file. See ``File.fbs`` for the precise details of the file
footer.

Schematically we have: ::

    <magic number "ARROW1">
    <empty padding bytes [to 8 byte boundary]>
    <STREAMING FORMAT with EOS>
    <FOOTER>
    <FOOTER SIZE: int32>
    <magic number "ARROW1">

In the file format, there is no requirement that dictionary keys should be
defined in a ``DictionaryBatch`` before they are used in a ``RecordBatch``, as long
as the keys are defined somewhere in the file.

Dictionary Encoding
-------------------

Dictionaries are written in the stream and file formats as a sequence of record
batches, each having a single field. The complete semantic schema for a
sequence of record batches, therefore, consists of the schema along with all of
the dictionaries. The dictionary types are found in the schema, so it is
necessary to read the schema to first determine the dictionary types so that
the dictionaries can be properly interpreted. ::

    table DictionaryBatch {
      id: long;
      data: RecordBatch;
      isDelta: boolean = false;
    }

The dictionary ``id`` in the message metadata can be referenced one or more times
in the schema, so that dictionaries can even be used for multiple fields. See
the :doc:`Layout` document for more about the semantics of
dictionary-encoded data.

The dictionary ``isDelta`` flag allows dictionary batches to be modified
mid-stream.  A dictionary batch with ``isDelta`` set indicates that its vector
should be concatenated with those of any previous batches with the same ``id``. A
stream which encodes one column, the list of strings
``["A", "B", "C", "B", "D", "C", "E", "A"]``, with a delta dictionary batch could
take the form: ::

    <SCHEMA>
    <DICTIONARY 0>
    (0) "A"
    (1) "B"
    (2) "C"

    <RECORD BATCH 0>
    0
    1
    2
    1

    <DICTIONARY 0 DELTA>
    (3) "D"
    (4) "E"

    <RECORD BATCH 1>
    3
    2
    4
    0
    EOS

Custom Application Metadata
---------------------------

We provide a ``custom_metadata`` field at three levels to provide a
mechanism for developers to pass application-specific metadata in
Arrow protocol messages. This includes ``Field``, ``Schema``, and
``Message``.

The colon symbol ``:`` is to be used as a namespace separator. It can
be used multiple times in a key.

The ``ARROW`` pattern is a reserved namespace for internal Arrow use
in the ``custom_metadata`` fields. For example,
``ARROW:extension:name``.

Extension Types
---------------

User-defined "extension" types can be defined setting certain
``KeyValue`` pairs in ``custom_metadata`` in the ``Field`` metadata
structure. These extension keys are:

* ``'ARROW:extension:name'`` for the string name identifying the
  custom data type. We recommend that you use a "namespace"-style
  prefix for extension type names to minimize the possibility of
  conflicts with multiple Arrow readers and writers in the same
  application. For example, use ``myorg.name_of_type`` instead of
  simply ``name_of_type``
* ``'ARROW:extension:metadata'`` for a serialized representation
  of the ``ExtensionType`` necessary to reconstruct the custom type

This extension metadata can annotate any of the built-in Arrow logical
types. The intent is that an implementation that does not support an
extension type can still handle the underlying data. For example a
16-byte UUID value could be embedded in ``FixedSizeBinary(16)``, and
implementations that do not have this extension type can still work
with the underlying binary values and pass along the
``custom_metadata`` in subsequent Arrow protocol messages.

Extension types may or may not use the
``'ARROW:extension:metadata'`` field. Let's consider some example
extension types:

* ``uuid`` represented as ``FixedSizeBinary(16)`` with empty metadata
* ``latitude-longitude`` represented as ``struct<latitude: double,
  longitude: double>``, and empty metadata
* ``tensor`` (multidimensional array) stored as ``Binary`` values and
  having serialized metadata indicating the data type and shape of
  each value. This could be JSON like ``{'type': 'int8', 'shape': [4,
  5]}`` for a 4x5 cell tensor.
* ``trading-time`` represented as ``Timestamp`` with serialized
  metadata indicating the market trading calendar the data corresponds
  to

Implementation guidelines
=========================

An execution engine (or framework, or UDF executor, or storage engine,
etc) can implement only a subset of the Arrow spec and/or extend it
given the following constraints:

Implementing a subset the spec
------------------------------

* **If only producing (and not consuming) arrow vectors**: Any subset
  of the vector spec and the corresponding metadata can be implemented.
* **If consuming and producing vectors**: There is a minimal subset of
  vectors to be supported.  Production of a subset of vectors and
  their corresponding metadata is always fine.  Consumption of vectors
  should at least convert the unsupported input vectors to the
  supported subset (for example Timestamp.millis to timestamp.micros
  or int32 to int64).

Extensibility
-------------

An execution engine implementor can also extend their memory
representation with their own vectors internally as long as they are
never exposed. Before sending data to another system expecting Arrow
data, these custom vectors should be converted to a type that exist in
the Arrow spec.  An example of this is operating on compressed data.
These custom vectors are not exchanged externally and there is no
support for custom metadata.

Integration Testing
===================

A JSON representation of the schema is provided for cross-language
integration testing purposes.

The high level structure of a JSON integration test files is as follows

**Data file** ::

    {
      "schema": /*Schema*/,
      "batches": [ /*RecordBatch*/ ],
      "dictionaries": [ /*DictionaryBatch*/ ],
    }

**Schema** ::

    {
      "fields" : [
        /* Field */
      ]
    }

**Field** ::

    {
      "name" : "name_of_the_field",
      "nullable" : false,
      "type" : /* Type */,
      "children" : [ /* Field */ ],
    }

RecordBatch and DictionaryBatch
-------------------------------

**RecordBatch**::

    {
      "count": /*length of batch*/,
      "columns": [ /* FieldData */ ]
    }

**FieldData**::

    {
      "name": "field_name",
      "count" "field_length",
      "BUFFER_TYPE": /* BufferData */
      ...
      "BUFFER_TYPE": /* BufferData */
      "children": [ /* FieldData */ ]
    }

Here ``BUFFER_TYPE`` is one of ``VALIDITY``, ``OFFSET`` (for
variable-length types), ``TYPE`` (for unions), or ``DATA``.

``BufferData`` is encoded based on the type of buffer:

* ``VALIDITY``: a JSON array of 1 (valid) and 0 (null)
* ``OFFSET``: a JSON array of integers for 32-bit offsets or
  string-formatted integers for 64-bit offsets
* ``TYPE``: a JSON array of integers
* ``DATA``: a JSON array of encoded values

The value encoding for ``DATA`` is different depending on the logical
type:

* For boolean type: an array of 1 (true) and 0 (false)
* For integer-based types (including timestamps): an array of integers
* For 64-bit integers: an array of integers formatted as JSON strings
  to avoid loss of precision
* For floating point types: as is
* For Binary types, a hex-string is produced to encode a variable- or
  fixed-size binary value

Logical Types
-------------

Type: ::

    {
      "name" : "null|struct|list|largelist|fixedsizelist|union|int|floatingpoint|utf8|largeutf8|binary|largebinary|fixedsizebinary|bool|decimal|date|time|timestamp|interval|duration|map"
      // fields as defined in the Flatbuffer depending on the type name
    }

Union: ::

    {
      "name" : "union",
      "mode" : "Sparse|Dense",
      "typeIds" : [ /* integer */ ]
    }

The ``typeIds`` field in the Union are the codes used to denote each type, which
may be different from the index of the child array. This is so that the union
type ids do not have to be enumerated from 0.

Int: ::

    {
      "name" : "int",
      "bitWidth" : /* integer */,
      "isSigned" : /* boolean */
    }

FloatingPoint: ::

    {
      "name" : "floatingpoint",
      "precision" : "HALF|SINGLE|DOUBLE"
    }

FixedSizeBinary: ::

    {
      "name" : "fixedsizebinary",
      "byteWidth" : /* byte width */
    }

Decimal: ::

    {
      "name" : "decimal",
      "precision" : /* integer */,
      "scale" : /* integer */
    }

Timestamp: ::

    {
      "name" : "timestamp",
      "unit" : "$TIME_UNIT"
      "timezone": "$timezone" [optional]
    }

``$TIME_UNIT`` is one of ``"SECOND|MILLISECOND|MICROSECOND|NANOSECOND"``

Duration: ::

    {
      "name" : "duration",
      "unit" : "$TIME_UNIT"
    }

Date: ::

    {
      "name" : "date",
      "unit" : "DAY|MILLISECOND"
    }

Time: ::

    {
      "name" : "time",
      "unit" : "$TIME_UNIT",
      "bitWidth": /* integer: 32 or 64 */
    }

Interval: ::

    {
      "name" : "interval",
      "unit" : "YEAR_MONTH"
    }

References
----------
* Apache Drill Documentation - `Value Vectors`_

.. _Flatbuffers: http://github.com/google/flatbuffers
.. _FlatbuffersFiles: https://github.com/apache/arrow/tree/master/format
.. _Schema.fbs: https://github.com/apache/arrow/blob/master/format/Schema.fbs
.. _Message.fbs: https://github.com/apache/arrow/blob/master/format/Message.fbs
.. _least-significant bit (LSB) numbering: https://en.wikipedia.org/wiki/Bit_numbering
.. _Intel performance guide: https://software.intel.com/en-us/articles/practical-intel-avx-optimization-on-2nd-generation-intel-core-processors
.. _Endianness: https://en.wikipedia.org/wiki/Endianness
.. _SIMD: https://software.intel.com/en-us/cpp-compiler-developer-guide-and-reference-introduction-to-the-simd-data-layout-templates
.. _Parquet: https://parquet.apache.org/documentation/latest/
.. _Value Vectors: https://drill.apache.org/docs/value-vectors/
