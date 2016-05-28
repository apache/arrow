# Arrow: Physical memory layout

## Definitions / Terminology

Since different projects have used differents words to describe various
concepts, here is a small glossary to help disambiguate.

* Array: a sequence of values with known length all having the same type.
* Slot or array slot: a single logical value in an array of some particular data type
* Contiguous memory region: a sequential virtual address space with a given
  length. Any byte can be reached via a single pointer offset less than the
  region's length.
* Contiguous memory buffer: A contiguous memory region that stores
  a multi-value component of an Array.  Sometimes referred to as just "buffer".
* Primitive type: a data type that occupies a fixed-size memory slot specified
  in bit width or byte width
* Nested or parametric type: a data type whose full structure depends on one or
  more other child relative types. Two fully-specified nested types are equal
  if and only if their child types are equal. For example, `List<U>` is distinct
  from `List<V>` iff U and V are different relative types.
* Relative type or simply type (unqualified): either a specific primitive type
  or a fully-specified nested type. When we say slot we mean a relative type
  value, not necessarily any physical storage region.
* Logical type: A data type that is implemented using some relative (physical)
  type. For example, a Decimal value stored in 16 bytes could be stored in a
  primitive array with slot size 16 bytes. Similarly, strings can be stored as
  `List<1-byte>`.
* Parent and child arrays: names to express relationships between physical
  value arrays in a nested type structure. For example, a `List<T>`-type parent
  array has a T-type array as its child (see more on lists below).
* Leaf node or leaf: A primitive value array that may or may not be a child
  array of some array with a nested type.

## Requirements, goals, and non-goals

Base requirements

* A physical memory layout enabling zero-deserialization data interchange
  amongst a variety of systems handling flat and nested columnar data, including
  such systems as Spark, Drill, Impala, Kudu, Ibis, Spark, ODBC protocols, and
  proprietary systems that utilize the open source components.
* All array slots are accessible in constant time, with complexity growing
  linearly in the nesting level
* Capable of representing fully-materialized and decoded / decompressed [Parquet][5]
  data
* All contiguous memory buffers are aligned at 64-byte boundaries and padded to a multiple of 64 bytes.
* Any relative type can have null slots
* Arrays are immutable once created. Implementations can provide APIs to mutate
  an array, but applying mutations will require a new array data structure to
  be built.
* Arrays are relocatable (e.g. for RPC/transient storage) without pointer
  swizzling. Another way of putting this is that contiguous memory regions can
  be migrated to a different address space (e.g. via a memcpy-type of
  operation) without altering their contents.

## Goals (for this document)

* To describe relative types (physical value types and a preliminary set of
  nested types) sufficient for an unambiguous implementation
* Memory layout and random access patterns for each relative type
* Null value representation

## Non-goals (for this document)

* To enumerate or specify logical types that can be implemented as primitive
  (fixed-width) value types. For example: signed and unsigned integers,
  floating point numbers, boolean, exact decimals, date and time types,
  CHAR(K), VARCHAR(K), etc.
* To specify standardized metadata or a data layout for RPC or transient file
  storage.
* To define a selection or masking vector construct
* Implementation-specific details
* Details of a user or developer C/C++/Java API.
* Any "table" structure composed of named arrays each having their own type or
  any other structure that composes arrays.
* Any memory management or reference counting subsystem
* To enumerate or specify types of encodings or compression support

## Byte Order ([Endianness][3])

The Arrow format is little endian.

## Alignment and Padding

As noted above, all buffers are intended to be aligned in memory at 64 byte
boundaries and padded to a length that is a multiple of 64 bytes.  The alignment
requirement follows best practices for optimized memory access:

* Elements in numeric arrays will be guaranteed to be retrieved via aligned access.
* On some architectures alignment can help limit partially used cache lines.
* 64 byte alignment is recommended by the [Intel performance guide][2] for
data-structures over 64 bytes (which will be a common case for Arrow Arrays).

Requiring padding to a multiple of 64 bytes allows for using [SIMD][4] instructions
consistently in loops without additional conditional checks.
This should allow for simpler and more efficient code.  
The specific padding length was chosen because it matches the largest known
SIMD instruction registers available as of April 2016 (Intel AVX-512).
Guaranteed padding can also allow certain compilers
to generate more optimized code directly (e.g. One can safely use Intel's
`-qopt-assume-safe-padding`).

Unless otherwise noted, padded bytes do not need to have a specific value.

## Array lengths

Any array has a known and fixed length, stored as a 32-bit signed integer, so a
maximum of 2<sup>31</sup> - 1 elements. We choose a signed int32 for a couple reasons:

* Enhance compatibility with Java and client languages which may have varying
  quality of support for unsigned integers.
* To encourage developers to compose smaller arrays (each of which contains
  contiguous memory in its leaf nodes) to create larger array structures
  possibly exceeding 2<sup>31</sup> - 1 elements, as opposed to allocating very large
  contiguous memory blocks.

## Null count

The number of null value slots is a property of the physical array and
considered part of the data structure. The null count is stored as a 32-bit
signed integer, as it may be as large as the array length.

## Null bitmaps

Any relative type can have null value slots, whether primitive or nested type.

An array with nulls must have a contiguous memory buffer, known as the null (or
validity) bitmap, whose length is a multiple of 64 bytes (as discussed above)  
and large enough to have at least 1 bit for each array
slot.

Whether any array slot is valid (non-null) is encoded in the respective bits of
this bitmap. A 1 (set bit) for index `j` indicates that the value is not null,
while a 0 (bit not set) indicates that it is null. Bitmaps are to be
initialized to be all unset at allocation time (this includes padding).

```
is_valid[j] -> bitmap[j / 8] & (1 << (j % 8))
```

We use [least-significant bit (LSB) numbering][1] (also known as
bit-endianness). This means that within a group of 8 bits, we read
right-to-left:

```
values = [0, 1, null, 2, null, 3]

bitmap
j mod 8   7  6  5  4  3  2  1  0
          0  0  1  0  1  0  1  1
```

Arrays having a 0 null count may choose to not allocate the null
bitmap. Implementations may choose to always allocate one anyway as a matter of
convenience, but this should be noted when memory is being shared.

Nested type arrays have their own null bitmap and null count regardless of
the null count and null bits of their child arrays.

## Primitive value arrays

A primitive value array represents a fixed-length array of values each having
the same physical slot width typically measured in bytes, though the spec also
provides for bit-packed types (e.g. boolean values encoded in bits).

Internally, the array contains a contiguous memory buffer whose total size is
equal to the slot width multiplied by the array length. For bit-packed types,
the size is rounded up to the nearest byte.

The associated null bitmap is contiguously allocated (as described above) but
does not need to be adjacent in memory to the values buffer.


### Example Layout: Int32 Array
For example a primitive array of int32s:

[1, 2, null, 4, 8]

Would look like:

```
* Length: 5, Null count: 1
* Null bitmap buffer:

  |Byte 0 (validity bitmap) | Bytes 1-63            |
  |-------------------------|-----------------------|
  |00011011                 | 0 (padding)           |

* Value Buffer:

  |Bytes 0-3   | Bytes 4-7   | Bytes 8-11  | Bytes 12-15 | Bytes 16-19 | Bytes 20-63 |
  |------------|-------------|-------------|-------------|-------------|-------------|
  | 1          | 2           | unspecified | 4           | 8           | unspecified |
```

### Example Layout: Non-null int32 Array

[1, 2, 3, 4, 8] has two possible layouts:

```
* Length: 5, Null count: 0
* Null bitmap buffer:

  | Byte 0 (validity bitmap) | Bytes 1-63            |
  |--------------------------|-----------------------|
  | 00011111                 | 0 (padding)           |

* Value Buffer:

  |Bytes 0-3   | Bytes 4-7   | Bytes 8-11  | bytes 12-15 | bytes 16-19 | Bytes 20-63 |
  |------------|-------------|-------------|-------------|-------------|-------------|
  | 1          | 2           | 3           | 4           | 8           | unspecified |
```

or with the bitmap elided:

```
* Length 5, Null count: 0
* Null bitmap buffer: Not required
* Value Buffer:

  |Bytes 0-3   | Bytes 4-7   | Bytes 8-11  | bytes 12-15 | bytes 16-19 | Bytes 20-63 |
  |------------|-------------|-------------|-------------|-------------|-------------|
  | 1          | 2           | 3           | 4           | 8           | unspecified |
```

## List type

List is a nested type in which each array slot contains a variable-size
sequence of values all having the same relative type (heterogeneity can be
achieved through unions, described later).

A list type is specified like `List<T>`, where `T` is any relative type
(primitive or nested).

A list-array is represented by the combination of the following:

* A values array, a child array of type T. T may also be a nested type.
* An offsets buffer containing 32-bit signed integers with length equal to the
  length of the top-level array plus one. Note that this limits the size of the
  values array to 2<sup>31</sup>-1.

The offsets array encodes a start position in the values array, and the length
of the value in each slot is computed using the first difference with the next
element in the offsets array. For example. the position and length of slot j is
computed as:

```
slot_position = offsets[j]
slot_length = offsets[j + 1] - offsets[j]  // (for 0 <= j < length)
```

The first value in the offsets array is 0, and the last element is the length
of the values array.

### Example Layout: `List<Char>` Array
Let's consider an example, the type `List<Char>`, where Char is a 1-byte
logical type.

For an array of length 4 with respective values:

[['j', 'o', 'e'], null, ['m', 'a', 'r', 'k'], []]

will have the following representation:

```
* Length: 4, Null count: 1
* Null bitmap buffer:

  | Byte 0 (validity bitmap) | Bytes 1-63            |
  |--------------------------|-----------------------|
  | 00001101                 | 0 (padding)           |

* Offsets buffer (int32)

  | Bytes 0-3  | Bytes 4-7   | Bytes 8-11  | Bytes 12-15 | Bytes 16-19 | Bytes 20-63 |
  |------------|-------------|-------------|-------------|-------------|-------------|
  | 0          | 3           | 3           | 7           | 7           | unspecified |

* Values array (char array):
  * Length: 7,  Null count: 0
  * Null bitmap buffer: Not required

    | Bytes 0-7  | Bytes 8-63  |
    |------------|-------------|
    | joemark    | unspecified |
```

### Example Layout: `List<List<byte>>`
[[[1, 2], [3, 4]], [[5, 6, 7], null, [8]], [[9, 10]]]

will be be represented as follows:

```
* Length 3
* Nulls count: 0
* Null bitmap buffer: Not required
* Offsets buffer (int32)

  | Bytes 0-3  | Bytes 4-7  | Bytes 8-11 | Bytes 12-15 | Bytes 16-63 |
  |------------|------------|------------|-------------|-------------|
  | 0          |  2         |  6         |  7          | unspecified |

* Values array (`List<byte>`)
  * Length: 6, Null count: 1
  * Null bitmap buffer:

    | Byte 0 (validity bitmap) | Bytes 1-63  |
    |--------------------------|-------------|
    | 00110111                 | 0 (padding) |

  * Offsets buffer (int32)

    | Bytes 0-28           | Bytes 29-63 |
    |----------------------|-------------|
    | 0, 2, 4, 7, 7, 8, 10 | unspecified |

  * Values array (bytes):
    * Length: 10, Null count: 0
    * Null bitmap buffer: Not required

      | Bytes 0-9                     | Bytes 10-63 |
      |-------------------------------|-------------|
      | 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 | unspecified |
```

## Struct type

A struct is a nested type parameterized by an ordered sequence of relative
types (which can all be distinct), called its fields.

Typically the fields have names, but the names and their types are part of the
type metadata, not the physical memory layout.

A struct array does not have any additional allocated physical storage for its values.
A struct array must still have an allocated null bitmap, if it has one or more null values.

Physically, a struct type has one child array for each field.

For example, the struct (field names shown here as strings for illustration
purposes)

```
Struct <
  name: String (= List<char>),
  age: Int32
>
```

has two child arrays, one List<char> array (layout as above) and one 4-byte
primitive value array having Int32 logical type.

### Example Layout: `Struct<List<char>, Int32>`:
The layout for [{'joe', 1}, {null, 2}, null, {'mark', 4}] would be:

```
* Length: 4, Null count: 1
* Null bitmap buffer:

  | Byte 0 (validity bitmap) | Bytes 1-7   | Bytes 8-63  |
  |--------------------------|-------------|-------------|
  | 00001011                 | 0 (padding) | unspecified |

* Children arrays:
  * field-0 array (`List<char>`):
    * Length: 4, Null count: 1
    * Null bitmap buffer:

      | Byte 0 (validity bitmap) | Bytes 1-7             |
      |--------------------------|-----------------------|
      | 00011101                 | 0 (padding)           |

    * Offsets buffer:

      | Bytes 0-19     |
      |----------------|
      | 0, 3, 3, 6, 10 |

     * Values array:
        * Length: 10, Null count: 0
        * Null bitmap buffer: Not required

        * Value buffer:

          | Bytes 0-9      |
          |----------------|
          | joebobmark     |

  * field-1 array (int32 array):
    * Length: 4, Null count: 0
    * Null bitmap buffer: Not required
    * Value Buffer:

      | Bytes 0-15     |
      |----------------|
      | 1, 2, 3, 4     |

```

While a struct does not have physical storage for each of its semantic slots
(i.e. each scalar C-like struct), an entire struct slot can be set to null via
the null bitmap. Any of the child field arrays can have null values according
to their respective independent null bitmaps.
This implies that for a particular struct slot the null bitmap for the struct
array might indicate a null slot when one or more of its child arrays has a
non-null value in their corresponding slot.  When reading the struct array the
parent null bitmap is authoritative.
This is illustrated in the example above, the child arrays have valid entries
for the null struct but are 'hidden' from the consumer by the parent array's
null bitmap.  However, when treated independently corresponding
values of the children array will be non-null.

## Dense union type

A dense union is semantically similar to a struct, and contains an ordered
sequence of relative types. While a struct contains multiple arrays, a union is
semantically a single array in which each slot can have a different type.

The union types may be named, but like structs this will be a matter of the
metadata and will not affect the physical memory layout.

We define two distinct union types that are optimized for different use
cases. This first, the dense union, represents a mixed-type array with 5 bytes
of overhead for each value. Its physical layout is as follows:

* One child array for each relative type
* Types buffer: A buffer of 8-bit signed integers, enumerated from 0 corresponding
  to each type.  A union with more then 127 possible types can be modeled as a
  union of unions. 
* Offsets buffer: A buffer of signed int32 values indicating the relative offset
  into the respective child array for the type in a given slot. The respective
  offsets for each child value array must be in order / increasing.

Critically, the dense union allows for minimal overhead in the ubiquitous
union-of-structs with non-overlapping-fields use case (`Union<s1: Struct1, s2:
Struct2, s3: Struct3, ...>`)

### Example Layout: Dense union

An example layout for logical union of:
`Union<f: float, i: int32>` having the values:
[{f=1.2}, null, {f=3.4}, {i=5}]

```
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


  * Field-1 array (f: float):
    * Length: 1, nulls: 0
    * Null bitmap buffer: Not required

    * Value Buffer:

      | Bytes 0-3 | Bytes 4-63  |
      |-----------|-------------|
      | 5         | unspecified |
```

## Sparse union type

A sparse union has the same structure as a dense union, with the omission of
the offsets array. In this case, the child arrays are each equal in length to
the length of the union.

While a sparse union may use significantly more space compared with a dense
union, it has some advantages that may be desirable in certain use cases:

* A sparse union is more amenable to vectorized expression evaluation in some use cases.
* Equal-length arrays can be interpreted as a union by only defining the types array.

### Example layout: `SparseUnion<u0: Int32, u1: Float, u2: List<Char>>`

For the union array:

[{u0=5}, {u1=1.2}, {u2='joe'}, {u1=3.4}, {u0=4}, 'mark']

will have the following layout:
```
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
      | 1          | unspecified | unspecified | unspecified | 4           |  unspecified | unspecified (padding) |

  * u1 (float):
    * Length: 6, Null count: 4
    * Null bitmap buffer:

      |Byte 0 (validity bitmap) | Bytes 1-63            |
      |-------------------------|-----------------------|
      |00001010                 | 0 (padding)           |

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

        | Bytes 0-7  | Bytes 8-63            |
        |------------|-----------------------|
        | joemark    | unspecified (padding) |
```

Note that nested types in a sparse union must be internally consistent
(e.g. see the List in the diagram), i.e. random access at any index j
on any child array will not cause an error.
In other words, the array for the nested type must be valid if it is
reinterpreted as a non-nested array.

Similar to structs, a particular child array may have a non-null slot
even if the null bitmap of the parent union array indicates the slot is
null.  Additionally, a child array may have a non-null slot even if
the the types array indicates that a slot contains a different type at the index.

## References

Apache Drill Documentation - [Value Vectors][6] 

[1]: https://en.wikipedia.org/wiki/Bit_numbering
[2]: https://software.intel.com/en-us/articles/practical-intel-avx-optimization-on-2nd-generation-intel-core-processors
[3]: https://en.wikipedia.org/wiki/Endianness
[4]: https://software.intel.com/en-us/node/600110
[5]: https://parquet.apache.org/documentation/latest/
[6]: https://drill.apache.org/docs/value-vectors/
