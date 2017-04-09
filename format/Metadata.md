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

# Metadata: Logical types, schemas, data headers

This is documentation for the Arrow metadata specification, which enables
systems to communicate the

* Logical array types (which are implemented using the physical memory layouts
  specified in [Layout.md][1])

* Schemas for table-like collections of Arrow data structures

* "Data headers" indicating the physical locations of memory buffers sufficient
  to reconstruct a Arrow data structures without copying memory.

## Canonical implementation

We are using [Flatbuffers][2] for low-overhead reading and writing of the Arrow
metadata. See [Message.fbs][3].

## Schemas

The `Schema` type describes a table-like structure consisting of any number of
Arrow arrays, each of which can be interpreted as a column in the table. A
schema by itself does not describe the physical structure of any particular set
of data.

A schema consists of a sequence of **fields**, which are metadata describing
the columns. The Flatbuffers IDL for a field is:

```
table Field {
  // Name is not required, in i.e. a List
  name: string;
  nullable: bool;
  type: Type;
  // present only if the field is dictionary encoded
  // will point to a dictionary provided by a DictionaryBatch message
  dictionary: long;
  // children apply only to Nested data types like Struct, List and Union
  children: [Field];
  /// layout of buffers produced for this type (as derived from the Type)
  /// does not include children
  /// each recordbatch will return instances of those Buffers.
  layout: [ VectorLayout ];
  // User-defined metadata
  custom_metadata: [ KeyValue ];
}
```

The `type` is the logical type of the field. Nested types, such as List,
Struct, and Union, have a sequence of child fields.

a JSON representation of the schema is also provided:
Field:
```
{
  "name" : "name_of_the_field",
  "nullable" : false,
  "type" : /* Type */,
  "children" : [ /* Field */ ],
  "typeLayout" : {
    "vectors" : [ /* VectorLayout */ ]
  }
}
```
VectorLayout:
```
{
  "type" : "DATA|OFFSET|VALIDITY|TYPE",
  "typeBitWidth" : /* int */
}
```
Type:
```
{
  "name" : "null|struct|list|union|int|floatingpoint|utf8|binary|fixedsizebinary|bool|decimal|date|time|timestamp|interval"
  // fields as defined in the Flatbuffer depending on the type name
}
```
Union:
```
{
  "name" : "union",
  "mode" : "Sparse|Dense",
  "typeIds" : [ /* integer */ ]
}
```

The `typeIds` field in the Union are the codes used to denote each type, which
may be different from the index of the child array. This is so that the union
type ids do not have to be enumerated from 0.

Int:
```
{
  "name" : "int",
  "bitWidth" : /* integer */,
  "isSigned" : /* boolean */
}
```
FloatingPoint:
```
{
  "name" : "floatingpoint",
  "precision" : "HALF|SINGLE|DOUBLE"
}
```
Decimal:
```
{
  "name" : "decimal",
  "precision" : /* integer */,
  "scale" : /* integer */
}
```

Timestamp:

```
{
  "name" : "timestamp",
  "unit" : "SECOND|MILLISECOND|MICROSECOND|NANOSECOND"
}
```

Date:

```
{
  "name" : "date",
  "unit" : "DAY|MILLISECOND"
}
```

Time:

```
{
  "name" : "time",
  "unit" : "SECOND|MILLISECOND|MICROSECOND|NANOSECOND",
  "bitWidth": /* integer: 32 or 64 */
}
```

Interval:

```
{
  "name" : "interval",
  "unit" : "YEAR_MONTH|DAY_TIME"
}
```
Schema:
```
{
  "fields" : [
    /* Field */
  ]
}
```

## Record data headers

A record batch is a collection of top-level named, equal length Arrow arrays
(or vectors). If one of the arrays contains nested data, its child arrays are
not required to be the same length as the top-level arrays.

One can be thought of as a realization of a particular schema. The metadata
describing a particular record batch is called a "data header". Here is the
Flatbuffers IDL for a record batch data header

```
table RecordBatch {
  length: long;
  nodes: [FieldNode];
  buffers: [Buffer];
}
```

The `RecordBatch` metadata provides for record batches with length exceeding
2^31 - 1, but Arrow implementations are not required to implement support
beyond this size.

The `nodes` and `buffers` fields are produced by a depth-first traversal /
flattening of a schema (possibly containing nested types) for a given in-memory
data set.

### Buffers

A buffer is metadata describing a contiguous memory region relative to some
virtual address space. This may include:

* Shared memory, e.g. a memory-mapped file
* An RPC message received in-memory
* Data in a file

The key form of the Buffer type is:

```
struct Buffer {
  offset: long;
  length: long;
}
```

In the context of a record batch, each field has some number of buffers
associated with it, which are derived from their physical memory layout.

Each logical type (separate from its children, if it is a nested type) has a
deterministic number of buffers associated with it. These will be specified in
the logical types section.

### Field metadata

The `FieldNode` values contain metadata about each level in a nested type
hierarchy.

```
struct FieldNode {
  /// The number of value slots in the Arrow array at this level of a nested
  /// tree
  length: long;

  /// The number of observed nulls.
  null_count: lohng;
}
```

The `FieldNode` metadata provides for fields with length exceeding 2^31 - 1,
but Arrow implementations are not required to implement support for large
arrays.

## Flattening of nested data

Nested types are flattened in the record batch in depth-first order. When
visiting each field in the nested type tree, the metadata is appended to the
top-level `fields` array and the buffers associated with that field (but not
its children) are appended to the `buffers` array.

For example, let's consider the schema

```
col1: Struct<a: Int32, b: List<Int64>, c: Float64>
col2: Utf8
```

The flattened version of this is:

```
FieldNode 0: Struct name='col1'
FieldNode 1: Int32 name=a'
FieldNode 2: List name='b'
FieldNode 3: Int64 name='item'  # arbitrary
FieldNode 4: Float64 name='c'
FieldNode 5: Utf8 name='col2'
```

For the buffers produced, we would have the following (as described in more
detail for each type below):

```
buffer 0: field 0 validity bitmap

buffer 1: field 1 validity bitmap
buffer 2: field 1 values <int32_t*>

buffer 3: field 2 validity bitmap
buffer 4: field 2 list offsets <int32_t*>

buffer 5: field 3 validity bitmap
buffer 6: field 3 values <int64_t*>

buffer 7: field 4 validity bitmap
buffer 8: field 4 values <double*>

buffer 9: field 5 validity bitmap
buffer 10: field 5 offsets <int32_t*>
buffer 11: field 5 data <uint8_t*>
```

## Logical types

A logical type consists of a type name and metadata along with an explicit
mapping to a physical memory representation. These may fall into some different
categories:

* Types represented as fixed-width primitive arrays (for example: C-style
  integers and floating point numbers)
* Types having equivalent memory layout to a physical nested type (e.g. strings
  use the list representation, but logically are not nested types)

### Integers

In the first version of Arrow we provide the standard 8-bit through 64-bit size
standard C integer types, both signed and unsigned:

* Signed types: Int8, Int16, Int32, Int64
* Unsigned types: UInt8, UInt16, UInt32, UInt64

The IDL looks like:

```
table Int {
  bitWidth: int;
  is_signed: bool;
}
```

The integer endianness is currently set globally at the schema level. If a
schema is set to be little-endian, then all integer types occurring within must
be little-endian. Integers that are part of other data representations, such as
list offsets and union types, must have the same endianness as the entire
record batch.

### Floating point numbers

We provide 3 types of floating point numbers as fixed bit-width primitive array

- Half precision, 16-bit width
- Single precision, 32-bit width
- Double precision, 64-bit width

The IDL looks like:

```
enum Precision:int {HALF, SINGLE, DOUBLE}

table FloatingPoint {
  precision: Precision;
}
```

### Boolean

The Boolean logical type is represented as a 1-bit wide primitive physical
type. The bits are numbered using least-significant bit (LSB) ordering.

Like other fixed bit-width primitive types, boolean data appears as 2 buffers
in the data header (one bitmap for the validity vector and one for the values).

### List

The `List` logical type is the logical (and identically-named) counterpart to
the List physical type.

In data header form, the list field node contains 2 buffers:

* Validity bitmap
* List offsets

The buffers associated with a list's child field are handled recursively
according to the child logical type (e.g. `List<Utf8>` vs. `List<Boolean>`).

### Utf8 and Binary

We specify two logical types for variable length bytes:

* `Utf8` data is unicode values with UTF-8 encoding
* `Binary` is any other variable length bytes

These types both have the same memory layout as the nested type `List<UInt8>`,
with the constraint that the inner bytes can contain no null values. From a
logical type perspective they are primitive, not nested types.

In data header form, while `List<UInt8>` would appear as 2 field nodes (`List`
and `UInt8`) and 4 buffers (2 for each of the nodes, as per above), these types
have a simplified representation single field node (of `Utf8` or `Binary`
logical type, which have no children) and 3 buffers:

* Validity bitmap
* List offsets
* Byte data

### Decimal

TBD

### Timestamp

All timestamps are stored as a 64-bit integer, with one of four unit
resolutions: second, millisecond, microsecond, and nanosecond.

### Date

We support two different date types:

* Days since the UNIX epoch as a 32-bit integer
* Milliseconds since the UNIX epoch as a 64-bit integer

### Time

Time supports the same unit resolutions: second, millisecond, microsecond, and
nanosecond. We represent time as the smallest integer accommodating the
indicated unit. For second and millisecond: 32-bit, for the others 64-bit.

## Dictionary encoding

[1]: https://github.com/apache/arrow/blob/master/format/Layout.md
[2]: http://github.com/google/flatbuffers
[3]: https://github.com/apache/arrow/blob/master/format/Message.fbs
