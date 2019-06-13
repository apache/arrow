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

Metadata: Logical types, schemas, data headers
==============================================

This is documentation for the Arrow metadata specification, which enables
systems to communicate the

* Logical array types (which are implemented using the physical memory layouts
  specified in :doc:`Layout`)

* Schemas for table-like collections of Arrow data structures

* "Data headers" indicating the physical locations of memory buffers sufficient
  to reconstruct a Arrow data structures without copying memory.

We are using `Flatbuffers`_ for low-overhead reading and writing of the Arrow
metadata. See ``Message.fbs``.

Schemas
-------

The ``Schema`` type describes a table-like structure consisting of any number of
Arrow arrays, each of which can be interpreted as a column in the table. A
schema by itself does not describe the physical structure of any particular set
of data.

A schema consists of a sequence of **fields**, which are metadata describing
the columns. The Flatbuffers IDL for a field is: ::

    table Field {
      // Name is not required, in i.e. a List
      name: string;
      nullable: bool;
      type: Type;

      // Present only if the field is dictionary encoded
      dictionary: DictionaryEncoding;

      // children apply only to Nested data types like Struct, List and Union
      children: [Field];

      // User-defined metadata
      custom_metadata: [ KeyValue ];
    }

The ``type`` is the logical type of the field. Nested types, such as List,
Struct, and Union, have a sequence of child fields.

Record Batch Data Headers
-------------------------

A record batch is a collection of top-level named, equal length Arrow arrays
(or vectors). If one of the arrays contains nested data, its child arrays are
not required to be the same length as the top-level arrays.

One can be thought of as a realization of a particular schema. The metadata
describing a particular record batch is called a "data header". Here is the
Flatbuffers IDL for a record batch data header: ::

    table RecordBatch {
      length: long;
      nodes: [FieldNode];
      buffers: [Buffer];
    }

The ``RecordBatch`` metadata provides for record batches with length exceeding
2 :sup:`31` - 1, but Arrow implementations are not required to implement support
beyond this size.

The ``nodes`` and ``buffers`` fields are produced by a depth-first traversal /
flattening of a schema (possibly containing nested types) for a given in-memory
data set.

Buffers
~~~~~~~

A buffer is metadata describing a contiguous memory region relative to some
virtual address space. This may include:

* Shared memory, e.g. a memory-mapped file
* An RPC message received in-memory
* Data in a file

The key form of the Buffer type is: ::

    struct Buffer {
      offset: long;
      length: long;
    }

In the context of a record batch, each field has some number of buffers
associated with it, which are derived from their physical memory layout.

Each logical type (separate from its children, if it is a nested type) has a
deterministic number of buffers associated with it. These will be specified in
the logical types section.

Field metadata
~~~~~~~~~~~~~~

The ``FieldNode`` values contain metadata about each level in a nested type
hierarchy. ::

    struct FieldNode {
      /// The number of value slots in the Arrow array at this level of a nested
      /// tree
      length: long;

      /// The number of observed nulls.
      null_count: lohng;
    }

The ``FieldNode`` metadata provides for fields with length exceeding 2 :sup:`31` - 1,
but Arrow implementations are not required to implement support for large
arrays.

Flattening of nested data
-------------------------

Nested types are flattened in the record batch in depth-first order. When
visiting each field in the nested type tree, the metadata is appended to the
top-level ``fields`` array and the buffers associated with that field (but not
its children) are appended to the ``buffers`` array.

For example, let's consider the schema ::

    col1: Struct<a: Int32, b: List<Int64>, c: Float64>
    col2: Utf8

The flattened version of this is: ::

    FieldNode 0: Struct name='col1'
    FieldNode 1: Int32 name=a'
    FieldNode 2: List name='b'
    FieldNode 3: Int64 name='item'  # arbitrary
    FieldNode 4: Float64 name='c'
    FieldNode 5: Utf8 name='col2'

For the buffers produced, we would have the following (as described in more
detail for each type below): ::

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

.. _spec-logical-types:

Logical types
-------------

A logical type consists of a type name and metadata along with an explicit
mapping to a physical memory representation. These may fall into some different
categories:

* Types represented as fixed-width primitive arrays (for example: C-style
  integers and floating point numbers)
* Types having equivalent memory layout to a physical nested type (e.g. strings
  use the list representation, but logically are not nested types)

Refer to `Schema.fbs`_ for up-to-date descriptions of each built-in
logical type.

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

Integration Testing
-------------------

A JSON representation of the schema is provided for cross-language
integration testing purposes.

Schema: ::

    {
      "fields" : [
        /* Field */
      ]
    }

Field: ::

    {
      "name" : "name_of_the_field",
      "nullable" : false,
      "type" : /* Type */,
      "children" : [ /* Field */ ],
    }

Type: ::

    {
      "name" : "null|struct|list|union|int|floatingpoint|utf8|binary|fixedsizebinary|bool|decimal|date|time|timestamp|interval"
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

Decimal: ::

    {
      "name" : "decimal",
      "precision" : /* integer */,
      "scale" : /* integer */
    }

Timestamp: ::

    {
      "name" : "timestamp",
      "unit" : "SECOND|MILLISECOND|MICROSECOND|NANOSECOND"
    }

Date: ::

    {
      "name" : "date",
      "unit" : "DAY|MILLISECOND"
    }

Time: ::

    {
      "name" : "time",
      "unit" : "SECOND|MILLISECOND|MICROSECOND|NANOSECOND",
      "bitWidth": /* integer: 32 or 64 */
    }

Interval: ::

    {
      "name" : "interval",
      "unit" : "YEAR_MONTH|DAY_TIME"
    }

.. _Flatbuffers: http://github.com/google/flatbuffers
.. _Schema.fbs: https://github.com/apache/arrow/blob/master/format/Schema.fbs
