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

.. _c-data-interface:

==========================
The Arrow C data interface
==========================

Rationale
=========

Apache Arrow is designed to be a universal in-memory format for the representation
of tabular ("columnar") data. However, some projects may face a difficult
choice between either depending on a fast-evolving project such as the
Arrow C++ library, or having to reimplement adapters for data interchange,
which may require significant, redundant development effort.

The Arrow C data interface defines a very small, stable set of C definitions
that can be easily *copied* in any project's source code and used for columnar
data interchange in the Arrow format.  For non-C/C++ languages and runtimes,
it should be almost as easy to translate the C definitions into the
corresponding C FFI declarations.

Applications and libraries can therefore work with Arrow memory without
necessarily using Arrow libraries or reinventing the wheel. Developers can
choose between tight integration
with the Arrow *software project* (benefitting from the growing array of
facilities exposed by e.g. the C++ or Java implementations of Apache Arrow,
but with the cost of a dependency) or minimal integration with the Arrow
*format* only.

Goals
-----

* Expose an ABI-stable interface.
* Make it easy for third-party projects to implement support for (including partial
  support where sufficient), with little initial investment.
* Allow zero-copy sharing of Arrow data between independent runtimes
  and components running in the same process.
* Match the Arrow array concepts closely to avoid the development of
  yet another marshalling layer.
* Avoid the need for one-to-one adaptation layers such as the limited
  JPype-based bridge between Java and Python.
* Enable integration without an explicit dependency (either at compile-time
  or runtime) on the Arrow software project.

Ideally, the Arrow C data interface can become a low-level *lingua franca*
for sharing columnar data at runtime and establish Arrow as the universal
building block in the columnar processing ecosystem.

Non-goals
---------

* Expose a C API mimicking operations available in higher-level runtimes
  (such as C++, Java...).
* Data sharing between distinct processes or storage persistence.


Comparison with the Arrow IPC format
------------------------------------

Pros of the C data interface vs. the IPC format:

* No dependency on Flatbuffers.
* No buffer reassembly (data is already exposed in logical Arrow format).
* Zero-copy by design.
* Easy to reimplement from scratch.
* Minimal C definition that can be easily copied into other codebases.
* Resource lifetime management through a custom release callback.

Pros of the IPC format vs. the data interface:

* Works across processes and machines.
* Allows data storage and persistence.
* Being a streamable format, the IPC format has room for composing more features
  (such as integrity checks, compression...).
* Does not require explicit C data access.


Data type description -- format strings
=======================================

A data type is described using a format string.  The format string only
encodes information about the top-level type; for nested type, child types
are described separately.  Also, metadata is encoded in a separate string.

The format strings are designed to be easily parsable, even from a language
such as C.  The most common primitive formats have one-character format
strings:

+-----------------+--------------------------+------------+
| Format string   | Arrow data type          | Notes      |
+=================+==========================+============+
| ``n``           | null                     |            |
+-----------------+--------------------------+------------+
| ``b``           | boolean                  |            |
+-----------------+--------------------------+------------+
| ``c``           | int8                     |            |
+-----------------+--------------------------+------------+
| ``C``           | uint8                    |            |
+-----------------+--------------------------+------------+
| ``s``           | int16                    |            |
+-----------------+--------------------------+------------+
| ``S``           | uint16                   |            |
+-----------------+--------------------------+------------+
| ``i``           | int32                    |            |
+-----------------+--------------------------+------------+
| ``I``           | uint32                   |            |
+-----------------+--------------------------+------------+
| ``l``           | int64                    |            |
+-----------------+--------------------------+------------+
| ``L``           | uint64                   |            |
+-----------------+--------------------------+------------+
| ``e``           | float16                  |            |
+-----------------+--------------------------+------------+
| ``f``           | float32                  |            |
+-----------------+--------------------------+------------+
| ``g``           | float64                  |            |
+-----------------+--------------------------+------------+

+-----------------+---------------------------------------------------+------------+
| Format string   | Arrow data type                                   | Notes      |
+=================+===================================================+============+
| ``z``           | binary                                            |            |
+-----------------+---------------------------------------------------+------------+
| ``Z``           | large binary                                      |            |
+-----------------+---------------------------------------------------+------------+
| ``u``           | utf-8 string                                      |            |
+-----------------+---------------------------------------------------+------------+
| ``U``           | large utf-8 string                                |            |
+-----------------+---------------------------------------------------+------------+
| ``d:19,10``     | decimal128 [precision 19, scale 10]               |            |
+-----------------+---------------------------------------------------+------------+
| ``d:19,10,NNN`` | decimal bitwidth = NNN [precision 19, scale 10]   |            |
+-----------------+---------------------------------------------------+------------+
| ``w:42``        | fixed-width binary [42 bytes]                     |            |
+-----------------+---------------------------------------------------+------------+

Temporal types have multi-character format strings starting with ``t``:

+-----------------+---------------------------------------------------+------------+
| Format string   | Arrow data type                                   | Notes      |
+=================+===================================================+============+
| ``tdD``         | date32 [days]                                     |            |
+-----------------+---------------------------------------------------+------------+
| ``tdm``         | date64 [milliseconds]                             |            |
+-----------------+---------------------------------------------------+------------+
| ``tts``         | time32 [seconds]                                  |            |
+-----------------+---------------------------------------------------+------------+
| ``ttm``         | time32 [milliseconds]                             |            |
+-----------------+---------------------------------------------------+------------+
| ``ttu``         | time64 [microseconds]                             |            |
+-----------------+---------------------------------------------------+------------+
| ``ttn``         | time64 [nanoseconds]                              |            |
+-----------------+---------------------------------------------------+------------+
| ``tss:...``     | timestamp [seconds] with timezone "..."           | \(1)       |
+-----------------+---------------------------------------------------+------------+
| ``tsm:...``     | timestamp [milliseconds] with timezone "..."      | \(1)       |
+-----------------+---------------------------------------------------+------------+
| ``tsu:...``     | timestamp [microseconds] with timezone "..."      | \(1)       |
+-----------------+---------------------------------------------------+------------+
| ``tsn:...``     | timestamp [nanoseconds] with timezone "..."       | \(1)       |
+-----------------+---------------------------------------------------+------------+
| ``tDs``         | duration [seconds]                                |            |
+-----------------+---------------------------------------------------+------------+
| ``tDm``         | duration [milliseconds]                           |            |
+-----------------+---------------------------------------------------+------------+
| ``tDu``         | duration [microseconds]                           |            |
+-----------------+---------------------------------------------------+------------+
| ``tDn``         | duration [nanoseconds]                            |            |
+-----------------+---------------------------------------------------+------------+
| ``tiM``         | interval [months]                                 |            |
+-----------------+---------------------------------------------------+------------+
| ``tiD``         | interval [days, time]                             |            |
+-----------------+---------------------------------------------------+------------+
| ``tin``         | interval [month, day, nanoseconds]                |            |
+-----------------+---------------------------------------------------+------------+


Dictionary-encoded types do not have a specific format string.  Instead, the
format string of the base array represents the dictionary index type, and the
value type can be read from the dependent dictionary array (see below
"Dictionary-encoded arrays").

Nested types have multiple-character format strings starting with ``+``.  The
names and types of child fields are read from the child arrays.

+------------------------+---------------------------------------------------+------------+
| Format string          | Arrow data type                                   | Notes      |
+========================+===================================================+============+
| ``+l``                 | list                                              |            |
+------------------------+---------------------------------------------------+------------+
| ``+L``                 | large list                                        |            |
+------------------------+---------------------------------------------------+------------+
| ``+w:123``             | fixed-sized list [123 items]                      |            |
+------------------------+---------------------------------------------------+------------+
| ``+s``                 | struct                                            |            |
+------------------------+---------------------------------------------------+------------+
| ``+m``                 | map                                               | \(2)       |
+------------------------+---------------------------------------------------+------------+
| ``+ud:I,J,...``        | dense union with type ids I,J...                  |            |
+------------------------+---------------------------------------------------+------------+
| ``+us:I,J,...``        | sparse union with type ids I,J...                 |            |
+------------------------+---------------------------------------------------+------------+

Notes:

(1)
   The timezone string is appended as-is after the colon character ``:``, without
   any quotes.  If the timezone is empty, the colon ``:`` must still be included.

(2)
   As specified in the Arrow columnar format, the map type has a single child type
   named ``entries``, itself a 2-child struct type of ``(key, value)``.

Examples
--------

* A dictionary-encoded ``decimal128(precision = 12, scale = 5)`` array
  with ``int16`` indices has format string ``s``, and its dependent dictionary
  array has format string ``d:12,5``.
* A ``list<uint64>`` array has format string ``+l``, and its single child
  has format string ``L``.
* A ``struct<ints: int32, floats: float32>`` has format string ``+s``; its two
  children have names ``ints`` and ``floats``, and format strings ``i`` and
  ``f`` respectively.
* A ``map<string, float64>`` array has format string ``+m``; its single child
  has name ``entries`` and format string ``+s``; its two grandchildren have names
  ``key`` and ``value``, and format strings ``u`` and ``g`` respectively.
* A ``sparse_union<ints: int32, floats: float32>`` with type ids ``4, 5``
  has format string ``+us:4,5``; its two children have names ``ints`` and
  ``floats``, and format strings ``i`` and ``f`` respectively.


Structure definitions
=====================

The following free-standing definitions are enough to support the Arrow
C data interface in your project.  Like the rest of the Arrow project, they
are available under the Apache License 2.0.

.. code-block:: c

   #ifndef ARROW_C_DATA_INTERFACE
   #define ARROW_C_DATA_INTERFACE

   #define ARROW_FLAG_DICTIONARY_ORDERED 1
   #define ARROW_FLAG_NULLABLE 2
   #define ARROW_FLAG_MAP_KEYS_SORTED 4

   struct ArrowSchema {
     // Array type description
     const char* format;
     const char* name;
     const char* metadata;
     int64_t flags;
     int64_t n_children;
     struct ArrowSchema** children;
     struct ArrowSchema* dictionary;

     // Release callback
     void (*release)(struct ArrowSchema*);
     // Opaque producer-specific data
     void* private_data;
   };

   struct ArrowArray {
     // Array data description
     int64_t length;
     int64_t null_count;
     int64_t offset;
     int64_t n_buffers;
     int64_t n_children;
     const void** buffers;
     struct ArrowArray** children;
     struct ArrowArray* dictionary;

     // Release callback
     void (*release)(struct ArrowArray*);
     // Opaque producer-specific data
     void* private_data;
   };

   #endif  // ARROW_C_DATA_INTERFACE

.. note::
   The canonical guard ``ARROW_C_DATA_INTERFACE`` is meant to avoid
   duplicate definitions if two projects copy the C data interface
   definitions in their own headers, and a third-party project
   includes from these two projects.  It is therefore important that
   this guard is kept exactly as-is when these definitions are copied.

The ArrowSchema structure
-------------------------

The ``ArrowSchema`` structure describes the type and metadata of an exported
array or record batch.  It has the following fields:

.. c:member:: const char* ArrowSchema.format

   Mandatory.  A null-terminated, UTF8-encoded string describing
   the data type.  If the data type is nested, child types are not
   encoded here but in the :c:member:`ArrowSchema.children` structures.

   Consumers MAY decide not to support all data types, but they
   should document this limitation.

.. c:member:: const char* ArrowSchema.name

   Optional.  A null-terminated, UTF8-encoded string of the field
   or array name.  This is mainly used to reconstruct child fields
   of nested types.

   Producers MAY decide not to provide this information, and consumers
   MAY decide to ignore it.  If omitted, MAY be NULL or an empty string.

.. c:member:: const char* ArrowSchema.metadata

   Optional.  A binary string describing the type's metadata.
   If the data type is nested, child types are not encoded here but
   in the :c:member:`ArrowSchema.children` structures.

   This string is not null-terminated but follows a specific format::

      int32: number of key/value pairs (noted N below)
      int32: byte length of key 0
      key 0 (not null-terminated)
      int32: byte length of value 0
      value 0 (not null-terminated)
      ...
      int32: byte length of key N - 1
      key N - 1 (not null-terminated)
      int32: byte length of value N - 1
      value N - 1 (not null-terminated)

   Integers are stored in native endianness.  For example, the metadata
   ``[('key1', 'value1')]`` is encoded on a little-endian machine as::

      \x01\x00\x00\x00\x04\x00\x00\x00key1\x06\x00\x00\x00value1

   On a big-endian machine, the same example would be encoded as::

      \x00\x00\x00\x01\x00\x00\x00\x04key1\x00\x00\x00\x06value1

   If omitted, this field MUST be NULL (not an empty string).

   Consumers MAY choose to ignore this information.

.. c:member:: int64_t ArrowSchema.flags

   Optional.  A bitfield of flags enriching the type description.
   Its value is computed by OR'ing together the flag values.
   The following flags are available:

   * ``ARROW_FLAG_NULLABLE``: whether this field is semantically nullable
     (regardless of whether it actually has null values).
   * ``ARROW_FLAG_DICTIONARY_ORDERED``: for dictionary-encoded types,
     whether the ordering of dictionary indices is semantically meaningful.
   * ``ARROW_FLAG_MAP_KEYS_SORTED``: for map types, whether the keys within
     each map value are sorted.

   If omitted, MUST be 0.

   Consumers MAY choose to ignore some or all of the flags.  Even then,
   they SHOULD keep this value around so as to propagate its information
   to their own consumers.

.. c:member:: int64_t ArrowSchema.n_children

   Mandatory.  The number of children this type has.

.. c:member:: ArrowSchema** ArrowSchema.children

   Optional.  A C array of pointers to each child type of this type.
   There must be :c:member:`ArrowSchema.n_children` pointers.

   MAY be NULL only if :c:member:`ArrowSchema.n_children` is 0.

.. c:member:: ArrowSchema* ArrowSchema.dictionary

   Optional.  A pointer to the type of dictionary values.

   MUST be present if the ArrowSchema represents a dictionary-encoded type.
   MUST be NULL otherwise.

.. c:member:: void (*ArrowSchema.release)(struct ArrowSchema*)

   Mandatory.  A pointer to a producer-provided release callback.

   See below for memory management and release callback semantics.

.. c:member:: void* ArrowSchema.private_data

   Optional.  An opaque pointer to producer-provided private data.

   Consumers MUST not process this member.  Lifetime of this member
   is handled by the producer, and especially by the release callback.


The ArrowArray structure
------------------------

The ``ArrowArray`` describes the data of an exported array or record batch.
For the ``ArrowArray`` structure to be interpreted type, the array type
or record batch schema must already be known.  This is either done by
convention -- for example a producer API that always produces the same data
type -- or by passing a ``ArrowSchema`` on the side.

It has the following fields:

.. c:member:: int64_t ArrowArray.length

   Mandatory.  The logical length of the array (i.e. its number of items).

.. c:member:: int64_t ArrowArray.null_count

   Mandatory.  The number of null items in the array.  MAY be -1 if not
   yet computed.

.. c:member:: int64_t ArrowArray.offset

   Mandatory.  The logical offset inside the array (i.e. the number of items
   from the physical start of the buffers).  MUST be 0 or positive.

   Producers MAY specify that they will only produce 0-offset arrays to
   ease implementation of consumer code.
   Consumers MAY decide not to support non-0-offset arrays, but they
   should document this limitation.

.. c:member:: int64_t ArrowArray.n_buffers

   Mandatory.  The number of physical buffers backing this array.  The
   number of buffers is a function of the data type, as described in the
   :ref:`Columnar format specification <format_columnar>`.

   Buffers of children arrays are not included.

.. c:member:: const void** ArrowArray.buffers

   Mandatory.  A C array of pointers to the start of each physical buffer
   backing this array.  Each `void*` pointer is the physical start of
   a contiguous buffer.  There must be :c:member:`ArrowArray.n_buffers` pointers.

   The producer MUST ensure that each contiguous buffer is large enough to
   represent `length + offset` values encoded according to the
   :ref:`Columnar format specification <format_columnar>`.

   It is recommended, but not required, that the memory addresses of the
   buffers be aligned at least according to the type of primitive data that
   they contain. Consumers MAY decide not to support unaligned memory.

   The buffer pointers MAY be null only in two situations:

   1. for the null bitmap buffer, if :c:member:`ArrowArray.null_count` is 0;
   2. for any buffer, if the size in bytes of the corresponding buffer would be 0.

   Buffers of children arrays are not included.

.. c:member:: int64_t ArrowArray.n_children

   Mandatory.  The number of children this array has.  The number of children
   is a function of the data type, as described in the
   :ref:`Columnar format specification <format_columnar>`.

.. c:member:: ArrowArray** ArrowArray.children

   Optional.  A C array of pointers to each child array of this array.
   There must be :c:member:`ArrowArray.n_children` pointers.

   MAY be NULL only if :c:member:`ArrowArray.n_children` is 0.

.. c:member:: ArrowArray* ArrowArray.dictionary

   Optional.  A pointer to the underlying array of dictionary values.

   MUST be present if the ArrowArray represents a dictionary-encoded array.
   MUST be NULL otherwise.

.. c:member:: void (*ArrowArray.release)(struct ArrowArray*)

   Mandatory.  A pointer to a producer-provided release callback.

   See below for memory management and release callback semantics.

.. c:member:: void* ArrowArray.private_data

   Optional.  An opaque pointer to producer-provided private data.

   Consumers MUST not process this member.  Lifetime of this member
   is handled by the producer, and especially by the release callback.


Dictionary-encoded arrays
-------------------------

For dictionary-encoded arrays, the :c:member:`ArrowSchema.format` string
encodes the *index* type.  The dictionary *value* type can be read
from the :c:member:`ArrowSchema.dictionary` structure.

The same holds for :c:member:`ArrowArray` structure: while the parent
structure points to the index data, the :c:member:`ArrowArray.dictionary`
points to the dictionary values array.

Extension arrays
----------------

For extension arrays, the :c:member:`ArrowSchema.format` string encodes the
*storage* type.  Information about the extension type is encoded in the
:c:member:`ArrowSchema.metadata` string, similarly to the
:ref:`IPC format <format_metadata_extension_types>`.  Specifically, the
metadata key ``ARROW:extension:name``  encodes the extension type name,
and the metadata key ``ARROW:extension:metadata`` encodes the
implementation-specific serialization of the extension type (for
parameterized extension types).

The ``ArrowArray`` structure exported from an extension array simply points
to the storage data of the extension array.


Semantics
=========

Memory management
-----------------

The ``ArrowSchema`` and ``ArrowArray`` structures follow the same conventions
for memory management.  The term *"base structure"* below refers to the
``ArrowSchema`` or ``ArrowArray`` that is passed between producer and consumer
-- not any child structure thereof.

Member allocation
'''''''''''''''''

It is intended for the base structure to be stack- or heap-allocated by the
consumer.  In this case, the producer API should take a pointer to the
consumer-allocated structure.

However, any data pointed to by the struct MUST be allocated and maintained
by the producer.  This includes the format and metadata strings, the arrays
of buffer and children pointers, etc.

Therefore, the consumer MUST not try to interfere with the producer's
handling of these members' lifetime.  The only way the consumer influences
data lifetime is by calling the base structure's ``release`` callback.

.. _c-data-interface-released:

Released structure
''''''''''''''''''

A released structure is indicated by setting its ``release`` callback to NULL.
Before reading and interpreting a structure's data, consumers SHOULD check
for a NULL release callback and treat it accordingly (probably by erroring
out).

Release callback semantics -- for consumers
'''''''''''''''''''''''''''''''''''''''''''

Consumers MUST call a base structure's release callback when they won't be using
it anymore, but they MUST not call any of its children's release callbacks
(including the optional dictionary).  The producer is responsible for releasing
the children.

In any case, a consumer MUST not try to access the base structure anymore
after calling its release callback -- including any associated data such
as its children.

Release callback semantics -- for producers
'''''''''''''''''''''''''''''''''''''''''''

If producers need additional information for lifetime handling (for
example, a C++ producer may want to use ``shared_ptr`` for array and
buffer lifetime), they MUST use the ``private_data`` member to locate the
required bookkeeping information.

The release callback MUST not assume that the structure will be located
at the same memory location as when it was originally produced.  The consumer
is free to move the structure around (see "Moving an array").

The release callback MUST walk all children structures (including the optional
dictionary) and call their own release callbacks.

The release callback MUST free any data area directly owned by the structure
(such as the buffers and children members).

The release callback MUST mark the structure as released, by setting
its ``release`` member to NULL.

Below is a good starting point for implementing a release callback, where the
TODO area must be filled with producer-specific deallocation code:

.. code-block:: c

   static void ReleaseExportedArray(struct ArrowArray* array) {
     // This should not be called on already released array
     assert(array->release != NULL);

     // Release children
     for (int64_t i = 0; i < array->n_children; ++i) {
       struct ArrowArray* child = array->children[i];
       if (child->release != NULL) {
         child->release(child);
         assert(child->release == NULL);
       }
     }

     // Release dictionary
     struct ArrowArray* dict = array->dictionary;
     if (dict != NULL && dict->release != NULL) {
       dict->release(dict);
       assert(dict->release == NULL);
     }

     // TODO here: release and/or deallocate all data directly owned by
     // the ArrowArray struct, such as the private_data.

     // Mark array released
     array->release = NULL;
   }


Moving an array
'''''''''''''''

The consumer can *move* the ``ArrowArray`` structure by bitwise copying or
shallow member-wise copying.  Then it MUST mark the source structure released
(see "released structure" above for how to do it) but *without* calling the
release callback.  This ensures that only one live copy of the struct is
active at any given time and that lifetime is correctly communicated to
the producer.

As usual, the release callback will be called on the destination structure
when it is not needed anymore.

Moving child arrays
~~~~~~~~~~~~~~~~~~~

It is also possible to move one or several child arrays, but the parent
``ArrowArray`` structure MUST be released immediately afterwards, as it
won't point to valid child arrays anymore.

The main use case for this is to keep alive only a subset of child arrays
(for example if you are only interested in certain columns of the data),
while releasing the others.

.. note::

   For moving to work correctly, the ``ArrowArray`` structure has to be
   trivially relocatable.  Therefore, pointer members inside the ``ArrowArray``
   structure (including ``private_data``) MUST not point inside the structure
   itself.  Also, external pointers to the structure MUST not be separately
   stored by the producer.  Instead, the producer MUST use the ``private_data``
   member so as to remember any necessary bookkeeping information.

Record batches
--------------

A record batch can be trivially considered as an equivalent struct array. In
this case the metadata of the top-level ``ArrowSchema`` can be used for the
schema-level metadata of the record batch.

Mutability
----------

Both the producer and the consumer SHOULD consider the exported data
(that is, the data reachable through the ``buffers`` member of ``ArrowArray``)
to be immutable, as either party could otherwise see inconsistent data while
the other is mutating it.


Example use case
================

A C++ database engine wants to provide the option to deliver results in Arrow
format, but without imposing themselves a dependency on the Arrow software
libraries.  With the Arrow C data interface, the engine can let the caller pass
a pointer to a ``ArrowArray`` structure, and fill it with the next chunk of
results.

It can do so without including the Arrow C++ headers or linking with the
Arrow DLLs.  Furthermore, the database engine's C API can benefit other
runtimes and libraries that know about the Arrow C data interface,
through e.g. a C FFI layer.

C producer examples
===================

Exporting a simple ``int32`` array
----------------------------------

Export a non-nullable ``int32`` type with empty metadata.  In this case,
all ``ArrowSchema`` members point to statically-allocated data, so the
release callback is trivial.

.. code-block:: c

   static void release_int32_type(struct ArrowSchema* schema) {
      // Mark released
      schema->release = NULL;
   }

   void export_int32_type(struct ArrowSchema* schema) {
      *schema = (struct ArrowSchema) {
         // Type description
         .format = "i",
         .name = "",
         .metadata = NULL,
         .flags = 0,
         .n_children = 0,
         .children = NULL,
         .dictionary = NULL,
         // Bookkeeping
         .release = &release_int32_type
      };
   }

Export a C-malloc()ed array of the same type as a Arrow array, transferring
ownership to the consumer through the release callback:

.. code-block:: c

   static void release_int32_array(struct ArrowArray* array) {
      assert(array->n_buffers == 2);
      // Free the buffers and the buffers array
      free((void *) array->buffers[1]);
      free(array->buffers);
      // Mark released
      array->release = NULL;
   }

   void export_int32_array(const int32_t* data, int64_t nitems,
                           struct ArrowArray* array) {
      // Initialize primitive fields
      *array = (struct ArrowArray) {
         // Data description
         .length = nitems,
         .offset = 0,
         .null_count = 0,
         .n_buffers = 2,
         .n_children = 0,
         .children = NULL,
         .dictionary = NULL,
         // Bookkeeping
         .release = &release_int32_array
      };
      // Allocate list of buffers
      array->buffers = (const void**) malloc(sizeof(void*) * array->n_buffers);
      assert(array->buffers != NULL);
      array->buffers[0] = NULL;  // no nulls, null bitmap can be omitted
      array->buffers[1] = data;
   }

Exporting a ``struct<float32, utf8>`` array
-------------------------------------------

Export the array type as a ``ArrowSchema`` with C-malloc()ed children:

.. code-block:: c

   static void release_malloced_type(struct ArrowSchema* schema) {
      int i;
      for (i = 0; i < schema->n_children; ++i) {
         struct ArrowSchema* child = schema->children[i];
         if (child->release != NULL) {
            child->release(child);
         }
      }
      free(schema->children);
      // Mark released
      schema->release = NULL;
   }

   void export_float32_utf8_type(struct ArrowSchema* schema) {
      struct ArrowSchema* child;

      //
      // Initialize parent type
      //
      *schema = (struct ArrowSchema) {
         // Type description
         .format = "+s",
         .name = "",
         .metadata = NULL,
         .flags = 0,
         .n_children = 2,
         .dictionary = NULL,
         // Bookkeeping
         .release = &release_malloced_type
      };
      // Allocate list of children types
      schema->children = malloc(sizeof(struct ArrowSchema*) * schema->n_children);

      //
      // Initialize child type #0
      //
      child = schema->children[0] = malloc(sizeof(struct ArrowSchema));
      *child = (struct ArrowSchema) {
         // Type description
         .format = "f",
         .name = "floats",
         .metadata = NULL,
         .flags = ARROW_FLAG_NULLABLE,
         .n_children = 0,
         .dictionary = NULL,
         .children = NULL,
         // Bookkeeping
         .release = &release_malloced_type
      };

      //
      // Initialize child type #1
      //
      child = schema->children[1] = malloc(sizeof(struct ArrowSchema));
      *child = (struct ArrowSchema) {
         // Type description
         .format = "u",
         .name = "strings",
         .metadata = NULL,
         .flags = ARROW_FLAG_NULLABLE,
         .n_children = 0,
         .dictionary = NULL,
         .children = NULL,
         // Bookkeeping
         .release = &release_malloced_type
      };
   }

Export C-malloc()ed arrays in Arrow-compatible layout as an Arrow struct array,
transferring ownership to the consumer:

.. code-block:: c

   static void release_malloced_array(struct ArrowArray* array) {
      int i;
      // Free children
      for (i = 0; i < array->n_children; ++i) {
         struct ArrowArray* child = array->children[i];
         if (child->release != NULL) {
            child->release(child);
         }
      }
      free(array->children);
      // Free buffers
      for (i = 0; i < array->n_buffers; ++i) {
         free((void *) array->buffers[i]);
      }
      free(array->buffers);
      // Mark released
      array->release = NULL;
   }

   void export_float32_utf8_array(
         int64_t nitems,
         const uint8_t* float32_nulls, const float* float32_data,
         const uint8_t* utf8_nulls, const int32_t* utf8_offsets, const uint8_t* utf8_data,
         struct ArrowArray* array) {
      struct ArrowArray* child;

      //
      // Initialize parent array
      //
      *array = (struct ArrowArray) {
         // Data description
         .length = nitems,
         .offset = 0,
         .null_count = 0,
         .n_buffers = 1,
         .n_children = 2,
         .dictionary = NULL,
         // Bookkeeping
         .release = &release_malloced_array
      };
      // Allocate list of parent buffers
      array->buffers = malloc(sizeof(void*) * array->n_buffers);
      array->buffers[0] = NULL;  // no nulls, null bitmap can be omitted
      // Allocate list of children arrays
      array->children = malloc(sizeof(struct ArrowArray*) * array->n_children);

      //
      // Initialize child array #0
      //
      child = array->children[0] = malloc(sizeof(struct ArrowArray));
      *child = (struct ArrowArray) {
         // Data description
         .length = nitems,
         .offset = 0,
         .null_count = -1,
         .n_buffers = 2,
         .n_children = 0,
         .dictionary = NULL,
         .children = NULL,
         // Bookkeeping
         .release = &release_malloced_array
      };
      child->buffers = malloc(sizeof(void*) * child->n_buffers);
      child->buffers[0] = float32_nulls;
      child->buffers[1] = float32_data;

      //
      // Initialize child array #1
      //
      child = array->children[1] = malloc(sizeof(struct ArrowArray));
      *child = (struct ArrowArray) {
         // Data description
         .length = nitems,
         .offset = 0,
         .null_count = -1,
         .n_buffers = 3,
         .n_children = 0,
         .dictionary = NULL,
         .children = NULL,
         // Bookkeeping
         .release = &release_malloced_array
      };
      child->buffers = malloc(sizeof(void*) * child->n_buffers);
      child->buffers[0] = utf8_nulls;
      child->buffers[1] = utf8_offsets;
      child->buffers[2] = utf8_data;
   }


Why two distinct structures?
============================

In many cases, the same type or schema description applies to multiple,
possibly short, batches of data.  To avoid paying the cost of exporting
and importing the type description for each batch, the ``ArrowSchema``
can be passed once, separately, at the beginning of the conversation between
producer and consumer.

In other cases yet, the data type is fixed by the producer API, and may not
need to be communicated at all.

However, if a producer is focused on one-shot exchange of data, it can
communicate the ``ArrowSchema`` and ``ArrowArray`` structures in the same
API call.

Updating this specification
===========================

Once this specification is supported in an official Arrow release, the C
ABI is frozen.  This means the ``ArrowSchema`` and ``ArrowArray`` structure
definitions should not change in any way -- including adding new members.

Backwards-compatible changes are allowed, for example new
:c:member:`ArrowSchema.flags` values or expanded possibilities for
the :c:member:`ArrowSchema.format` string.

Any incompatible changes should be part of a new specification, for example
"Arrow C data interface v2".

Inspiration
===========

The Arrow C data interface is inspired by the `Python buffer protocol`_,
which has proven immensely successful in allowing various Python libraries
exchange numerical data with no knowledge of each other and near-zero
adaptation cost.


.. _Python buffer protocol: https://www.python.org/dev/peps/pep-3118/
