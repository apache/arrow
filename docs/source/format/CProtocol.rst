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

=========================
The Arrow C data protocol
=========================

Rationale
=========

Apache Arrow aims to be a universal in-memory format for the representation
of tabular ("columnar") data, but some projects may face a difficult
choice between either depending on a fast-evolving dependency such as the
Arrow C++ library, or having to implement adapters for data interchange
(for example by reimplementing the Arrow IPC format, which is non-trivial
and does not allow cross-runtime zero-copy).

The Arrow C data protocol defines a very small, stable set of C definitions
that can be easily *copied* in any project's source code and used for columnar
data interchange in the Arrow format.  For non-C/C++ languages and runtimes,
it should be almost as easy to translate the C definitions into the
corresponding C FFI declarations.

Applications and libraries can therefore choose between tight integration
with the Arrow *software project* (benefitting from the growing array of
facilities exposed by e.g. the C++ or Java implementations of Apache Arrow,
but with the cost of a dependency) or minimal integration with the Arrow
*format*.

Goals
-----

* Expose an ABI-stable interface.
* Easy for third-party projects to implement support for (including partial
  support where sufficient), with little initial investment.
* Zero-copy sharing of Arrow data between independent runtimes
  and components running in the same process.
* Match the Arrow array concepts closely, to avoid the development of
  yet another marshalling layer.
* Avoid the need for one-to-one adaptation layers such as the limited
  JPype-based bridge between Java and Python.
* Allow integration without an explicit dependency (either at compile-time
  or runtime) on the Arrow software project.

Ideally, the Arrow C data protocol can become a low-level *lingua franca*
for sharing columnar data at runtime and establish Arrow as the universal
building block in the columnar processing ecosystem.

Non-goals
---------

* Expose a C API mimicking operations available in higher-level runtimes
  (such as C++, Java...).
* Data sharing between distinct processes.

Comparison with the Arrow IPC format
------------------------------------

Pros of the C data protocol vs. the IPC format:

* No dependency on Flatbuffers.
* No buffer reassembly (data is already exposed in logical Arrow format).
* Zero-copy by design.
* Easy to reimplement from scratch.

Pros of the IPC format vs. the data protocol:

* Works accross processes and machines.
* Allows data storage and persistency.
* Being a streamable format, has room for composing more features (such as
  integrity checks, compression...).

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

+-----------------+-----------------------------------+------------+
| Format string   | Arrow data type                   | Notes      |
+=================+===================================+============+
| ``z``           | binary                            |            |
+-----------------+-----------------------------------+------------+
| ``Z``           | large binary                      |            |
+-----------------+-----------------------------------+------------+
| ``u``           | utf-8 string                      |            |
+-----------------+-----------------------------------+------------+
| ``U``           | large utf-8 string                |            |
+-----------------+-----------------------------------+------------+
| ``d``           | decimal128                        |            |
+-----------------+-----------------------------------+------------+
| ``w:42``        | fixed-width binary [42 bytes]     |            |
+-----------------+-----------------------------------+------------+

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
   any quotes.

(2)
   As specified in the Arrow columnar format, the map type has a single child type
   named ``entry``, itself a 2-child struct type of ``(key, value)``.

Examples
--------

* A dictionary-encoded ``decimal128`` array with ``int16`` indices has format
  string ``s``, and its dependent dictionary array has format string ``d``.
* A ``list<uint64>`` array has format string ``+l``, and its single child
  has format string ``L``.
* A ``struct<ints: int32, floats: float32>`` has format string ``+s``; its two
  children have names ``ints`` and ``floats``, and format strings ``i`` and
  ``f`` respectively.
* A ``map<string, float64>`` array has format string ``+m``; its single child
  has name ``entry`` and format string ``+s``; its two grandchildren have names
  ``key`` and ``value``, and format strings ``u`` and ``g`` respectively.
* A ``sparse_union<ints: int32, floats: float32>`` with type ids ``4, 5``
  has format string ``+us:4,5``; its two children have names ``ints`` and
  ``floats``, and format strings ``i`` and ``f`` respectively.


Structure definitions
=====================

The following free-standing definitions are enough to support the Arrow
C data protocol in your project.  Like the rest of the Arrow project, they
are available under the Apache License 2.0.

.. code-block:: c

   #define ARROW_FLAG_ORDERED 1
   #define ARROW_FLAG_NULLABLE 2

   struct ArrowArray {
     // Type description
     const char* format;
     const char* name;
     const char* metadata;
     int64_t flags;

     // Data description
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

The ArrowArray structure
------------------------

.. c:member:: const char* ArrowArray.format

   Mandatory.  A null-terminated, UTF8-encoded string describing
   the data type.  If the data type is nested, child types are not
   encoded here but in the :c:member:`ArrowArray.children` arrays.

   Consumers MAY decide not to support all data types, but they
   should document this limitation.

.. c:member:: const char* ArrowArray.name

   Optional.  A null-terminated, UTF8-encoded string of the field
   or array name.  This is mainly used to reconstruct child fields
   of nested arrays.

   Producers MAY decide not to provide this information, and consumers
   MAY decide to ignore it.  If omitted, MAY be NULL or an empty string.

.. c:member:: const char* ArrowArray.metadata

   Optional.  A null-terminated, UTF8-encoded string describing
   the type's metadata.  If the data type is nested, child types are not
   encoded here but in the :c:member:`ArrowArray.children` arrays.

   MUST be a JSON-compatible mapping of UTF8 strings to UTF8 strings.
   Whitespace MUST only use the 0x20 character. Example::

      {"key1": "base64-encoded value1", "key2": "base64-encode value2"}

   If omitted, MUST be NULL (not an empty string).

   Consumers MAY choose to ignore metadata.  Even then, they SHOULD keep
   the metadata string around so as to propagate its information to their
   own consumers.

.. c:member:: int64_t ArrowArray.flags

   Optional.  A bitfield of flags enriching the type or array description.
   Its value is computed by OR'ing together the flag values.
   The following flags are available:

   * ``ARROW_FLAG_NULLABLE``: whether this field is semantically nullable
     (regardless of whether it actually has null values).
   * ``ARROW_FLAG_ORDERED``: for dictionary-encoded arrays, whether the
     ordering of dictionary indices is semantically meaningful.

   If omitted, MUST be 0.

   Consumers MAY choose to ignore some or all of the flags.  Even then,
   they SHOULD keep this value around so as to propagate its information
   to their own consumers.

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

   The pointer to the null bitmap buffer, if the data type specifies one,
   MAY be NULL only if :c:member:`ArrowArray.null_count` is 0.

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

   Recommended.  A pointer to a producer-provided release callback.

   The release callback MAY be null which means absent.  In this case,
   the consumer will not be able to tell the producer when it is finished
   with the data.  Still, this may be acceptable for synchronous consumers
   called by the producer, or if application-specific lifetime rules
   are defined.

   If not NULL, the consumer MUST call this callback to signal that it
   doesn't need the array, its data or any of its child data anymore.
   It must pass the *current* address of the ArrowArray struct as the callback
   parameter.

.. c:member:: void* ArrowArray.private_data

   Optional.  An opaque pointer to producer-provided private data.

   Consumers MUST not process this member.  Lifetime of this member
   is handled by the producer, and especially by the release callback.


Dictionary-encoded arrays
-------------------------

For dictionary-encoded arrays, the :c:member:`ArrowArray.format` string
encodes the *index* type.  The dictionary *value* type can be read
from the :c:member:`ArrowArray.dictionary` struct.

Extension arrays
----------------

For extension arrays, the :c:member:`ArrowArray.format` string encodes the
*storage* type.  Information about the extension type is encoded in the
:c:member:`ArrowArray.metadata` string, similarly to the
:ref:`IPC format <format_metadata_extension_types>`.  Specifically, the
metadata key ``ARROW:extension:name``  encodes the extension type name,
and the metadata key ``ARROW:extension:metadata`` encodes the
implementation-specific serialization of the extension type (for
parameterized extension types).  The base64 encoding of metadata values
ensures that any possible serialization is representable.

Memory management
-----------------

Member allocation
'''''''''''''''''

While the base ArrowArray struct MAY be stack- or heap-allocated by
the consumer (and then a pointer passed around to the producer), any
data pointed to by the struct MUST be allocated by the producer.  This
includes the format and metadata strings, the arrays of buffer and children
pointers, etc.

Therefore, the consumer MUST not try to interfere with the producer's
handling of these members' lifetime.  The only way the consumer influences
data lifetime is by calling the base ArrowArray's release callback.

Released array
''''''''''''''

A released array is indicated by setting :c:member:`ArrowArray.format` to
NULL.  Consumers SHOULD check for a NULL format string and treat it
accordingly (probably by erroring out).  Additionally, a released array
MAY set :c:member:`ArrowArray.release` to NULL.

Release callback semantics -- for consumers
'''''''''''''''''''''''''''''''''''''''''''

Consumers MUST call an array's release callback when they won't be using
it anymore, but they MUST not call any of its child arrays' release callbacks
(including the optional dictionary).  The producer is responsible for releasing
the children.

Consumers MUST check that the release callback is non-NULL before calling it.
The release callback being NULL is not an error, it should just be ignored.

In any case, a consumer MUST not try to access the ArrowArray struct anymore
after calling its release callback -- including any associated data such
as its children.

Release callback semantics -- for producers
'''''''''''''''''''''''''''''''''''''''''''

If producers need additional information for lifetime handling (for
example, a C++ producer may want to use ``shared_ptr`` for array and
buffer lifetime), they MUST use the :c:member:`ArrowArray.private_data`
member to locate the required bookkeeping information.

The release callback MUST not assume that the struct will be located
at the same memory location as when it was originally produced.  The consumer
is free to move the struct around (see "Movability").

The release callback MUST walk all children arrays (including the optional
dictionary) and call their own release callbacks.

The release callback MUST free any data area directly owned by the struct
(such as the buffers and children arrays).

The release callback MUST mark the array as released, by setting
:c:member:`ArrowArray.format` to NULL.

Additionally, the release callback MUST be idempotent, which is commonly
achieved by setting itself to NULL.

Movability
''''''''''

The consumer can *move* the ``ArrowArray`` struct by bitwise copying (or
shallow member-wise copying).  Then it MUST mark the source struct released
(see "released array" above for how to do it) but **without** calling the
release callback.  This ensures that only one live copy of the struct is
active at any given time and that lifetime is correctly communicated to
the producer.

It is possible to move a child array, but the parent array MUST be released
immediately afterwards, as it won't point to a valid child array anymore.
This satisfies the use case of keeping only a subset of child arrays, while
releasing the others.

.. note::

   For bitwise copying to work correctly, the pointers inside the struct
   (including private_data) MUST not point inside the struct itself.
   Also, external pointers to the struct MUST not be stored by the producer.
   Instead, the producer MUST use the :c:member:`ArrowArray.private_data`
   member so as to remember any necessary bookkeeping information.

Example use case
================

A C++ database engine wants to provide the option to deliver results in Arrow
format, but without imposing themselves a dependency on the Arrow software
libraries.  With the Arrow C data protocol, the engine can let the caller pass
a pointer to a ``ArrowArray`` structure, and fill it with the next chunk of
results.

It can do so without including the Arrow C++ headers or linking with the
Arrow DLLs.  Furthermore, the database engine's C API can benefit other
runtimes and libraries that know about the Arrow C data protocol,
through e.g. a C FFI layer.

If the database wants to return a multi-column result set, it can easily
be represented in either of two ways:

* an array of ``ArrowArray`` structures, one per column;
* or a single ``ArrowArray`` structure representing a struct array, with one
  child array per column.

C producer examples
===================

Exporting a simple ``int32`` array
----------------------------------

Export a no-nulls C-malloc()ed ``int32`` array as a Arrow array, transferring
ownership to the consumer:

.. code-block:: c

   static void release_int32_array(struct ArrowArray* array) {
      assert(array->n_children == NULL);
      assert(array->n_buffers == 2);
      free(array->buffers[1]);
      free(array->buffers);
      array->format = NULL;
      array->release = NULL;
   }

   void export_int32_array(const int32_t* data, int64_t nitems,
                           struct ArrowArray* array) {
      // Initialize primitive fields
      *array = (struct ArrowArray) {
         // Type description
         .format = "l",
         .name = "",
         .metadata = NULL,
         .flags = 0,
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

Export C-malloc()ed arrays in Arrow-compatible layout as a Arrow struct array,
transferring ownership to the consumer:

.. code-block:: c

   static void release_owned_array(struct ArrowArray* array) {
      int i;
      for (i = 0; i < array->n_children; ++i) {
         struct ArrowArray* child = &array->children[i];
         if (child->release != NULL) {
            child->release(child);
         }
      }
      free(array->children);
      for (i = 0; i < array->n_buffers; ++i) {
         free(array->buffers[i]);
      }
      free(array->buffers);
      array->format = NULL;
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
         // Type description
         .format = "+s",
         .name = "",
         .metadata = NULL,
         .flags = 0,
         // Data description
         .length = nitems,
         .offset = 0,
         .null_count = 0,
         .n_buffers = 1,
         .n_children = 2,
         .dictionary = NULL,
         // Bookkeeping
         .release = &release_owned_array
      };
      // Allocate list of parent buffers
      array->buffers = (const void**) malloc(sizeof(void*) * array->n_buffers);
      array->buffers[0] = NULL;  // no nulls, null bitmap can be omitted
      // Allocate list of children arrays
      array->children = (const void**) malloc(sizeof(struct ArrowArray*) *
                                              array->n_children);

      //
      // Initialize child array #1
      //
      child = array->children[0] = malloc(sizeof(struct ArrowArray));
      *child = (struct ArrowArray) {
         // Type description
         .format = "f",
         .name = "floats",
         .metadata = NULL,
         .flags = ARROW_FLAG_NULLABLE,
         // Data description
         .length = nitems,
         .offset = 0,
         .null_count = -1,
         .n_buffers = 2,
         .n_children = 0,
         .dictionary = NULL,
         .children = NULL,
         // Bookkeeping
         .release = &release_owned_array
      };
      child->buffers = (const void**) malloc(sizeof(void*) * array->n_buffers);
      child->buffers[0] = float32_nulls;
      child->buffers[1] = float32_data;

      //
      // Initialize child array #2
      //
      child = array->children[1] = malloc(sizeof(struct ArrowArray));
      *child = (struct ArrowArray) {
         // Type description
         .format = "u",
         .name = "strings",
         .metadata = NULL,
         .flags = ARROW_FLAG_NULLABLE,
         // Data description
         .length = nitems,
         .offset = 0,
         .null_count = -1,
         .n_buffers = 3,
         .n_children = 0,
         .dictionary = NULL,
         .children = NULL,
         // Bookkeeping
         .release = &release_owned_array
      };
      child->buffers = (const void**) malloc(sizeof(void*) * array->n_buffers);
      child->buffers[0] = utf8_nulls;
      child->buffers[1] = utf8_offsets;
      child->buffers[2] = utf8_data;
   }


Updating this specification
===========================

Once this specification is supported in an official Arrow release, the C
ABI is frozen.  This means the C struct ``ArrowArray`` should not get any
more changes.  Backwards-compatible changes may still be added, for example
new :c:member:`ArrowArray.flags` values or expanded possibilities for
the :c:member:`ArrowArray.format` string.

Any incompatible changes should be part of a new specification, for example
"Arrow C data protocol v2".

Inspiration
===========

The Arrow C data protocol is inspired by the `Python buffer protocol`_,
which has proven immensely successful in allowing various Python libraries
exchange numerical data with no knowledge of each other and near-zero
adaptation cost.


.. _Python buffer protocol: https://www.python.org/dev/peps/pep-3118/
