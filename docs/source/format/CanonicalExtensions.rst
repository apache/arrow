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

.. _format_canonical_extensions:

*************************
Canonical Extension Types
*************************

============
Introduction
============

The Arrow columnar format allows defining
:ref:`extension types <format_metadata_extension_types>` so as to extend
standard Arrow data types with custom semantics.  Often these semantics
will be specific to a system or application.  However, it is beneficial
to share the definitions of well-known extension types so as to improve
interoperability between different systems integrating Arrow columnar data.

Standardization
===============

These rules must be followed for the standardization of canonical extension
types:

* Canonical extension types are described and maintained below in this document.

* Each canonical extension type requires a distinct discussion and vote
  on the `Arrow development mailing-list <https://arrow.apache.org/community/>`__.

* The specification text to be added *must* follow these requirements:

  1) It *must* define a well-defined extension name starting with "``arrow.``".

  2) Its parameters, if any, *must* be described in the proposal.

  3) Its serialization *must* be described in the proposal and should
     not require unduly implementation work or unusual software dependencies
     (for example, a trivial custom text format or a JSON-based format would be acceptable).

  4) Its expected semantics *should* be described as well and any
     potential ambiguities or pain points addressed or at least mentioned.

* The extension type *should* have one implementation submitted;
  preferably two if non-trivial (for example if parameterized).

Making Modifications
====================

Like standard Arrow data types, canonical extension types should be considered
stable once standardized.  Modifying a canonical extension type (for example
to expand the set of parameters) should be an exceptional event, follow the
same rules as laid out above, and provide backwards compatibility guarantees.


=============
Official List
=============

.. _fixed_shape_tensor_extension:

Fixed shape tensor
==================

* Extension name: ``arrow.fixed_shape_tensor``.

* The storage type of the extension: ``FixedSizeList`` where:

  * **value_type** is the data type of individual tensor elements.
  * **list_size** is the product of all the elements in tensor shape.

* Extension type parameters:

  * **value_type** = the Arrow data type of individual tensor elements.
  * **shape** = the physical shape of the contained tensors
    as an array.

  Optional parameters describing the logical layout:

  * **dim_names** = explicit names to tensor dimensions
    as an array. The length of it should be equal to the shape
    length and equal to the number of dimensions.

    ``dim_names`` can be used if the dimensions have well-known
    names and they map to the physical layout (row-major).

  * **permutation**  = indices of the desired ordering of the
    original dimensions, defined as an array.

    The indices contain a permutation of the values [0, 1, .., N-1] where
    N is the number of dimensions. The permutation indicates which
    dimension of the logical layout corresponds to which dimension of the
    physical tensor (the i-th dimension of the logical view corresponds
    to the dimension with number ``permutations[i]`` of the physical tensor).

    Permutation can be useful in case the logical order of
    the tensor is a permutation of the physical order (row-major).

    When logical and physical layout are equal, the permutation will always
    be ([0, 1, .., N-1]) and can therefore be left out.

* Description of the serialization:

  The metadata must be a valid JSON object including shape of
  the contained tensors as an array with key **"shape"** plus optional
  dimension names with keys **"dim_names"** and ordering of the
  dimensions with key **"permutation"**.

  - Example: ``{ "shape": [2, 5]}``
  - Example with ``dim_names`` metadata for NCHW ordered data:

    ``{ "shape": [100, 200, 500], "dim_names": ["C", "H", "W"]}``

  - Example of permuted 3-dimensional tensor:

    ``{ "shape": [100, 200, 500], "permutation": [2, 0, 1]}``

    This is the physical layout shape and the shape of the logical
    layout would in this case be ``[500, 100, 200]``.

.. note::

  Elements in a fixed shape tensor extension array are stored
  in row-major/C-contiguous order.

.. note::

  Other Data Structures in Arrow include a
  `Tensor (Multi-dimensional Array) <https://arrow.apache.org/docs/format/Other.html>`_
  to be used as a message in the interprocess communication machinery (IPC).

  This structure has no relationship with the Fixed shape tensor extension type defined
  by this specification. Instead, this extension type lets one use fixed shape tensors
  as elements in a field of a RecordBatch or a Table.

.. _variable_shape_tensor_extension:

Variable shape tensor
=====================

* Extension name: ``arrow.variable_shape_tensor``.

* The storage type of the extension is: ``StructArray`` where struct
  is composed of **data** and **shape** fields describing a single
  tensor per row:

  * **data** is a ``List`` holding tensor elements (each list element is
    a single tensor). The List's value type is the value type of the tensor,
    such as an integer or floating-point type.
  * **shape** is a ``FixedSizeList<int32>[ndim]`` of the tensor shape where
    the size of the list ``ndim`` is equal to the number of dimensions of the
    tensor.

* Extension type parameters:

  * **value_type** = the Arrow data type of individual tensor elements.

  Optional parameters describing the logical layout:

  * **dim_names** = explicit names to tensor dimensions
    as an array. The length of it should be equal to the shape
    length and equal to the number of dimensions.

    ``dim_names`` can be used if the dimensions have well-known
    names and they map to the physical layout (row-major).

  * **permutation**  = indices of the desired ordering of the
    original dimensions, defined as an array.

    The indices contain a permutation of the values [0, 1, .., N-1] where
    N is the number of dimensions. The permutation indicates which
    dimension of the logical layout corresponds to which dimension of the
    physical tensor (the i-th dimension of the logical view corresponds
    to the dimension with number ``permutations[i]`` of the physical tensor).

    Permutation can be useful in case the logical order of
    the tensor is a permutation of the physical order (row-major).

    When logical and physical layout are equal, the permutation will always
    be ([0, 1, .., N-1]) and can therefore be left out.

  * **uniform_shape** = sizes of individual tensor's dimensions which are
    guaranteed to stay constant in uniform dimensions and can vary in
    non-uniform dimensions. This holds over all tensors in the array.
    Sizes in uniform dimensions are represented with int32 values, while
    sizes of the non-uniform dimensions are not known in advance and are
    represented with null. If ``uniform_shape`` is not provided it is assumed
    that all dimensions are non-uniform.
    An array containing a tensor with shape (2, 3, 4) and whose first and
    last dimensions are uniform would have ``uniform_shape`` (2, null, 4).
    This allows for interpreting the tensor correctly without accounting for
    uniform dimensions while still permitting optional optimizations that
    take advantage of the uniformity.

* Description of the serialization:

  The metadata must be a valid JSON object that optionally includes
  dimension names with keys **"dim_names"** and  ordering of dimensions
  with key **"permutation"**.
  Shapes of tensors can be defined in a subset of dimensions by providing
  key **"uniform_shape"**.
  Minimal metadata is an empty string.

  - Example with ``dim_names`` metadata for NCHW ordered data (note that the first
    logical dimension, ``N``, is mapped to the **data** List array: each element in the List
    is a CHW tensor and the List of tensors implicitly constitutes a single NCHW tensor):

    ``{ "dim_names": ["C", "H", "W"] }``

  - Example with ``uniform_shape`` metadata for a set of color images
    with fixed height, variable width and three color channels:

    ``{ "dim_names": ["H", "W", "C"], "uniform_shape": [400, null, 3] }``

  - Example of permuted 3-dimensional tensor:

    ``{ "permutation": [2, 0, 1] }``

    For example, if the physical **shape** of an individual tensor
    is ``[100, 200, 500]``, this permutation would denote a logical shape
    of ``[500, 100, 200]``.

.. note::

  With the exception of ``permutation``, the parameters and storage
  of VariableShapeTensor relate to the *physical* storage of the tensor.

  For example, consider a tensor with::
    shape = [10, 20, 30]
    dim_names = [x, y, z]
    permutations = [2, 0, 1]

  This means the logical tensor has names [z, x, y] and shape [30, 10, 20].

.. note::
   Values inside each **data** tensor element are stored in row-major/C-contiguous
   order according to the corresponding **shape**.

.. _json_extension:

JSON
====

* Extension name: ``arrow.json``.

* The storage type of this extension is ``String`` or
  ``LargeString`` or ``StringView``.
  Only UTF-8 encoded JSON as specified in `rfc8259`_ is supported.

* Extension type parameters:

  This type does not have any parameters.

* Description of the serialization:

  Metadata is either an empty string or a JSON string with an empty object.
  In the future, additional fields may be added, but they are not required
  to interpret the array.

.. _uuid_extension:

UUID
====

* Extension name: ``arrow.uuid``.

* The storage type of the extension is ``FixedSizeBinary`` with a length of 16 bytes.

.. note::
   A specific UUID version is not required or guaranteed. This extension represents
   UUIDs as FixedSizeBinary(16) with big-endian notation and does not interpret the bytes in any way.

Opaque
======

Opaque represents a type that an Arrow-based system received from an external
(often non-Arrow) system, but that it cannot interpret.  In this case, it can
pass on Opaque to its clients to at least show that a field exists and
preserve metadata about the type from the other system.

Extension parameters:

* Extension name: ``arrow.opaque``.

* The storage type of this extension is any type.  If there is no underlying
  data, the storage type should be Null.

* Extension type parameters:

  * **type_name** = the name of the unknown type in the external system.
  * **vendor_name** = the name of the external system.

* Description of the serialization:

  A valid JSON object containing the parameters as fields.  In the future,
  additional fields may be added, but all fields current and future are never
  required to interpret the array.

  Developers **should not** attempt to enable public semantic interoperability
  of Opaque by canonicalizing specific values of these parameters.

Rationale
---------

Interfacing with non-Arrow systems requires a way to handle data that doesn't
have an equivalent Arrow type.  In this case, use the Opaque type, which
explicitly represents an unsupported field.  Other solutions are inadequate:

* Raising an error means even one unsupported field makes all operations
  impossible, even if (for instance) the user is just trying to view a schema.
* Dropping unsupported columns misleads the user as to the actual schema.
* An extension type may not exist for the unsupported type.
* Generating an extension type on the fly would falsely imply support.

Applications **should not** make conventions around vendor_name and type_name.
These parameters are meant for human end users to understand what type wasn't
supported.  Applications may try to interpret these fields, but must be
prepared for breakage (e.g., when the type becomes supported with a custom
extension type later on).  Similarly, **Opaque is not a generic container for
file formats**.  Considerations such as MIME types are irrelevant.  In both of
these cases, create a custom extension type instead.

Examples:

* A Flight SQL service that supports connecting external databases may
  encounter columns with unsupported types in external tables.  In this case,
  it can use the Opaque[Null] type to at least report that a column exists
  with a particular name and type name.  This lets clients know that a column
  exists, but is not supported.  Null is used as the storage type here because
  only schemas are involved.

  An example of the extension metadata would be::

    {"type_name": "varray", "vendor_name": "Oracle"}

* The ADBC PostgreSQL driver gets results as a series of length-prefixed byte
  fields.  But the driver will not always know how to parse the bytes, as
  there may be extensions (e.g. PostGIS).  It can use Opaque[Binary] to still
  return those bytes to the application, which may be able to parse the data
  itself.  Opaque differentiates the column from an actual binary column and
  makes it clear that the value is directly from PostgreSQL.  (A custom
  extension type is preferred, but there will always be extensions that the
  driver does not know about.)

  An example of the extension metadata would be::

    {"type_name": "geometry", "vendor_name": "PostGIS"}

* The ADBC PostgreSQL driver may also know how to parse the bytes, but not
  know the intended semantics.  For example, `composite types
  <https://www.postgresql.org/docs/current/rowtypes.html>`_ can add new
  semantics to existing types, somewhat like Arrow extension types.  The
  driver would be able to parse the underlying bytes in this case, but would
  still use the Opaque type.

  Consider the example in the PostgreSQL documentation of a ``complex`` type.
  Mapping the type to a plain Arrow ``struct`` type would lose meaning, just
  like how an Arrow system deciding to treat all extension types by dropping
  the extension metadata would be undesirable.  Instead, the driver can use
  Opaque[Struct] to pass on the composite type info.  (It would be wrong to
  try to map this to an Arrow-defined complex type: it does not know the
  proper semantics of a user-defined type, which cannot and should not be
  hardcoded into the driver in the first place.)

  An example of the extension metadata would be::

    {"type_name": "database_name.schema_name.complex", "vendor_name": "PostgreSQL"}

* The JDBC adapter in the Arrow Java libraries converts JDBC result sets into
  Arrow arrays, and can get Arrow schemas from result sets.  JDBC, however,
  allows drivers to return `arbitrary Java objects
  <https://docs.oracle.com/javase/8/docs/api/java/sql/Types.html#OTHER>`_.

  The driver can use Opaque[Null] as a placeholder during schema conversion,
  only erroring if the application tries to fetch the actual data.  That way,
  clients can at least introspect result schemas to decide whether it can
  proceed to fetch the data, or only query certain columns.

  An example of the extension metadata would be::

    {"type_name": "OTHER", "vendor_name": "JDBC driver name"}

8-bit Boolean
=============

Bool8 represents a boolean value using 1 byte (8 bits) to store each value instead of only 1 bit as in
the original Arrow Boolean type. Although less compact than the original representation, Bool8 may have
better zero-copy compatibility with various systems that also store booleans using 1 byte.

* Extension name: ``arrow.bool8``.

* The storage type of this extension is ``Int8`` where:

  * **false** is denoted by the value ``0``.
  * **true** can be specified using any non-zero value. Preferably ``1``.

* Extension type parameters:

  This type does not have any parameters.

* Description of the serialization:

  Metadata is an empty string.

.. _variant_extension:

Variant
=======

Variant represents a value that may be one of:

* Primitive: a type and corresponding value (e.g. INT, STRING)

* Array: An ordered list of Variant values

* Object: An unordered collection of string/Variant pairs (i.e. key/value pairs). An object may not contain duplicate keys

Particularly, this provides a way to represent semi-structured data which is stored as a
`Parquet Variant <https://github.com/apache/parquet-format/blob/master/VariantEncoding.md>`__ value within Arrow columns in
a lossless fashion. This also provides the ability to represent `shredded <https://github.com/apache/parquet-format/blob/master/VariantShredding.md>`__
variant values. This will make it easy for systems to pass Variant data around without having to upgrade their Arrow version
or otherwise require special handling unless they want to directly interact with the encoded variant data. See the previous links
to the Parquet format specification for details on what the actual binary values should look like.

* Extension name: ``parquet.variant``.

* The storage type of this extension is a ``StructArray`` that obeys the following rules:

  * A *non-nullable* field named ``metadata`` which is of type ``Binary``, ``LargeBinary``, or ``BinaryView``.

  * At least one (or both) of the following:

    * A field named ``value`` which is of type ``Binary``, ``LargeBinary``, or ``BinaryView``.
      *(unshredded variants consist of just the ``metadata`` and ``value`` fields only)*

    * A field named ``typed_value`` which can be any *primitive type* or a ``List``, ``LargeList``, ``ListView`` or ``Struct``

      * If the ``typed_value`` field is a *nested* type, its elements **must** be *non-nullable* and **must** be a ``struct`` consisting of
        at least one (or both) of the following:

        * A field named ``value`` which is of type ``Binary``, ``LargeBinary``, or ``BinaryView``.

        * A field named ``typed_value`` which follows these same rules for the ``typed_value`` field.

.. note::

  It is also *permissible* for the ``metadata`` field to be dictionary-encoded with an index type of ``int8``.

.. note::

  The fields may be in any order, and thus must be accessed by **name** not by *position*.

Examples
--------

Unshredded
''''''''''

The simplest case, an unshredded variant always consists of **exactly** two fields: ``metadata`` and ``value``. Any of
the following storage types are valid (not an exhaustive list):

* ``struct<metadata: binary required, value: binary required>``
* ``struct<value: binary optional, metadata: binary required>``
* ``struct<metadata: dictionary<int8, binary> required, value: binary_view required>``

Simple Shredding
''''''''''''''''

Suppose a Variant field named *measurement* and we want to shred the ``int64`` values into a separate column for efficiency.
In Parquet, this could be represented as::

  required group measurement (VARIANT) {
    required binary metadata;
    optional binary value;
    optional int64 typed_value;
  }

Thus the corresponding storage type for the ``parquet.variant`` Arrow extension type would be: ::

  struct<
    metadata: binary required,
    value: binary optional,
    typed_value: int64 optional
  >

If we suppose a series of measurements consisting of: ::

  34, null, "n/a", 100

The data should be stored/represented in Arrow as: ::

  * Length: 4, Null count: 1
  * Validity Bitmap buffer:

    | Byte 0 (validity bitmap) | Bytes 1-63    |
    |--------------------------|---------------|
    | 00001011                 | 0 (padding)   |

  * Children arrays:
    * field-0 array (`VarBinary`)
      * Length: 4, Null count: 0
      * Offsets buffer:

        | Bytes 0-19       | Bytes 20-63              |
        |------------------|--------------------------|
        | 0, 2, 4, 6, 8    | unspecified (padding)    |

      * Value buffer: (01 00 -> indicates version 1 empty metadata)

        | Bytes 0-7          | Bytes 8-63               |
        |--------------------|--------------------------|
        | 01 00 01 00 01 00  | unspecified (padding)    |

    * field-1 array (`VarBinary`)
      * Length: 4, Null count: 2
      * Validity Bitmap buffer:

        | Byte 0 (validity bitmap) | Bytes 1-63    |
        |--------------------------|---------------|
        | 00000110                 | 0 (padding)   |

      * Offsets buffer:

        | Bytes 0-19       | Bytes 20-63              |
        |------------------|--------------------------|
        | 0, 0, 1, 5, 5    | unspecified (padding)    |

      * Value buffer: (`00` -> literal null, `0x13 0x6E 0x2F 0x61` -> variant encoding literal string "n/a")

        | Bytes 0-4              | Bytes 5-63               |
        |------------------------|--------------------------|
        | 00 0x13 0x6E 0x2F 0x61 | unspecified (padding)    |

    * field-2 array (int64 array)
      * Length: 4, Null count: 2
      * Validity Bitmap buffer:

        | Byte 0 (validity bitmap) | Bytes 1-63    |
        |--------------------------|---------------|
        | 00001001                 | 0 (padding)   |

      * Value buffer:

        | Bytes 0-31          | Bytes 32-63               |
        |---------------------|--------------------------|
        | 34, 00, 00, 100     | unspecified (padding)    |

.. note::

  Notice that there is a variant ``literal null`` in the ``value`` array, this is due to the
  `shredding specification <https://github.com/apache/parquet-format/blob/master/VariantShredding.md#value-shredding>`__
  so that a consumer can tell the difference between a *missing* field and a **null** field.

Shredding an Array
''''''''''''''''''

For our next example, we will represent a shredded array of strings. Let's consider a column that looks like: ::

  ["comedy", "drama"], ["horror", null], ["comedy", "drama", "romance"], null

Representing this shredded variant in Parquet could look like: ::

  optional group tags (VARIANT) {
    required binary metadata;
    optional binary value;
    optional group typed_value (LIST) { # optional to allow null lists
      repeated group list {
        required group element {        # shredded element
          optional binary value;
          optional binary typed_value (STRING);
        }
      }
    }
  }

The array structure for Variant encoding does not allow missing elements, so all elements of the array must
be *non-nullable*. As such, either **typed_value** or **value** (*but not both!*) must be *non-null*. A null
element must be encoded as a Variant null: *basic type* ``0`` (primitive) and *physical type* ``0`` (null).

The storage type to represent this in Arrow as a Variant extension type would be: ::

  struct<
    metadata: binary required,
    value: binary optional,
    typed_value: list<element: struct<
      value: binary optional,
      typed_value: string optional
    > required> optional
  >

.. note::

  As usual, **Binary** could also be **LargeBinary** or **BinaryView**, **String** could also be **LargeString** or **StringView**,
  and **List** could also be **LargeList** or **ListView**.

The data would then be stored in Arrow as follows: ::

  * Length: 4, Null count: 1
  * Validity Bitmap buffer:

    | Byte 0 (validity bitmap) | Bytes 1-63    |
    |--------------------------|---------------|
    | 00000111                 | 0 (padding)   |

  * Children arrays:
    * field-0 array (`VarBinary` Metadata)
      * Length: 4, Null count: 0
      * Offsets buffer:

        | Bytes 0-19       | Bytes 20-63              |
        |------------------|--------------------------|
        | 0, 2, 4, 6, 8    | unspecified (padding)    |

      * Value buffer: (01 00 -> indicates version 1 empty metadata)

        | Bytes 0-7          | Bytes 8-63               |
        |--------------------|--------------------------|
        | 01 00 01 00 01 00  | unspecified (padding)    |

    * field-1 array (`VarBinary` Value)
      * Length: 4, Null count: 1
      * Validity bitmap buffer:

        | Byte 0 (validity bitmap) | Bytes 1-63    |
        |--------------------------|---------------|
        | 00001000                 | 0 (padding)   |

      * Offsets buffer:

        | Bytes 0-19       | Bytes 20-63              |
        |------------------|--------------------------|
        | 0, 0, 0, 0, 1    | unspecified (padding)    |

      * Value buffer: (00 -> variant null)

        | Bytes 0            | Bytes 1-63               |
        |--------------------|--------------------------|
        | 00                 | unspecified (padding)    |

    * field-2 array (`List<Struct<VarBinary, String>>` typed_value)
      * Length: 4, Null count: 1
      * Validity bitmap buffer:

        | Byte 0 (validity bitmap) | Bytes 1-63  |
        |--------------------------|-------------|
        | 00000111                 | 0 (padding) |

      * Offsets buffer (int32)

        | Bytes 0-19        | Bytes 20-63           |
        |-------------------|-----------------------|
        | 0, 2, 4, 7, 7     | unspecified (padding) |

      * Values array (`Struct<VarBinary, String>` element):
        * Length: 7, Null count: 0
        * Validity bitmap buffer: Not required

        * Children arrays:
          * field-0 array (`VarBinary` value)
            * Length: 7, Null count: 6
            * Validity bitmap buffer:

              | Byte 0 (validity bitmap) | Bytes 1-63  |
              |--------------------------|-------------|
              | 00001000                 | 0 (padding) |

            * Offsets buffer (int32):

              | Bytes 0-31                | Bytes 32-63              |
              |---------------------------|--------------------------|
              | 0, 0, 0, 0, 1, 1, 1, 1    | unspecified (padding)    |

            * Values buffer (`00` -> variant null):

              | Bytes 0            | Bytes 1-63               |
              |--------------------|--------------------------|
              | 00                 | unspecified (padding)    |

          * field-1 array (`String` typed_value)
            * Length: 7, Null count: 1
            * Validity bitmap buffer:

              | Byte 0 (validity bitmap) | Bytes 1-63  |
              |--------------------------|-------------|
              | 01110111                 | 0 (padding) |

            * Offsets buffer (int32):

              | Bytes 0-31                      | Bytes 32-63              |
              |---------------------------------|--------------------------|
              | 0, 6, 11, 17, 17, 23, 28, 35    | unspecified (padding)    |

            * Values buffer:

              | Bytes 0-35                           | Bytes 36-63              |
              |--------------------------------------|--------------------------|
              | comedydramahorrorcomedydramaromance  | unspecified (padding)    |

Shredding an Object
'''''''''''''''''''

Let's consider a simple JSON column of "events" which contain a field named ``event_type`` (a string)
and a field named ``event_ts`` (a timestamp) that we wish to shred into separate columns, In Parquet,
it could look something like this: ::

  optional group event (VARIANT) {
    required binary metadata;
    optional binary value;        # variant, remaining fields/values
    optional group typed_value {  # shredded fields for variant object
      required group event_type { # event_type shredded field
        optional binary value;
        optional binary typed_value (STRING);
      }
      required group event_ts {   # event_ts shredded field
        optional binary value;
        optional int64 typed_value (TIMESTAMP(true, MICROS))
      }
    }
  }

We can then, fairly easily, translate this into the expected extension storage type: ::

  struct<
    metadata: binary required,
    value: binary optional,
    typed_value: struct<
      event_type: struct<
        value: binary optional,
        typed_value: string optional
      > required,
      event_ts: struct<
        value: binary optional,
        typed_value: timestamp(us, UTC) optional
      > required
    > optional
  >

If a field *does not exist* in the variant object value, then both the **value** and **typed_value** columns for that row
will be null. If a field is *present*, but the value is null, then **value** must contain a Variant null: *basic type*
``0`` (primitive) and *physical type* ``0`` (null).

It is *invalid* for both **value** and **typed_value** to be non-null for a given index. A reader can choose not to error
in this scenario, but if so it **must** use the value in the **typed_value** column for that index.

Let's consider the following series of objects: ::

  {"event_type": "noop", "event_ts": 1729794114937}

  {"event_type": "login", "event_ts": 1729794146402, "email": "user@example.com"}

  {"error_msg": "malformed..."}

  "malformed: not an object"

  {"event_ts": 1729794240241, "click": "_button"}

  {"event_ts": null, "event_ts": 1729794954163}

  {"event_type": "noop", "event_ts": "2024-10-24"}

  {}

  null

  *Entirely missing*

To represent those values as a column of Variant values using the Variant extension type we get the following: ::

  * Length: 10, Null count: 1
  * Validity bitmap buffer:

    | Byte 0 (validity bitmap) | Byte 1    | Bytes 2-63            |
    |--------------------------|-----------|-----------------------|
    | 11111111                 | 00000001  | 0 (padding)           |

  * Children arrays
    * field-0 array (`VarBinary` Metadata)
      * Length: 10, Null count: 0
      * Offsets buffer:

        | Bytes 0-43 (int32)                       | Bytes 44-63             |
        |------------------------------------------|-------------------------|
        | 0, 2, 11, 24, 26, 35, 37, 39, 41, 43, 45 | unspecified (padding)   |

      * Value buffer: (01 00 -> version 1 empty metadata,
                       01 01 00 XX ... -> Version 1, metadata with 1 elem, offset 0, offset XX == len(string), ... is dict string bytes)

        | Bytes 0-1 | Bytes 2-10        | Bytes 11-23           | Bytes 24-25 | Bytes 26-34       |
        |-------------------------------|-----------------------|-------------|-------------------|
        | 01 00     | 01 01 00 05 email | 01 01 00 09 error_msg | 01 00       | 01 01 00 05 click |

        | Bytes 35-36 | Bytes 37-38 | Bytes 39-40 | Bytes 41-42 | Bytes 43-44 | Bytes 45-63           |
        |-------------|-------------|-------------|-------------|-------------|-----------------------|
        | 01 00       | 01 00       | 01 00       | 01 00       | 01 00       | unspecified (padding) |

    * field-1 array (`VarBinary` Value)
      * Length: 10, Null count: 5
      * Validity bitmap buffer:

        | Byte 0 (validity bitmap)  | Byte 1    | Bytes 2-63            |
        |---------------------------|-----------|-----------------------|
        | 00011110                  | 00000001  | 0 (padding)           |

      * Offsets buffer (filled in based on lengths of encoded variants):

        | ... |

      * Value buffer:

        | VariantEncode({"email": "user@email.com"}) | VariantEncode({"error_msg": "malformed..."}) |
        | VariantEncode("malformed: not an object")  | VariantEncode({"click": "_button"})          | 00 (null) |

    * field-2 array (`Struct<...>` typed_value)
      * Length: 10, Null count: 3
      * Validity bitmap buffer:

        | Byte 0 (validity bitmap) | Byte 1    | Bytes 2-63            |
        |--------------------------|-----------|-----------------------|
        | 11110111                 | 00000000  | 0 (padding)           |

      * Children arrays:
        * field-0 array (`Struct<VarBinary, String>` event_type)
          * Length: 10, Null count: 0
          * Validity bitmap buffer: not required

          * Children arrays
            * field-0 array (`VarBinary` value)
              * Length: 10, Null count: 9
              * Validity bitmap buffer:

                | Byte 0 (validity bitmap) | Byte 1    | Bytes 2-63            |
                |--------------------------|-----------|-----------------------|
                | 01000000                 | 00000000  | 0 (padding)           |

              * Offsets buffer (int32)

                | Bytes 0-43 (int32)              | Bytes 44-63             |
                |---------------------------------|-------------------------|
                | 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1 | unspecified (padding)   |

              * Value buffer:

                | Byte 0 | Bytes 1-63             |
                |--------|------------------------|
                | 00     | unspecified (padding)  |

            * field-1 array (`String` typed_value)
              * Length: 10, Null count: 7
              * Validity bitmap buffer:

                | Byte 0 (validity bitmap) | Byte 1    | Bytes 2-63            |
                |--------------------------|-----------|-----------------------|
                | 01000011                 | 00000000  | 0 (padding)           |

              * Offsets buffer (int32)

                | Byte 0-43                           | Bytes 44-63            |
                |-------------------------------------|------------------------|
                | 0, 4, 9, 9, 9, 9, 9, 13, 13, 13, 13 | unspecified (padding)  |

              * Value buffer:

                | Bytes 0-3 | Bytes 4-8 | Bytes 9-12 | Bytes 13-63            |
                |-----------|-----------|------------|------------------------|
                | noop      | login     | noop       | unspecified (padding)  |


        * field-1 array (`Struct<VarBinary, Timestamp>` event_ts)
          * Length: 10, Null count: 0
          * Validity bitmap buffer: not required

          * Children arrays
            * field-0 array (`VarBinary` value)
              * Length: 10, Null count: 9
              * Validity bitmap buffer:

                | Byte 0 (validity bitmap) | Byte 1    | Bytes 2-63            |
                |--------------------------|-----------|-----------------------|
                | 01000000                 | 00000000  | 0 (padding)           |

              * Offsets buffer (int32)

                | Bytes 0-43 (int32)              | Bytes 44-63             |
                |---------------------------------|-------------------------|
                | ...                             | unspecified (padding)   |

              * Value buffer:

                | VariantEncode("2024-10-24")     |

            * field-1 array (`Timestamp(us, UTC)` typed_value)
              * Length: 10, Null count: 6
              * Validity bitmap buffer:

                | Byte 0 (validity bitmap) | Byte 1    | Bytes 2-63            |
                |--------------------------|-----------|-----------------------|
                | 00110011                 | 00000000  | 0 (padding)           |

              * Value buffer:

                | Bytes 0-7     | Bytes 8-15    | Bytes 16-31  | Bytes 32-39   | Bytes 40-47   | Bytes 48-63            |
                |---------------|---------------|--------------|---------------|---------------|------------------------|
                | 1729794114937 | 1729794146402 | unspecified  | 1729794240241 | 1729794954163 | unspecified (padding)  |


Putting it all together
'''''''''''''''''''''''

As mentioned, the **typed_value** field associated with a Variant **value** can be of any shredded type. As a result,
as long as we follow the original rules you can have an arbitrary number of nested levels based on how you want to
shred the object. For example, we might have a few more fields alongside **event_type** to shred out. Possibly an object
that looks like this: ::

  {
    "event_type": "login",
    “event_ts”: 1729794114937,
    “location”: { “longitude”: 1.5, “latitude”: 5.5 },
    “tags”: [“foo”, “bar”, “baz”]
  }

If we shred the extra fields out and represent it as Parquet it looks like: ::

  optional group event (VARIANT) {
    required binary metadata;
    optional binary value;        # variant, remaining fields/values
    optional group typed_value {  # shredded fields for variant object
      required group event_type { # event_type shredded field
        optional binary value;
        optional binary typed_value (STRING);
      }
      required group event_ts {   # event_ts shredded field
        optional binary value;
        optional int64 typed_value (TIMESTAMP(true, MICROS))
      }
      required group location {   # location shredded field
        optional binary value;
        optional group typed_value {
          required group longitude {
            optional binary value;
            optional float64 typed_value;
          }
          required group latitude {
            optional binary value;
            optional float64 typed_value;
          }
        }
      }
      required group tags {       # tags shredded field
        optional binary value;
        optional group typed_value (LIST) {
          repeated group list {
            required group element {
              optional binary value;
              optional binary typed_value (STRING);
            }
          }
        }
      }
    }
  }

Finally, following the rules we set forth on constructing the Variant Extension Type storage type, we end up with: ::

  struct<
    metadata: binary required,
    value: binary optional,
    typed_value: struct<
      event_type: struct<value: binary optional, typed_value: string optional> required,
      event_ts: struct<value: binary optional, typed_value: timestamp(us, UTC) optional> required,
      location: struct<
        value: binary optional,
        typed_value: struct<
          longitude: struct<value: binary optional, typed_value: double optional> required,
          latitude: struct<value: binary optional, typed_value: double optional> required
        > optional> required,
      tags: struct<
          value: binary optional,
          typed_value: list<struct<value: binary optional, typed_value: string optional> required> optional
        > required
    > optional
  >


Community Extension Types
=========================

In addition to the canonical extension types listed above, there exist Arrow
extension types that have been established as standards within specific domain
areas. These have not been officially designated as canonical through a
discussion and vote on the Arrow development mailing list but are well known
within subcommunities of Arrow developers.

GeoArrow
========

`GeoArrow <https://github.com/geoarrow/geoarrow>`_ defines a collection of
Arrow extension types for representing vector geometries. It is well known
within the Arrow geospatial subcommunity. The GeoArrow specification is not yet
finalized.

.. _rfc8259: https://datatracker.ietf.org/doc/html/rfc8259
