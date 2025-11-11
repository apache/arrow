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

.. _parquet_variant_extension:

Parquet Variant
===============

Variant represents a value that may be one of:

* Primitive: a type and corresponding value (e.g. ``INT``, ``STRING``)

* Array: An ordered list of Variant values

* Object: An unordered collection of string/Variant pairs (i.e. key/value pairs). An object may not contain duplicate keys

Particularly, this provides a way to represent semi-structured data which is stored as a
`Parquet Variant <https://github.com/apache/parquet-format/blob/master/VariantEncoding.md>`__ value within Arrow columns in
a lossless fashion. This also provides the ability to represent `shredded <https://github.com/apache/parquet-format/blob/master/VariantShredding.md>`__
variant values. The canonical extension type allows systems to pass Variant encoded data around without special handling unless
they want to directly interact with the encoded variant data. See the Parquet format specification for details on what the actual
binary values look like.

* Extension name: ``arrow.parquet.variant``.

* The storage type of this extension is a ``Struct`` that obeys the following rules:

  * A *non-nullable* field named ``metadata`` which is of type ``Binary``, ``LargeBinary``, or ``BinaryView``.

  * At least one (or both) of the following:

    * A field named ``value`` which is of type ``Binary``, ``LargeBinary``, or ``BinaryView``.
      (unshredded variants consist of just the ``metadata`` and ``value`` fields only)

    * A field named ``typed_value`` which can be a :ref:`variant_primitive_type_mapping` or a ``List``, ``LargeList``, ``ListView`` or ``Struct``

      * If the ``typed_value`` field is a ``List``, ``LargeList`` or ``ListView`` its elements **must** be *non-nullable* and **must**
        be a ``Struct`` consisting of at least one (or both) of the following:

        * A field named ``value`` which is of type ``Binary``, ``LargeBinary``, or ``BinaryView``.

        * A field named ``typed_value`` which follows the rules outlined above (this allows for arbitrarily nested data).

      * If the ``typed_value`` field is a ``Struct``, then its fields **must** be *non-nullable*, representing the fields being shredded
        from the objects, and **must** be a ``Struct`` consisting of at least one (or both) of the following:

        * A field named ``value`` which is of type ``Binary``, ``LargeBinary``, or ``BinaryView``.

        * A field named ``typed_value`` which follows the rules outlined above (this allows for arbitrarily nested data).

* Extension type parameters:

  This type does not have any parameters.

* Description of the serialization:

  Extension metadata is an empty string.

.. note::

   It is also *permissible* for the ``metadata`` field to be dictionary-encoded with a preferred (*but not required*) index type of ``int8``,
   or run-end-encoded with a preferred (*but not required*) runs type of ``int8``.

.. note::

   The fields may be in any order, and thus must be accessed by **name** not by *position*. The field names are case sensitive.

.. _variant_primitive_type_mapping:

Primitive Type Mappings
-----------------------

+----------------------+------------------------+
| Arrow Primitive Type | Variant Primitive Type |
+======================+========================+
| Null                 | Null                   |
+----------------------+------------------------+
| Boolean              | Boolean (true/false)   |
+----------------------+------------------------+
| Int8                 | Int8                   |
+----------------------+------------------------+
| Uint8                | Int16                  |
+----------------------+------------------------+
| Int16                | Int16                  |
+----------------------+------------------------+
| Uint16               | Int32                  |
+----------------------+------------------------+
| Int32                | Int32                  |
+----------------------+------------------------+
| Uint32               | Int64                  |
+----------------------+------------------------+
| Int64                | Int64                  |
+----------------------+------------------------+
| Float                | Float                  |
+----------------------+------------------------+
| Double               | Double                 |
+----------------------+------------------------+
| Decimal32            | decimal4               |
+----------------------+------------------------+
| Decimal64            | decimal8               |
+----------------------+------------------------+
| Decimal128           | decimal16              |
+----------------------+------------------------+
| Date32               | Date                   |
+----------------------+------------------------+
| Time64               | TimeNTZ                |
+----------------------+------------------------+
| Timestamp(us, UTC)   | Timestamp (micro)      |
+----------------------+------------------------+
| Timestamp(us)        | TimestampNTZ (micro)   |
+----------------------+------------------------+
| Timestamp(ns, UTC)   | Timestamp (nano)       |
+----------------------+------------------------+
| Timestamp(ns)        | TimestampNTZ (nano)    |
+----------------------+------------------------+
| Binary               | Binary                 |
+----------------------+------------------------+
| LargeBinary          | Binary                 |
+----------------------+------------------------+
| BinaryView           | Binary                 |
+----------------------+------------------------+
| String               | String                 |
+----------------------+------------------------+
| LargeString          | String                 |
+----------------------+------------------------+
| StringView           | String                 |
+----------------------+------------------------+
| UUID extension type  | UUID                   |
+----------------------+------------------------+

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
