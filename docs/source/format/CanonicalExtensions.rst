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

The Arrow Columnar Format allows defining
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
     (for example, a trivial custom text format or JSON would be acceptable).

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

* Extension name: `arrow.fixed_shape_tensor`.

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

* Extension name: `arrow.variable_shape_tensor`.

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

=========================
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
