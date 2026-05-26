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

.. highlight:: console
.. _development-cpp-compute:

============================
Developing Arrow C++ Compute
============================

This section provides information for developers of the Arrow C++ Compute module.

Row Table
=========

The row table in Arrow represents data stored in row-major format. This format
is particularly useful for scenarios involving random access to individual rows
and where all columns are frequently accessed together. It is especially
advantageous for hash-table keys and facilitates efficient operations such as
grouping and hash joins by optimizing memory access patterns and data locality.

Metadata
--------

A row table is defined by its metadata, ``RowTableMetadata``, which includes
information about its schema, alignment, and derived properties.

The schema specifies the types and order of columns. Each row in the row table
contains the data for each column in that logical order (the physical order may
vary; see :ref:`row-encoding` for details).

.. note::
   Columns of nested types or large binary types are **not** supported in the
   row table.

One important property derived from the schema is whether the row table is
fixed-length or varying-length. A fixed-length row table contains only
fixed-length columns, while a varying-length row table includes at least one
varying-length column. This distinction determines how data is stored and
accessed in the row table.

Each row in the row table is aligned to ``RowTableMetadata::row_alignment``
bytes. Fixed-length columns with non-power-of-2 lengths are also aligned to
``RowTableMetadata::row_alignment`` bytes. Varying-length columns are aligned to
``RowTableMetadata::string_alignment`` bytes.

Buffer Layout
-------------

Similar to most Arrow ``Array``\s, the row table consists of three buffers:

- **Null Masks Buffer**: Indicates null values for each column in each row.
- **Fixed-length Buffer**: Stores row data for fixed-length tables or offsets to
  varying-length data for varying-length tables.
- **Varying-length Buffer** (Optional): Contains row data for varying-length
  tables; unused for fixed-length tables.

Row Format
----------

Null Masks
~~~~~~~~~~

For each row, a contiguous sequence of bits represents whether each column in
that row is null. Each bit corresponds to a specific column, with ``1``
indicating the value is null and ``0`` indicating the value is valid. Note that
this is the opposite of how the validity bitmap works for ``Array``\s. The null
mask for a row occupies ``RowTableMetadata::null_masks_bytes_per_row`` bytes.

Fixed-length Row Data
~~~~~~~~~~~~~~~~~~~~~

In a fixed-length row table, row data is directly stored in the fixed-length
buffer. All columns in each row are stored sequentially. Notably, a ``boolean``
column is special because, in a normal Arrow ``Array``, it is stored using 1
bit, whereas in a row table, it occupies 1 byte. The varying-length buffer is
not used in this case.

For example, a row table with the schema ``(int32, boolean)`` and rows
``[[7, false], [8, true], [9, false], ...]`` is stored in the fixed-length
buffer as follows:

.. list-table::
   :header-rows: 1

   * - Row 0
     - Row 1
     - Row 2
     - ...
   * - ``7 0 0 0, 0 (padding)``
     - ``8 0 0 0, 1 (padding)``
     - ``9 0 0 0, 0 (padding)``
     - ...

Offsets for Varying-length Row Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In a varying-length row table, the fixed-length buffer contains offsets to the
varying-length row data, which is stored separately in the optional
varying-length buffer. The offsets are of type ``RowTableMetadata::offset_type``
(fixed as ``int64_t``) and indicate the starting position of the row data for
each row.

Varying-length Row Data
~~~~~~~~~~~~~~~~~~~~~~~

In a varying-length row table, the varying-length buffer contains the actual row
data, stored contiguously. The offsets in the fixed-length buffer point to the
starting position of each row's data.

.. _row-encoding:

Row Encoding
^^^^^^^^^^^^

A varying-length row is encoded as follows:

- Fixed-length columns are stored first.
- A sequence of offsets to each varying-length column follows. Each offset is
  32-bit and indicates the **end** position within the row data of the
  corresponding varying-length column.
- Varying-length columns are stored last.

For example, a row table with the schema ``(int32, string, string, int32)`` and
rows ``[[7, 'Alice', 'x', 0], [8, 'Bob', 'y', 1], [9, 'Charlotte', 'z', 2], ...]``
is stored as follows (assuming 8-byte alignment for varying-length columns):

Fixed-length buffer (row offsets):

.. list-table::
   :header-rows: 1

   * - Row 0
     - Row 1
     - Row 2
     - Row 3
     - ...
   * - ``0 0 0 0 0 0 0 0``
     - ``32 0 0 0 0 0 0 0``
     - ``64 0 0 0 0 0 0 0``
     - ``104 0 0 0 0 0 0 0``
     - ...

Varying-length buffer (row data):

.. list-table::
   :header-rows: 1

   * - Row
     - Fixed-length Cols
     - Varying-length Offsets
     - Varying-length Cols
   * - 0
     - ``7 0 0 0, 0 0 0 0``
     - ``21 0 0 0, 25 0 0 0``
     - ``Alice~~~x~~~~~~~``
   * - 1
     - ``8 0 0 0, 1 0 0 0``
     - ``19 0 0 0, 25 0 0 0``
     - ``Bob~~~~~y~~~~~~~``
   * - 2
     - ``9 0 0 0, 2 0 0 0``
     - ``25 0 0 0, 33 0 0 0``
     - ``Charlotte~~~~~~~z~~~~~~~``
   * - 3
     - ...
     - ...
     - ...
