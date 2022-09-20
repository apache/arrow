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

========
Glossary
========

.. glossary::
   :sorted:

   array
   vector
       A *contiguous*, *one-dimensional* sequence of values with known
       length where all values have the same type.  An array consists
       of zero or more :term:`buffers <buffer>`, a non-negative
       length, and a :term:`data type`.  The buffers of an array are
       laid out according to the data type as defined by the columnar
       format.

       Arrays are contiguous in the sense that iterating the values of
       an array will iterate through a single set of buffers, even
       though an array may consist of multiple disjoint buffers, or
       may consist of child arrays that themselves span multiple
       buffers.

       Arrays are one-dimensional in that they are a sequence of
       :term:`slots <slot>` or singular values, even though for some
       data types (like structs or unions), a slot may represent
       multiple values.

       Defined by the :doc:`./Columnar`.

   buffer
       A *contiguous* region of memory with a given length.  Buffers
       are used to store data for arrays.

       Buffers may be in CPU memory, memory-mapped from a file, in
       device (e.g. GPU) memory, etc., though not all Arrow
       implementations support all of these possibilities.

   child array
   parent array
       In an array of a :term:`nested type`, the parent array
       corresponds to the :term:`parent type` and the child array(s)
       correspond to the :term:`child type(s) <child type>`.  For
       example, a ``List[Int32]``-type parent array has an
       ``Int32``-type child array.

   child type
   parent type
       In a :term:`nested type`, the nested type is the parent type,
       and the child type(s) are its parameters.  For example, in
       ``List[Int32]``, ``List`` is the parent type and ``Int32`` is
       the child type.

   chunked array
       A *discontiguous*, *one-dimensional* sequence of values with
       known length where all values have the same type.  Consists of
       zero or more :term:`arrays <array>`, the "chunks".

       Chunked arrays are discontiguous in the sense that iterating
       the values of a chunked array may require iterating through
       different buffers for different indices.

       Not part of the columnar format; this term is specific to
       certain language implementations of Arrow (primarily C++ and
       its bindings).

       .. seealso:: :term:`record batch`, :term:`table`

   complex type
   nested type
       A :term:`data type` whose structure depends on one or more
       other :term:`child data types <child type>`. For instance,
       ``List`` is a nested type that has one child.

       Two nested types are equal if and only if their child types are
       also equal.

   data type
   type
       A type that a value can take, such as ``Int8`` or
       ``List[Utf8]``. The type of an array determines how its values
       are laid out in memory according to :doc:`./Columnar`.

       .. seealso:: :term:`nested type`, :term:`primitive type`

   dictionary
       An array of values that accompany a :term:`dictionary-encoded
       <dictionary-encoding>` array.

   dictionary-encoding
       An array that stores its values as indices into a
       :term:`dictionary` array instead of storing the values
       directly.

       .. seealso:: :ref:`dictionary-encoded-layout`

   extension type
   storage type
       A user-defined :term:`data type` that adds additional semantics
       to an existing data type.  This allows implementations that do
       not support a particular extension type to still handle the
       underlying data type (the "storage type").

       For example, a UUID can be represented as a 16-byte fixed-size
       binary type.

       .. seealso:: :ref:`format_metadata_extension_types`

   field
       A column in a :term:`schema`.  Consists of a field name, a
       :term:`data type`, a flag indicating whether the field is
       nullable or not, and optional key-value metadata.

   IPC format
       A specification for how to serialize Arrow data, so it can be
       sent between processes/machines, or persisted on disk.

       .. seealso:: :term:`IPC file format`,
                    :term:`IPC streaming format`

   IPC file format
   file format
   random-access format
       An extension of the :term:`IPC streaming format` that can be
       used to serialize Arrow data to disk, then read it back with
       random access to individual record batches.

   IPC message
   message
       The IPC representation of a particular in-memory structure,
       like a record batch or schema.

   IPC streaming format
   streaming format
       A protocol for streaming Arrow data or for serializing data to
       a file, consisting of a stream of :term:`IPC messages <IPC
       message>`.

   physical layout
       A specification for how to arrange values in memory.

       .. seealso:: :ref:`format_layout`

   primitive type
       A data type that does not have any child types.

       .. seealso:: :term:`data type`

   record batch
       **In the :ref:`IPC format <format-ipc>`**: the primitive unit
       of data.  A record batch consists of an ordered list of
       :term:`buffers <buffer>` corresponding to a :term:`schema`.

       **In some implementations** (primarily C++ and its bindings): a
       *contiguous*, *two-dimensional* chunk of data.  A record batch
       consists of an ordered collection of :term:`arrays <array>` of
       the same length.

       Like arrays, record batches are contiguous in the sense that
       iterating the rows of a record batch will iterate through a
       single set of buffers.

   schema
       A collection of :term:`fields <field>` with optional metadata
       that determines all the :term:`data types <data type>` of an
       object like a :term:`record batch` or :term:`table`.

   slot
       A single logical value within an array, i.e. a "row".

   table
       A *discontiguous*, *two-dimensional* chunk of data consisting
       of an ordered collection of :term:`chunked arrays <chunked
       array>`.  All chunked arrays have the same length, but may have
       different types.  Different columns may be chunked
       differently.

       Like chunked arrays, tables are discontiguous in the sense that
       iterating the rows of a table may require iterating through
       different buffers for different indices.

       Not part of the columnar format; this term is specific to
       certain language implementations of Arrow (for example C++ and
       its bindings, and Go).

       .. image:: ../cpp/tables-versus-record-batches.svg
          :alt: A graphical representation of an Arrow Table and a 
                Record Batch, with structure as described in text above.

       .. seealso:: :term:`chunked array`, :term:`record batch`
