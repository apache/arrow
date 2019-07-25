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

.. default-domain:: cpp
.. highlight:: cpp

High-Level Overview
===================

The Arrow C++ library is comprised of different parts, each of which serves
a specific purpose.

The physical layer
------------------

**Memory management** abstractions provide a uniform API over memory that
may be allocated through various means, such as heap allocation, the memory
mapping of a file or a static memory area.  In particular, the **buffer**
abstraction represents a contiguous area of physical data.

The one-dimensional layer
-------------------------

**Data types** govern the *logical* interpretation of *physical* data.
Many operations in Arrow are parametered, at compile-time or at runtime,
by a data type.

**Arrays** assemble one or several buffers with a data type, allowing to
view them as a logical contiguous sequence of values (possibly nested).

**Chunked arrays** are a generalization of arrays, comprising several same-type
arrays into a longer logical sequence of values.

The two-dimensional layer
-------------------------

**Schemas** describe a logical collection of several pieces of data,
each with a distinct name and type, and optional metadata.

**Tables** are collections of chunked array in accordance to a schema. They
are the most capable dataset-providing abstraction in Arrow.

**Record batches** are collections of contiguous arrays, described
by a schema.  They allow incremental construction or serialization of tables.

The compute layer
-----------------

**Datums** are flexible dataset references, able to hold for example an array or table
reference.

**Kernels** are specialized computation functions running in a loop over a
given set of datums representing input and output parameters to the functions.

The IO layer
------------

**Streams** allow untyped sequential or seekable access over external data
of various kinds (for example compressed or memory-mapped).

The Inter-Process Communication (IPC) layer
-------------------------------------------

A **messaging format** allows interchange of Arrow data between processes, using
as few copies as possible.

The file formats layer
----------------------

Reading and writing Arrow data from/to various file formats is possible, for
example **Parquet**, **CSV**, **Orc** or the Arrow-specific **Feather** format.

The devices layer
-----------------

Basic **CUDA** integration is provided, allowing to describe Arrow data backed
by GPU-allocated memory.
