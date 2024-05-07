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

***************************
Arrow Columnar Format INTRO
***************************

Apache Arrow was born with the idea to define a set of standards for
data representation and interchange between languages and systems to
avoid costs of data serialization/deserialization and in order to
avoid reinventing the wheel on each of those systems and languages.

The initial problem:
Each system / language requires their own format definitions, implementation
of common algorithms, etcetera. In our heterogeneous environments we
often have to move data from one system/language to accommodate our
workflows that meant copy&convert the data between them, which is
quite costly.

Apart from the initial vision, Arrow has grown to also develop a
multi-language collection of libraries for solving systems problems
related to in-memory analytical data processing. This includes such
topics as:

* Zero-copy shared memory and RPC-based data movement
* Reading and writing file formats (like CSV, Apache ORC, and Apache Parquet)
* In-memory analytics and query processing

NanoArrow
=========

The Arrow libraries are growing with a lot of functionality and
`nanoarrow <https://github.com/apache/arrow-nanoarrow>`_ was born to
solve the problem where linking to the Arrow implementation is
difficult or impossible.

The NanoArrow library is a set of helper functions to interpret and
generate Arrow C Data Interface and Arrow C Stream Interface structures.
The library is in active development.

The NanoArrow Python bindings are intended to support clients that wish
to produce or interpret Arrow C Data and/or Arrow C Stream structures
in Python, without a dependency on the larger PyArrow package.

Arrow Columnar Format
=====================

Row format
----------

Traditionally, in order to read the following data into memory you
would have some kind of structure representing the following rows:

.. TODO picture

That means that you have all the information for every row together
in memory.

Columnar format
---------------

If we have a much bigger table and we just want, for example the
average cost of transaction skipping all the data that is irrelevant
to do that computation would be costly. That's why storage and memory
representation for Columnar format is important.

.. TODO picture


A columnar format keeps the data organised by column instead of by row.
Analytical operations like filtering, grouping, aggregations and others
are much more efficient. CPU can maintain memory locality and require
less memory jumps to process the data. By keeping the data contiguous
in memory it also enables vectorization of the computations. Most modern
CPUs have single instructions, multiple data (SIMD) enabling parallel
processing and execution of instructions on vector data in single CPU instructions.

Compression is another element where columnar format representation can
take high advantage. Data similarity allows for better compression
techniques and algorithms. Having the same data types locality close
allows us to have better compression ratios.

Primitive layouts
=================

Fixed Size Primitive Layout
---------------------------

Support for null values
-----------------------

Variable length binary and string
---------------------------------

Nested layouts
==============

List
----

Struct
------

Map
---

Union
-----

Dictionary Encoded Layout
=========================

Run-End Encoded Layout
======================

All types overview
==================

Extension Types
===============

Overview of Arrow terminology
=============================

The Arrow C Data Interface
==========================

Arrow PyCapsule Interface
=========================
