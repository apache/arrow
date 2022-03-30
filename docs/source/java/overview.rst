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

.. default-domain:: java
.. highlight:: java

===================
High-Level Overview
===================

.. contents::

The Apache Arrow Java modules implement various specifications including the
columnar format and IPC. Most modules are native Java implementations,
but some modules are JNI bindings to the C++ library.

.. list-table:: Arrow Java Modules
   :widths: 25 50 25
   :header-rows: 1

   * - Module
     - Description
     - Implementation
   * - arrow-format
     - Generated Java files from the IPC Flatbuffer definitions.
     - Native
   * - arrow-memory-core
     - Core off-heap memory management libraries for Arrow ValueVectors.
     - Native
   * - arrow-memory-unsafe
     - Memory management implementation based on sun.misc.Unsafe.
     - Native
   * - arrow-memory-netty
     - Memory management implementation based on Netty.
     - Native
   * - arrow-vector
     - An off-heap reference implementation for Arrow columnar data format.
     - Native
   * - arrow-tools
     - Java applications for working with Arrow ValueVectors.
     - Native
   * - arrow-jdbc
     - (Experimental) A library for converting JDBC data to Arrow data.
     - Native
   * - arrow-plasma
     - (Experimental) Java client for the Plasma object store.
     - Native
   * - flight-core
     - (Experimental) An RPC mechanism for transferring ValueVectors.
     - Native
   * - flight-grpc
     - (Experimental) Contains utility class to expose Flight gRPC service and client.
     - Native
   * - flight-sql
     - (Experimental) Contains utility classes to expose Flight SQL semantics for clients and servers over Arrow Flight.
     - Native
   * - flight-integration-tests
     - Integration tests for Flight RPC.
     - Native
   * - arrow-performance
     - JMH benchmarks for the Arrow libraries.
     - Native
   * - arrow-algorithm
     - (Experimental) A collection of algorithms for working with ValueVectors.
     - Native
   * - arrow-avro
     - (Experimental) A library for converting Avro data to Arrow data.
     - Native
   * - arrow-compression
     - (Experimental) A library for working with compression/decompression of Arrow data.
     - Native
   * - arrow-c-data
     - Java implementation of `C Data Interface`_
     - JNI
   * - arrow-orc
     - (Experimental) A JNI wrapper for the C++ ORC reader implementation.
     - JNI
   * - arrow-gandiva
     - Java wrappers around the native Gandiva SQL expression compiler.
     - JNI
   * - arrow-dataset
     - Java bindings to the Arrow Datasets library.
     - JNI

Arrow Java modules support working with data (1) in-memory, (2) at rest, and (3) on-the-wire.

For more detail about how to install this modules please review `Installing Java Modules`.

Arrow Java In-Memory (The Physical Layer)
-----------------------------------------

Off-Heap Memory
***************

Interact with the memory in Java is a little bit complex, but interact
with direct-memory is a major challenge.

Java memory modules is based on `off-heap-memory` and offer helpers to
interact with direct memory in an easy way thru
their interfaces (i.e.: BufferAllocator).

For more detail on Arrow Java memory please review
:doc:`Memory Management <memory>`.

Columnar Format
***************

There is a detailed explanation about columnar format specification
at :doc:`Columnar Format <../format/Columnar>`.

This columnar data format specification is implemented by Arrow Java
vector module.

The One-Dimensional Layer
*************************

``Data Types``: Govern the `logical` interpretation of `physical` data.
Arrow Java vector module use freemarker `template`_ to generate data types
defined on columnar format specification. On this resource you could see
implementation status of different data types `supported`_ by Java vector module.

Each logical data type has a well-defined physical layout. Here are
the `different physical layouts defined by Arrow`_.

``Value Vector``: Also known as "arrays" in the  columnar format specification.
Value vector represent a one-dimensional sequence of homogeneous values.

For more detail on Arrow Java vector please review :doc:`Value Vector <vector>`.

More examples available at `java cookbook create objects`_.

The Two-Dimensional Layer
*************************

Let's start talk about tabular data. Data often comes in the form of two-dimensional
sets of heterogeneous data (such as database tables, CSV files...). Arrow provides
several abstractions to handle such data conveniently and efficiently.

``Fields``: Fields are used to denote the particular columns of tabular data.

``Schema``: It holds a sequence of fields together with some optional metadata.

``VectorSchemaRoot``: It combines ValueVectors with a Schema to represent tabular data.
It is somewhat analogous to tables and record batches in the other Arrow implementations.

For more detail on Arrow Java vector please review :doc:`VectorSchemaRoot <vector_schema_root>`.

More examples available at `java cookbook working with schema`_.

Arrow Java At-Rest
------------------

The Arrow iInterprocess communication (IPC) format defines two types of binary formats
for serializing Arrow data:

``Streaming format``: For sending an arbitrary number of record batches. The format must
be processed from start to end, and does not support random access

``File or Random Access format``: For serializing a fixed number of record batches. It
supports random access.

For more detail on Arrow Java io please review :doc:`Reading/Writing IPC formats <ipc>`.

More examples available at `java cookbook reading and writing data`_.

Arrow Java On-the-wire
----------------------

Arrow offer high performance data transport protocol through Java ``flight`` module.
Arrow Java flight is built using gRPC, protocol buffer and Arrow columnar format,
it provides a framework for sending and receiving Arrow data natively.

For more detail on Arrow Java on the wire please review :doc:`Arrow Flight RPC <../format/Flight>`
and :doc:`Arrow Flight SQL <../format/FlightSql>`.

More examples available at `java cookbook arrow flight`_.

To complete this initial overview about Arrow Java, consider this as the variety of Arrow Java documentation:

* Specification and protocols: This contains agnostic specification that is implemented in this case by Arrow Java modules.
* Supported environment (like this): This contains answers for what-is-that Arrow Java module.
* Cookbook: This contains answers about how-to-use Arrow Java modules with practices examples.
* Development: This contains detailed information about what you need to consider to start with Arrow Java development.

.. _`C Data Interface`: https://arrow.apache.org/docs/format/CDataInterface.html
.. _`template`: https://github.com/apache/arrow/tree/master/java/vector/src/main/codegen/templates
.. _`supported`: https://arrow.apache.org/docs/status.html#data-types
.. _`different physical layouts defined by Arrow`: https://arrow.apache.org/docs/format/Columnar.html#physical-memory-layout
.. _`java cookbook create objects`: https://arrow.apache.org/cookbook/java/create.html
.. _`java cookbook working with schema`: https://arrow.apache.org/cookbook/java/schema.html
.. _`java cookbook reading and writing data`: https://arrow.apache.org/cookbook/java/io.html
.. _`java cookbook arrow flight`: https://arrow.apache.org/cookbook/java/flight.html