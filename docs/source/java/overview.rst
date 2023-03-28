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

.. _`C Data Interface`: https://arrow.apache.org/docs/format/CDataInterface.html
