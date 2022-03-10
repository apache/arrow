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

=================
Memory Management
=================

The memory management package contains all the memory allocation related items that Arrow uses to manage memory.
This section will introduce you to the major concepts in Java’s memory management:

* Allocator
* Arrowbuf

.. contents::

Getting Started
===============

Java memory implementation is independently from C++ (not a wrapper around).
Java memory was implemented considering these specifications: Arrow Columnar Format and Java Off Heap references.

.. note::

    Java Memory Data = Data (Columnar mode reference) + Metadata (Flatbuffers serialization reference).

These are the java memory modules:

* Memory Core: Core off-heap memory management libraries for Arrow ValueVectors.
* Memory Netty: Netty allocator and utils for allocating memory in Arrow.
* Memory Unsafe: Allocator and utils for allocating memory in Arrow based on sun.misc.Unsafe.

Allocators
==========

Memory core module define the next allocators:

* Buffer Allocator: The public interface application users should be leveraging.
* Root Allocator: A root allocator for using direct memory. Typically only one created for a JVM.

Arrow provides a tree-based model for memory allocation. The RootAllocator is created first,
then all allocators are created as children of that allocator. The RootAllocator is responsible
for being the master bookkeeper for memory allocations.

Please consider this note on your development:

* Use BufferAllocator instead of RootAllocator in your allocator creation.
* Create your allocator inside of a try-with-resources statement.

.. code-block:: Java

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE) ) { ; }

ArrowBuf
========

The facade for interacting directly with a chunk of memory.

Two important instance variables of an ArrowBuf:

* Address: Starting virtual address in the underlying memory chunk that this ArrowBuf has access to.
* Length: Length (in bytes) in the underlying memory chunk that this ArrowBuf has access to.

Memory Modules
==============

Memory core define the bases to work with direct memory and the application decided to allocate arrow buffer bases on
the dependency added on your pom.xml (memory-unsafe or memory-netty). If any of these is not added the application raise
an exception.
