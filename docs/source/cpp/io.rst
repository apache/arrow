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
.. cpp:namespace:: arrow::io

==============================
Input / output and filesystems
==============================

Arrow provides a range of C++ interfaces abstracting the concrete details
of input / output operations.  They operate on streams of untyped binary data.
Those abstractions are used for various purposes such as reading CSV or
Parquet data, transmitting IPC streams, and more.

.. seealso::
   :doc:`API reference for input/output facilities <api/io>`.

Reading binary data
===================

Interfaces for reading binary data come in two flavours:

* Sequential reading: the :class:`InputStream` interface provides
  ``Read`` methods; it is recommended to ``Read`` to a ``Buffer`` as it
  may in some cases avoid a memory copy.

* Random access reading: the :class:`RandomAccessFile` interface
  provides additional facilities for positioning and, most importantly,
  the ``ReadAt`` methods which allow parallel reading from multiple threads.

Concrete implementations are available for :class:`in-memory reads <BufferReader>`,
:class:`unbuffered file reads <ReadableFile>`,
:class:`memory-mapped file reads <MemoryMappedFile>`,
:class:`buffered reads <BufferedInputStream>`,
:class:`compressed reads <CompressedInputStream>`.

Writing binary data
===================

Writing binary data is mostly done through the :class:`OutputStream`
interface.

Concrete implementations are available for :class:`in-memory writes <BufferOutputStream>`,
:class:`unbuffered file writes <FileOutputStream>`,
:class:`memory-mapped file writes <MemoryMappedFile>`,
:class:`buffered writes <BufferedOutputStream>`,
:class:`compressed writes <CompressedOutputStream>`.

.. cpp:namespace:: arrow::fs

Filesystems
===========

The :class:`filesystem interface <FileSystem>` allows abstracted access over
various data storage backends such as the local filesystem or a S3 bucket.
It provides input and output streams as well as directory operations.

The filesystem interface exposes a simplified view of the underlying data
storage.  Data paths are represented as *abstract paths*, which are
``/``-separated, even on Windows, and shouldn't include special path
components such as ``.`` and ``..``.  Symbolic links, if supported by the
underlying storage, are automatically dereferenced.  Only basic
:class:`metadata <FileStats>` about file entries, such as the file size
and modification time, is made available.

Concrete implementations are available for
:class:`local filesystem access <LocalFileSystem>`,
:class:`HDFS <HadoopFileSystem>` and
:class:`Amazon S3-compatible storage <S3FileSystem>`.
