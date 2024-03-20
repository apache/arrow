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

.. _cpp-filesystems:

Filesystems
===========

The :class:`filesystem interface <FileSystem>` allows abstracted access over
various data storage backends such as the local filesystem or a S3 bucket.
It provides input and output streams as well as directory operations.

.. seealso::
    :ref:`Filesystems API reference <cpp-api-filesystems>`.

The filesystem interface exposes a simplified view of the underlying data
storage.  Data paths are represented as *abstract paths*, which are
``/``-separated, even on Windows, and shouldn't include special path
components such as ``.`` and ``..``.  Symbolic links, if supported by the
underlying storage, are automatically dereferenced.  Only basic
:class:`metadata <FileStats>` about file entries, such as the file size
and modification time, is made available.

Filesystem instances can be constructed from URI strings using one of the
:ref:`FromUri factories <filesystem-factory-functions>`, which dispatch to
implementation-specific factories based on the URI's ``scheme``. Other properties
for the new instance are extracted from the URI's other properties such as the
``hostname``, ``username``, etc. Arrow supports runtime registration of new
filesystems, and provides built-in support for several filesystems.

Which built-in filesystems are supported is configured at build time and may include
:class:`local filesystem access <LocalFileSystem>`,
:class:`HDFS <HadoopFileSystem>`,
:class:`Amazon S3-compatible storage <S3FileSystem>` and
:class:`Google Cloud Storage <GcsFileSystem>`.

.. note::

  Tasks that use filesystems will typically run on the
  :ref:`I/O thread pool<io_thread_pool>`.  For filesystems that support high levels
  of concurrency you may get a benefit from increasing the size of the I/O thread pool.

Defining new filesystems
========================

Support for additional URI schemes can be added to the
:ref:`FromUri factories <filesystem-factory-functions>`
by registering a factory for each new URI scheme with
:func:`~arrow::fs::RegisterFileSystemFactory`. To enable the common case
wherein it is preferred that registration be automatic, an instance of
:class:`~arrow::fs::FileSystemRegistrar` can be defined at namespace
scope, which will register a factory whenever the instance is loaded:

.. code-block:: cpp

    arrow::fs::FileSystemRegistrar kExampleFileSystemModule{
      "example",
      [](const Uri& uri, const io::IOContext& io_context,
          std::string* out_path) -> Result<std::shared_ptr<arrow::fs::FileSystem>> {
        EnsureExampleFileSystemInitialized();
        return std::make_shared<ExampleFileSystem>();
      },
      &EnsureExampleFileSystemFinalized,
    };

If a filesystem implementation requires initialization before any instances
may be constructed, this should be included in the corresponding factory or
otherwise automatically ensured before the factory is invoked. Likewise if
a filesystem implementation requires tear down before the process ends, this
can be wrapped in a function and registered alongside the factory. All
finalizers will be called by :func:`~arrow::fs::EnsureFinalized`.

Build complexity can be decreased by compartmentalizing a filesystem
implementation into a separate shared library, which applications may
link or load dynamically. Arrow's built-in filesystem implementations
also follow this pattern. If a shared library containing instances of
:class:`~arrow::fs::FileSystemRegistrar` must be dynamically loaded,
:func:`~arrow::fs::LoadFileSystemFactories` should be used to load it.
If such a library might link statically to arrow, it
should have exactly one of its sources
``#include "arrow/filesystem/filesystem_library.h"``
in order to ensure the presence of the symbol on which
:func:`~arrow::fs::LoadFileSystemFactories` depends.

