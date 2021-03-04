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
.. cpp:namespace:: arrow::ipc

========================================
Reading and writing the Arrow IPC format
========================================

.. seealso::
   :ref:`Arrow IPC format specification <format-ipc>`.

   :doc:`API reference for IPC readers and writers <api/ipc>`.

Arrow C++ provides readers and writers for the Arrow IPC format which wrap
lower level input/output, handled through the :doc:`IO interfaces <io>`.
For reading, there is also an event-driven API that enables feeding
arbitrary data into the IPC decoding layer asynchronously.

Reading IPC streams and files
=============================

Synchronous reading
-------------------

For most cases, it is most convenient to use the :class:`RecordBatchStreamReader`
or :class:`RecordBatchFileReader` class, depending on which variant of the IPC
format you want to read.  The former requires a :class:`~arrow::io::InputStream`
source, while the latter requires a :class:`~arrow::io::RandomAccessFile`.

Reading Arrow IPC data is inherently zero-copy if the source allows it.
For example, a :class:`~arrow::io::BufferReader` or :class:`~arrow::io::MemoryMappedFile`
can typically be zero-copy.  Exceptions are when the data must be transformed
on the fly, e.g. when buffer compression has been enabled on the IPC stream
or file.

Event-driven reading
--------------------

When it is necessary to process the IPC format without blocking (for example
to integrate Arrow with an event loop), or if data is coming from an unusual
source, use the event-driven :class:`StreamDecoder`.  You will need to define
a subclass of :class:`Listener` and implement the virtual methods for the
desired events (for example, implement :func:`Listener::OnRecordBatchDecoded`
to be notified of each incoming :class:`RecordBatch`).

Writing IPC streams and files
=============================

Use one of the factory functions, :func:`MakeStreamWriter` or
:func:`MakeFileWriter`, to obtain a :class:`RecordBatchWriter` instance for
the given IPC format variant.

Configuring
===========

Various aspects of reading and writing the IPC format can be configured
using the :class:`IpcReadOptions` and :class:`IpcWriteOptions` classes,
respectively.
