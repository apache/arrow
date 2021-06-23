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

=========
Arrow IPC
=========

IPC options
===========

.. doxygenstruct:: arrow::ipc::IpcReadOptions
   :members:

.. doxygenstruct:: arrow::ipc::IpcWriteOptions
   :members:

Reading IPC streams and files
=============================

Blocking API
------------

Use either of these two classes, depending on which IPC format you want
to read.  The file format requires a random-access file, while the stream
format only requires a sequential input stream.

.. doxygenclass:: arrow::ipc::RecordBatchStreamReader
   :members:

.. doxygenclass:: arrow::ipc::RecordBatchFileReader
   :members:

Event-driven API
----------------

To read an IPC stream in event-driven fashion, you must implement a
:class:`~arrow::ipc::Listener` subclass that you will pass to
:class:`~arrow::ipc::StreamDecoder`.

.. doxygenclass:: arrow::ipc::Listener
   :members:

.. doxygenclass:: arrow::ipc::StreamDecoder
   :members:

Statistics
----------

.. doxygenstruct:: arrow::ipc::ReadStats
   :members:

Writing IPC streams and files
=============================

Blocking API
------------

The IPC stream format is only optionally terminated, whereas the IPC file format
must include a terminating footer. Thus a writer of the IPC file format must be
explicitly finalized with :func:`~arrow::ipc::RecordBatchWriter::Close()` or the resulting
file will be corrupt.

.. doxygengroup:: record-batch-writer-factories
   :content-only:

.. doxygenclass:: arrow::ipc::RecordBatchWriter
   :members:

Statistics
----------

.. doxygenstruct:: arrow::ipc::WriteStats
   :members:
