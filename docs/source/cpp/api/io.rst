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

==============
Input / output
==============

Interfaces
==========

.. doxygenclass:: arrow::io::FileInterface
   :members:

.. doxygenclass:: arrow::io::Readable
   :members:

.. doxygenclass:: arrow::io::Seekable
   :members:

.. doxygenclass:: arrow::io::Writable
   :members:

.. doxygenclass:: arrow::io::InputStream
   :members:

.. doxygenclass:: arrow::io::RandomAccessFile
   :members:

.. doxygenclass:: arrow::io::OutputStream
   :members:

.. doxygenclass:: arrow::io::ReadWriteFileInterface
   :members:

Concrete implementations
========================

In-memory streams
-----------------

.. doxygenclass:: arrow::io::BufferReader
   :members:

.. doxygenclass:: arrow::io::MockOutputStream
   :members:

.. doxygenclass:: arrow::io::BufferOutputStream
   :members:

.. doxygenclass:: arrow::io::FixedSizeBufferWriter
   :members:

Local files
-----------

.. doxygenclass:: arrow::io::ReadableFile
   :members:

.. doxygenclass:: arrow::io::FileOutputStream
   :members:

.. doxygenclass:: arrow::io::MemoryMappedFile
   :members:

Buffering input / output wrappers
---------------------------------

.. doxygenclass:: arrow::io::BufferedInputStream
   :members:

.. doxygenclass:: arrow::io::BufferedOutputStream
   :members:

Compressed input / output wrappers
----------------------------------

.. doxygenclass:: arrow::io::CompressedInputStream
   :members:

.. doxygenclass:: arrow::io::CompressedOutputStream
   :members:

Transforming input wrapper
--------------------------

.. doxygenclass:: arrow::io::TransformInputStream
   :members:
