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

.. currentmodule:: pyarrow

Streams and File Access
=======================

.. _api.io:

Factory Functions
-----------------

These factory functions are the recommended way to create a Arrow stream.
They accept various kinds of sources, such as in-memory buffers or on-disk files.

.. autosummary::
   :toctree: ../generated/

   input_stream
   output_stream
   memory_map
   create_memory_map

Stream Classes
--------------

.. autosummary::
   :toctree: ../generated/

   NativeFile
   OSFile
   PythonFile
   BufferReader
   BufferOutputStream
   FixedSizeBufferWriter
   MemoryMappedFile
   CompressedInputStream
   CompressedOutputStream

File Systems
------------

.. autosummary::
   :toctree: ../generated/

   hdfs.connect
   LocalFileSystem

.. class:: HadoopFileSystem
   :noindex:
