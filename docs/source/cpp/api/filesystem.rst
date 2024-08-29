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

===========
Filesystems
===========

Interface
=========

.. doxygenenum:: arrow::fs::FileType

.. doxygenstruct:: arrow::fs::FileInfo
   :members:

.. doxygenstruct:: arrow::fs::FileSelector
   :members:

.. doxygenclass:: arrow::fs::FileSystem
   :members:

High-level factory function
===========================

.. doxygengroup:: filesystem-factories
   :content-only:

Concrete implementations
========================

"Subtree" filesystem wrapper
----------------------------

.. doxygenclass:: arrow::fs::SubTreeFileSystem
   :members:

Local filesystem
----------------

.. doxygenstruct:: arrow::fs::LocalFileSystemOptions
   :members:

.. doxygenclass:: arrow::fs::LocalFileSystem
   :members:

S3 filesystem
-------------

.. doxygenstruct:: arrow::fs::S3Options
   :members:

.. doxygenclass:: arrow::fs::S3FileSystem
   :members:

.. doxygenfunction:: arrow::fs::InitializeS3(const S3GlobalOptions& options)

Hadoop filesystem
-----------------

.. doxygenstruct:: arrow::fs::HdfsOptions
   :members:

.. doxygenclass:: arrow::fs::HadoopFileSystem
   :members:

Google Cloud Storage filesystem
-------------------------------

.. doxygenstruct:: arrow::fs::GcsOptions
   :members:

.. doxygenclass:: arrow::fs::GcsFileSystem
   :members:
