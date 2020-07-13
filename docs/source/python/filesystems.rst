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

.. _filesystem:

.. currentmodule:: pyarrow.fs

Filesystem Interface
====================

PyArrow comes with an abstract filesystem interface, as well as concrete
implementations for various storage types.

The filesystem interface provides input and output streams as well as
directory operations.  A simplified view of the underlying data
storage is exposed.  Data paths are represented as *abstract paths*, which
are ``/``-separated, even on Windows, and shouldn't include special path
components such as ``.`` and ``..``.  Symbolic links, if supported by the
underlying storage, are automatically dereferenced.  Only basic
:class:`metadata <FileStats>` about file entries, such as the file size
and modification time, is made available.

Types
-----

The core interface is represented by the base class :class:`FileSystem`.
Concrete subclasses are available for various kinds of storage:
:class:`local filesystem access <LocalFileSystem>`,
:class:`HDFS <HadoopFileSystem>` and
:class:`Amazon S3-compatible storage <S3FileSystem>`.

Example
-------

Assuming your S3 credentials are correctly configured (for example by setting
the ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY`` environment variables),
here is how you can read contents from a S3 bucket::

   >>> from pyarrow import fs
   >>> s3 = fs.S3FileSystem(region='eu-west-3')

   # List all contents in a bucket, recursively
   >>> s3.get_target_stats(fs.FileSelector('my-test-bucket', recursive=True))
   [<FileStats for 'my-test-bucket/File1': type=FileType.File, size=10>,
    <FileStats for 'my-test-bucket/File5': type=FileType.File, size=10>,
    <FileStats for 'my-test-bucket/Dir1': type=FileType.Directory>,
    <FileStats for 'my-test-bucket/Dir2': type=FileType.Directory>,
    <FileStats for 'my-test-bucket/EmptyDir': type=FileType.Directory>,
    <FileStats for 'my-test-bucket/Dir1/File2': type=FileType.File, size=11>,
    <FileStats for 'my-test-bucket/Dir1/Subdir': type=FileType.Directory>,
    <FileStats for 'my-test-bucket/Dir2/Subdir': type=FileType.Directory>,
    <FileStats for 'my-test-bucket/Dir2/Subdir/File3': type=FileType.File, size=10>]

   # Open a file for reading and download its contents
   >>> f = s3.open_input_stream('my-test-bucket/Dir1/File2')
   >>> f.readall()
   b'some data'
