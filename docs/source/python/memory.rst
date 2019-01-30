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
.. _io:

========================
Memory and IO Interfaces
========================

This section will introduce you to the major concepts in PyArrow's memory
management and IO systems:

* Buffers
* Memory pools
* File-like and stream-like objects

Referencing and Allocating Memory
=================================

pyarrow.Buffer
--------------

The :class:`Buffer` object wraps the C++ :cpp:class:`arrow::Buffer` type
which is the primary tool for memory management in Apache Arrow in C++. It permits
higher-level array classes to safely interact with memory which they may or may
not own. ``arrow::Buffer`` can be zero-copy sliced to permit Buffers to cheaply
reference other Buffers, while preserving memory lifetime and clean
parent-child relationships.

There are many implementations of ``arrow::Buffer``, but they all provide a
standard interface: a data pointer and length. This is similar to Python's
built-in `buffer protocol` and ``memoryview`` objects.

A :class:`Buffer` can be created from any Python object implementing
the buffer protocol by calling the :func:`py_buffer` function. Let's consider
a bytes object:

.. ipython:: python

   import pyarrow as pa

   data = b'abcdefghijklmnopqrstuvwxyz'
   buf = pa.py_buffer(data)
   buf
   buf.size

Creating a Buffer in this way does not allocate any memory; it is a zero-copy
view on the memory exported from the ``data`` bytes object.

External memory, under the form of a raw pointer and size, can also be
referenced using the :func:`foreign_buffer` function.

Buffers can be used in circumstances where a Python buffer or memoryview is
required, and such conversions are zero-copy:

.. ipython:: python

   memoryview(buf)

The Buffer's :meth:`~Buffer.to_pybytes` method converts the Buffer's data to a
Python bytestring (thus making a copy of the data):

.. ipython:: python

   buf.to_pybytes()

Memory Pools
------------

All memory allocations and deallocations (like ``malloc`` and ``free`` in C)
are tracked in an instance of ``arrow::MemoryPool``. This means that we can
then precisely track amount of memory that has been allocated:

.. ipython:: python

   pa.total_allocated_bytes()

PyArrow uses a default built-in memory pool, but in the future there may be
additional memory pools (and subpools) to choose from. Let's allocate
a resizable ``Buffer`` from the default pool:

.. ipython:: python

   buf = pa.allocate_buffer(1024, resizable=True)
   pa.total_allocated_bytes()
   buf.resize(2048)
   pa.total_allocated_bytes()

The default allocator requests memory in a minimum increment of 64 bytes. If
the buffer is garbaged-collected, all of the memory is freed:

.. ipython:: python

   buf = None
   pa.total_allocated_bytes()

.. seealso::
   On-GPU buffers using Arrow's optional :doc:`CUDA integration <cuda>`.


Input and Output
================

.. _io.native_file:

The Arrow C++ libraries have several abstract interfaces for different kinds of
IO objects:

* Read-only streams
* Read-only files supporting random access
* Write-only streams
* Write-only files supporting random access
* File supporting reads, writes, and random access

In the interest of making these objects behave more like Python's built-in
``file`` objects, we have defined a :class:`~pyarrow.NativeFile` base class
which implements the same API as regular Python file objects.

:class:`~pyarrow.NativeFile` has some important features which make it
preferable to using Python files with PyArrow where possible:

* Other Arrow classes can access the internal C++ IO objects natively, and do
  not need to acquire the Python GIL
* Native C++ IO may be able to do zero-copy IO, such as with memory maps

There are several kinds of :class:`~pyarrow.NativeFile` options available:

* :class:`~pyarrow.OSFile`, a native file that uses your operating system's
  file descriptors
* :class:`~pyarrow.MemoryMappedFile`, for reading (zero-copy) and writing with
  memory maps
* :class:`~pyarrow.BufferReader`, for reading :class:`~pyarrow.Buffer` objects
  as a file
* :class:`~pyarrow.BufferOutputStream`, for writing data in-memory, producing a
  Buffer at the end
* :class:`~pyarrow.FixedSizeBufferWriter`, for writing data into an already
  allocated Buffer
* :class:`~pyarrow.HdfsFile`, for reading and writing data to the Hadoop Filesystem
* :class:`~pyarrow.PythonFile`, for interfacing with Python file objects in C++
* :class:`~pyarrow.CompressedInputStream` and
  :class:`~pyarrow.CompressedOutputStream`, for on-the-fly compression or
  decompression to/from another stream

There are also high-level APIs to make instantiating common kinds of streams
easier.

High-Level API
--------------

Input Streams
~~~~~~~~~~~~~

The :func:`~pyarrow.input_stream` function allows creating a readable
:class:`~pyarrow.NativeFile` from various kinds of sources.

* If passed a :class:`~pyarrow.Buffer` or a ``memoryview`` object, a
  :class:`~pyarrow.BufferReader` will be returned:

   .. ipython:: python

      buf = memoryview(b"some data")
      stream = pa.input_stream(buf)
      stream.read(4)

* If passed a string or file path, it will open the given file on disk
  for reading, creating a :class:`~pyarrow.OSFile`.  Optionally, the file
  can be compressed: if its filename ends with a recognized extension
  such as ``.gz``, its contents will automatically be decompressed on
  reading.

  .. ipython:: python

     import gzip
     with gzip.open('example.gz', 'wb') as f:
         f.write(b'some data\n' * 3)

     stream = pa.input_stream('example.gz')
     stream.read()

* If passed a Python file object, it will wrapped in a :class:`PythonFile`
  such that the Arrow C++ libraries can read data from it (at the expense
  of a slight overhead).

Output Streams
~~~~~~~~~~~~~~

:func:`~pyarrow.output_stream` is the equivalent function for output streams
and allows creating a writable :class:`~pyarrow.NativeFile`.  It has the same
features as explained above for :func:`~pyarrow.input_stream`, such as being
able to write to buffers or do on-the-fly compression.

.. ipython:: python

   with pa.output_stream('example1.dat') as stream:
       stream.write(b'some data')

   f = open('example1.dat', 'rb')
   f.read()


On-Disk and Memory Mapped Files
-------------------------------

PyArrow includes two ways to interact with data on disk: standard operating
system-level file APIs, and memory-mapped files. In regular Python we can
write:

.. ipython:: python

   with open('example2.dat', 'wb') as f:
       f.write(b'some example data')

Using pyarrow's :class:`~pyarrow.OSFile` class, you can write:

.. ipython:: python

   with pa.OSFile('example3.dat', 'wb') as f:
       f.write(b'some example data')

For reading files, you can use :class:`~pyarrow.OSFile` or
:class:`~pyarrow.MemoryMappedFile`. The difference between these is that
:class:`~pyarrow.OSFile` allocates new memory on each read, like Python file
objects. In reads from memory maps, the library constructs a buffer referencing
the mapped memory without any memory allocation or copying:

.. ipython:: python

   file_obj = pa.OSFile('example2.dat')
   mmap = pa.memory_map('example3.dat')
   file_obj.read(4)
   mmap.read(4)

The ``read`` method implements the standard Python file ``read`` API. To read
into Arrow Buffer objects, use ``read_buffer``:

.. ipython:: python

   mmap.seek(0)
   buf = mmap.read_buffer(4)
   print(buf)
   buf.to_pybytes()

Many tools in PyArrow, particular the Apache Parquet interface and the file and
stream messaging tools, are more efficient when used with these ``NativeFile``
types than with normal Python file objects.

.. ipython:: python
   :suppress:

   buf = mmap = file_obj = None
   !rm example.dat
   !rm example2.dat

In-Memory Reading and Writing
-----------------------------

To assist with serialization and deserialization of in-memory data, we have
file interfaces that can read and write to Arrow Buffers.

.. ipython:: python

   writer = pa.BufferOutputStream()
   writer.write(b'hello, friends')

   buf = writer.getvalue()
   buf
   buf.size
   reader = pa.BufferReader(buf)
   reader.seek(7)
   reader.read(7)

These have similar semantics to Python's built-in ``io.BytesIO``.
