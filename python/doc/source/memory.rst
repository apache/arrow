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

Memory and IO Interfaces
========================

This section will introduce you to the major concepts in PyArrow's memory
management and IO systems:

* Buffers
* File-like and stream-like objects
* Memory pools

pyarrow.Buffer
--------------

The :class:`~pyarrow.Buffer` object wraps the C++ ``arrow::Buffer`` type and is
the primary tool for memory management in Apache Arrow in C++. It permits
higher-level array classes to safely interact with memory which they may or may
not own. ``arrow::Buffer`` can be zero-copy sliced to permit Buffers to cheaply
reference other Buffers, while preserving memory lifetime and clean
parent-child relationships.

There are many implementations of ``arrow::Buffer``, but they all provide a
standard interface: a data pointer and length. This is similar to Python's
built-in `buffer protocol` and ``memoryview`` objects.

A :class:`~pyarrow.Buffer` can be created from any Python object which
implements the buffer protocol. Let's consider a bytes object:

.. ipython:: python

   import pyarrow as pa

   data = b'abcdefghijklmnopqrstuvwxyz'
   buf = pa.py_buffer(data)
   buf
   buf.size

Creating a Buffer in this way does not allocate any memory; it is a zero-copy
view on the memory exported from the ``data`` bytes object.

The Buffer's ``to_pybytes`` method can convert to a Python byte string:

.. ipython:: python

   buf.to_pybytes()

Buffers can be used in circumstances where a Python buffer or memoryview is
required, and such conversions are also zero-copy:

.. ipython:: python

   memoryview(buf)

.. _io.native_file:

Native Files
------------

The Arrow C++ libraries have several abstract interfaces for different kinds of
IO objects:

* Read-only streams
* Read-only files supporting random access
* Write-only streams
* Write-only files supporting random access
* File supporting reads, writes, and random access

In the the interest of making these objects behave more like Python's built-in
``file`` objects, we have defined a :class:`~pyarrow.NativeFile` base class
which is intended to mimic Python files and able to be used in functions where
a Python file (such as ``file`` or ``BytesIO``) is expected.

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
* :class:`~pyarrow.HdfsFile`, for reading and writing data to the Hadoop Filesystem
* :class:`~pyarrow.PythonFile`, for interfacing with Python file objects in C++

We will discuss these in the following sections after explaining memory pools.

Memory Pools
------------

All memory allocations and deallocations (like ``malloc`` and ``free`` in C)
are tracked in an instance of ``arrow::MemoryPool``. This means that we can
then precisely track amount of memory that has been allocated:

.. ipython:: python

   pa.total_allocated_bytes()

PyArrow uses a default built-in memory pool, but in the future there may be
additional memory pools (and subpools) to choose from. Let's consider an
``BufferOutputStream``, which is like a ``BytesIO``:

.. ipython:: python

   stream = pa.BufferOutputStream()
   stream.write(b'foo')
   pa.total_allocated_bytes()
   for i in range(1024): stream.write(b'foo')
   pa.total_allocated_bytes()

The default allocator requests memory in a minimum increment of 64 bytes. If
the stream is garbaged-collected, all of the memory is freed:

.. ipython:: python

   stream = None
   pa.total_allocated_bytes()

On-Disk and Memory Mapped Files
-------------------------------

PyArrow includes two ways to interact with data on disk: standard operating
system-level file APIs, and memory-mapped files. In regular Python we can
write:

.. ipython:: python

   with open('example.dat', 'wb') as f:
       f.write(b'some example data')

Using pyarrow's :class:`~pyarrow.OSFile` class, you can write:

.. ipython:: python

   with pa.OSFile('example2.dat', 'wb') as f:
       f.write(b'some example data')

For reading files, you can use ``OSFile`` or
:class:`~pyarrow.MemoryMappedFile`. The difference between these is that
:class:`~pyarrow.OSFile` allocates new memory on each read, like Python file
objects. In reads from memory maps, the library constructs a buffer referencing
the mapped memory without any memory allocation or copying:

.. ipython:: python

   file_obj = pa.OSFile('example.dat')
   mmap = pa.memory_map('example.dat')
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

   buf = writer.get_result()
   buf
   buf.size
   reader = pa.BufferReader(buf)
   reader.seek(7)
   reader.read(7)

These have similar semantics to Python's built-in ``io.BytesIO``.
