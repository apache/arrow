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

.. _ipc:

IPC: Fast Streaming and Serialization
=====================================

Arrow defines two types of binary formats for serializing record batches:

* **Streaming format**: for sending an arbitrary length sequence of record
  batches. The format must be processed from start to end, and does not support
  random access

* **File or Random Access format**: for serializing a fixed number of record
  batches. Supports random access, and thus is very useful when used with
  memory maps

To follow this section, make sure to first read the section on :ref:`Memory and
IO <io>`.

Writing and Reading Streams
---------------------------

First, let's create a small record batch:

.. ipython:: python

   import pyarrow as pa

   data = [
       pa.array([1, 2, 3, 4]),
       pa.array(['foo', 'bar', 'baz', None]),
       pa.array([True, None, False, True])
   ]

   batch = pa.RecordBatch.from_arrays(data, ['f0', 'f1', 'f2'])
   batch.num_rows
   batch.num_columns

Now, we can begin writing a stream containing some number of these batches. For
this we use :class:`~pyarrow.StreamWriter`, which can write to a writeable
``NativeFile`` object or a writeable Python object:

.. ipython:: python

   sink = pa.InMemoryOutputStream()
   writer = pa.StreamWriter(sink, batch.schema)

Here we used an in-memory Arrow buffer stream, but this could have been a
socket or some other IO sink.

When creating the ``StreamWriter``, we pass the schema, since the schema
(column names and types) must be the same for all of the batches sent in this
particular stream. Now we can do:

.. ipython:: python

   for i in range(5):
      writer.write_batch(batch)
   writer.close()

   buf = sink.get_result()
   buf.size

Now ``buf`` contains the complete stream as an in-memory byte buffer. We can
read such a stream with :class:`~pyarrow.StreamReader`:

.. ipython:: python

   reader = pa.StreamReader(buf)
   reader.schema

   batches = [b for b in reader]
   len(batches)

We can check the returned batches are the same as the original input:

.. ipython:: python

   batches[0].equals(batch)

An important point is that if the input source supports zero-copy reads
(e.g. like a memory map, or ``pyarrow.BufferReader``), then the returned
batches are also zero-copy and do not allocate any new memory on read.

Writing and Reading Random Access Files
---------------------------------------

The :class:`~pyarrow.FileWriter` has the same API as
:class:`~pyarrow.StreamWriter`:

.. ipython:: python

   sink = pa.InMemoryOutputStream()
   writer = pa.FileWriter(sink, batch.schema)

   for i in range(10):
      writer.write_batch(batch)
   writer.close()

   buf = sink.get_result()
   buf.size

The difference between :class:`~pyarrow.FileReader` and
:class:`~pyarrow.StreamReader` is that the input source must have a ``seek``
method for random access. The stream reader only requires read operations:

.. ipython:: python

   reader = pa.FileReader(buf)

Because we have access to the entire payload, we know the number of record
batches in the file, and can read any at random:

.. ipython:: python

   reader.num_record_batches
   b = reader.get_batch(3)
   b.equals(batch)
