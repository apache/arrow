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

.. ipython:: python
    :suppress:

    # set custom tmp working directory for files that create data
    import os
    import tempfile

    orig_working_dir = os.getcwd()
    temp_working_dir = tempfile.mkdtemp(prefix="pyarrow-")
    os.chdir(temp_working_dir)

.. currentmodule:: pyarrow

.. _ipc:

Streaming, Serialization, and IPC
=================================

Writing and Reading Streams
---------------------------

Arrow defines two types of binary formats for serializing record batches:

* **Streaming format**: for sending an arbitrary length sequence of record
  batches. The format must be processed from start to end, and does not support
  random access

* **File or Random Access format**: for serializing a fixed number of record
  batches. Supports random access, and thus is very useful when used with
  memory maps

To follow this section, make sure to first read the section on :ref:`Memory and
IO <io>`.

Using streams
~~~~~~~~~~~~~

First, let's create a small record batch:

.. ipython:: python

   import pyarrow as pa

   data = [
       pa.array([1, 2, 3, 4]),
       pa.array(['foo', 'bar', 'baz', None]),
       pa.array([True, None, False, True])
   ]

   batch = pa.record_batch(data, names=['f0', 'f1', 'f2'])
   batch.num_rows
   batch.num_columns

Now, we can begin writing a stream containing some number of these batches. For
this we use :class:`~pyarrow.RecordBatchStreamWriter`, which can write to a
writeable ``NativeFile`` object or a writeable Python object. For convenience,
this one can be created with :func:`~pyarrow.ipc.new_stream`:

.. ipython:: python

   sink = pa.BufferOutputStream()
   
   with pa.ipc.new_stream(sink, batch.schema) as writer:
      for i in range(5):
         writer.write_batch(batch)

Here we used an in-memory Arrow buffer stream (``sink``), 
but this could have been a socket or some other IO sink.

When creating the ``StreamWriter``, we pass the schema, since the schema
(column names and types) must be the same for all of the batches sent in this
particular stream. Now we can do:

.. ipython:: python

   buf = sink.getvalue()
   buf.size

Now ``buf`` contains the complete stream as an in-memory byte buffer. We can
read such a stream with :class:`~pyarrow.RecordBatchStreamReader` or the
convenience function ``pyarrow.ipc.open_stream``:

.. ipython:: python

   with pa.ipc.open_stream(buf) as reader:
         schema = reader.schema
         batches = [b for b in reader]
   
   schema
   len(batches)

We can check the returned batches are the same as the original input:

.. ipython:: python

   batches[0].equals(batch)

An important point is that if the input source supports zero-copy reads
(e.g. like a memory map, or ``pyarrow.BufferReader``), then the returned
batches are also zero-copy and do not allocate any new memory on read.

Writing and Reading Random Access Files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :class:`~pyarrow.RecordBatchFileWriter` has the same API as
:class:`~pyarrow.RecordBatchStreamWriter`. You can create one with
:func:`~pyarrow.ipc.new_file`:

.. ipython:: python

   sink = pa.BufferOutputStream()
   
   with pa.ipc.new_file(sink, batch.schema) as writer:
      for i in range(10):
         writer.write_batch(batch)

   buf = sink.getvalue()
   buf.size

The difference between :class:`~pyarrow.RecordBatchFileReader` and
:class:`~pyarrow.RecordBatchStreamReader` is that the input source must have a
``seek`` method for random access. The stream reader only requires read
operations. We can also use the :func:`~pyarrow.ipc.open_file` method to open a file:

.. ipython:: python

   with pa.ipc.open_file(buf) as reader:
      num_record_batches = reader.num_record_batches
      b = reader.get_batch(3)

Because we have access to the entire payload, we know the number of record
batches in the file, and can read any at random.

.. ipython:: python

   num_record_batches
   b.equals(batch)

Reading from Stream and File Format for pandas
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The stream and file reader classes have a special ``read_pandas`` method to
simplify reading multiple record batches and converting them to a single
DataFrame output:

.. ipython:: python

   with pa.ipc.open_file(buf) as reader:
      df = reader.read_pandas()
   
   df[:5]

Efficiently Writing and Reading Arrow Data
------------------------------------------

Being optimized for zero copy and memory mapped data, Arrow allows to easily
read and write arrays consuming the minimum amount of resident memory.

When writing and reading raw Arrow data, we can use the Arrow File Format
or the Arrow Streaming Format.

To dump an array to file, you can use the :meth:`~pyarrow.ipc.new_file`
which will provide a new :class:`~pyarrow.ipc.RecordBatchFileWriter` instance
that can be used to write batches of data to that file.

For example to write an array of 10M integers, we could write it in 1000 chunks
of 10000 entries:

.. ipython:: python

      BATCH_SIZE = 10000
      NUM_BATCHES = 1000

      schema = pa.schema([pa.field('nums', pa.int32())])

      with pa.OSFile('bigfile.arrow', 'wb') as sink:
         with pa.ipc.new_file(sink, schema) as writer:
            for row in range(NUM_BATCHES):
                  batch = pa.record_batch([pa.array(range(BATCH_SIZE), type=pa.int32())], schema)
                  writer.write(batch)

record batches support multiple columns, so in practice we always write the
equivalent of a :class:`~pyarrow.Table`.

Writing in batches is effective because we in theory need to keep in memory only
the current batch we are writing. But when reading back, we can be even more effective
by directly mapping the data from disk and avoid allocating any new memory on read.

Under normal conditions, reading back our file will consume a few hundred megabytes
of memory:

.. ipython:: python

      with pa.OSFile('bigfile.arrow', 'rb') as source:
         loaded_array = pa.ipc.open_file(source).read_all()

      print("LEN:", len(loaded_array))
      print("RSS: {}MB".format(pa.total_allocated_bytes() >> 20))

To more efficiently read big data from disk, we can memory map the file, so that
Arrow can directly reference the data mapped from disk and avoid having to
allocate its own memory.
In such case the operating system will be able to page in the mapped memory
lazily and page it out without any write back cost when under pressure,
allowing to more easily read arrays bigger than the total memory.

.. ipython:: python

      with pa.memory_map('bigfile.arrow', 'rb') as source:
         loaded_array = pa.ipc.open_file(source).read_all()
      print("LEN:", len(loaded_array))
      print("RSS: {}MB".format(pa.total_allocated_bytes() >> 20))

.. note::

   Other high level APIs like :meth:`~pyarrow.parquet.read_table` also provide a
   ``memory_map`` option. But in those cases, the memory mapping can't help with
   reducing resident memory consumption. See :ref:`parquet_mmap` for details.
