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
   writer = pa.ipc.new_stream(sink, batch.schema)

Here we used an in-memory Arrow buffer stream, but this could have been a
socket or some other IO sink.

When creating the ``StreamWriter``, we pass the schema, since the schema
(column names and types) must be the same for all of the batches sent in this
particular stream. Now we can do:

.. ipython:: python

   for i in range(5):
      writer.write_batch(batch)
   writer.close()

   buf = sink.getvalue()
   buf.size

Now ``buf`` contains the complete stream as an in-memory byte buffer. We can
read such a stream with :class:`~pyarrow.RecordBatchStreamReader` or the
convenience function ``pyarrow.ipc.open_stream``:

.. ipython:: python

   reader = pa.ipc.open_stream(buf)
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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :class:`~pyarrow.RecordBatchFileWriter` has the same API as
:class:`~pyarrow.RecordBatchStreamWriter`. You can create one with
:func:`~pyarrow.ipc.new_file`:

.. ipython:: python

   sink = pa.BufferOutputStream()
   writer = pa.ipc.new_file(sink, batch.schema)

   for i in range(10):
      writer.write_batch(batch)
   writer.close()

   buf = sink.getvalue()
   buf.size

The difference between :class:`~pyarrow.RecordBatchFileReader` and
:class:`~pyarrow.RecordBatchStreamReader` is that the input source must have a
``seek`` method for random access. The stream reader only requires read
operations. We can also use the :func:`~pyarrow.ipc.open_file` method to open a file:

.. ipython:: python

   reader = pa.ipc.open_file(buf)

Because we have access to the entire payload, we know the number of record
batches in the file, and can read any at random:

.. ipython:: python

   reader.num_record_batches
   b = reader.get_batch(3)
   b.equals(batch)

Reading from Stream and File Format for pandas
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The stream and file reader classes have a special ``read_pandas`` method to
simplify reading multiple record batches and converting them to a single
DataFrame output:

.. ipython:: python

   df = pa.ipc.open_file(buf).read_pandas()
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

Arbitrary Object Serialization
------------------------------

.. warning::

   The custom serialization functionality is deprecated in pyarrow 2.0, and
   will be removed in a future version.

   While the serialization functions in this section utilize the Arrow stream
   protocol internally, they do not produce data that is compatible with the
   above ``ipc.open_file`` and ``ipc.open_stream`` functions.

   For arbitrary objects, you can use the standard library ``pickle``
   functionality instead. For pyarrow objects, you can use the IPC
   serialization format through the ``pyarrow.ipc`` module, as explained
   above.

   PyArrow serialization was originally meant to provide a higher-performance
   alternative to ``pickle`` thanks to zero-copy semantics.  However,
   ``pickle`` protocol 5 gained support for zero-copy using out-of-band
   buffers, and can be used instead for similar benefits.

In ``pyarrow`` we are able to serialize and deserialize many kinds of Python
objects.  As an example, consider a dictionary containing NumPy arrays:

.. ipython:: python

   import numpy as np

   data = {
       i: np.random.randn(500, 500)
       for i in range(100)
   }

We use the ``pyarrow.serialize`` function to convert this data to a byte
buffer:

.. ipython:: python
   :okwarning:

   buf = pa.serialize(data).to_buffer()
   type(buf)
   buf.size

``pyarrow.serialize`` creates an intermediate object which can be converted to
a buffer (the ``to_buffer`` method) or written directly to an output stream.

``pyarrow.deserialize`` converts a buffer-like object back to the original
Python object:

.. ipython:: python
   :okwarning:

   restored_data = pa.deserialize(buf)
   restored_data[0]


Serializing Custom Data Types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If an unrecognized data type is encountered when serializing an object,
``pyarrow`` will fall back on using ``pickle`` for converting that type to a
byte string. There may be a more efficient way, though.

Consider a class with two members, one of which is a NumPy array:

.. code-block:: python

   class MyData:
       def __init__(self, name, data):
           self.name = name
           self.data = data

We write functions to convert this to and from a dictionary with simpler types:

.. code-block:: python

   def _serialize_MyData(val):
       return {'name': val.name, 'data': val.data}

   def _deserialize_MyData(data):
       return MyData(data['name'], data['data']

then, we must register these functions in a ``SerializationContext`` so that
``MyData`` can be recognized:

.. code-block:: python

   context = pa.SerializationContext()
   context.register_type(MyData, 'MyData',
                         custom_serializer=_serialize_MyData,
                         custom_deserializer=_deserialize_MyData)

Lastly, we use this context as an additional argument to ``pyarrow.serialize``:

.. code-block:: python

   buf = pa.serialize(val, context=context).to_buffer()
   restored_val = pa.deserialize(buf, context=context)

The ``SerializationContext`` also has convenience methods ``serialize`` and
``deserialize``, so these are equivalent statements:

.. code-block:: python

   buf = context.serialize(val).to_buffer()
   restored_val = context.deserialize(buf)

Component-based Serialization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For serializing Python objects containing some number of NumPy arrays, Arrow
buffers, or other data types, it may be desirable to transport their serialized
representation without having to produce an intermediate copy using the
``to_buffer`` method. To motivate this, suppose we have a list of NumPy arrays:

.. ipython:: python

   import numpy as np
   data = [np.random.randn(10, 10) for i in range(5)]

The call ``pa.serialize(data)`` does not copy the memory inside each of these
NumPy arrays. This serialized representation can be then decomposed into a
dictionary containing a sequence of ``pyarrow.Buffer`` objects containing
metadata for each array and references to the memory inside the arrays. To do
this, use the ``to_components`` method:

.. ipython:: python
   :okwarning:

   serialized = pa.serialize(data)
   components = serialized.to_components()

The particular details of the output of ``to_components`` are not too
important. The objects in the ``'data'`` field are ``pyarrow.Buffer`` objects,
which are zero-copy convertible to Python ``memoryview`` objects:

.. ipython:: python

   memoryview(components['data'][0])

A memoryview can be converted back to a Arrow ``Buffer`` with
``pyarrow.py_buffer``:

.. ipython:: python

   mv = memoryview(components['data'][0])
   buf = pa.py_buffer(mv)

An object can be reconstructed from its component-based representation using
``deserialize_components``:

.. ipython:: python
   :okwarning:

   restored_data = pa.deserialize_components(components)
   restored_data[0]

``deserialize_components`` is also available as a method on
``SerializationContext`` objects.
