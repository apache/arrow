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

   batch = pa.RecordBatch.from_arrays(data, ['f0', 'f1', 'f2'])
   batch.num_rows
   batch.num_columns

Now, we can begin writing a stream containing some number of these batches. For
this we use :class:`~pyarrow.RecordBatchStreamWriter`, which can write to a writeable
``NativeFile`` object or a writeable Python object:

.. ipython:: python

   sink = pa.BufferOutputStream()
   writer = pa.RecordBatchStreamWriter(sink, batch.schema)

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
convenience function ``pyarrow.open_stream``:

.. ipython:: python

   reader = pa.open_stream(buf)
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
:class:`~pyarrow.RecordBatchStreamWriter`:

.. ipython:: python

   sink = pa.BufferOutputStream()
   writer = pa.RecordBatchFileWriter(sink, batch.schema)

   for i in range(10):
      writer.write_batch(batch)
   writer.close()

   buf = sink.getvalue()
   buf.size

The difference between :class:`~pyarrow.RecordBatchFileReader` and
:class:`~pyarrow.RecordBatchStreamReader` is that the input source must have a
``seek`` method for random access. The stream reader only requires read
operations. We can also use the ``pyarrow.open_file`` method to open a file:

.. ipython:: python

   reader = pa.open_file(buf)

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

   df = pa.open_file(buf).read_pandas()
   df[:5]

Arbitrary Object Serialization
------------------------------

In ``pyarrow`` we are able to serialize and deserialize many kinds of Python
objects. While not a complete replacement for the ``pickle`` module, these
functions can be significantly faster, particular when dealing with collections
of NumPy arrays.

As an example, consider a dictionary containing NumPy arrays:

.. ipython:: python

   import numpy as np

   data = {
       i: np.random.randn(500, 500)
       for i in range(100)
   }

We use the ``pyarrow.serialize`` function to convert this data to a byte
buffer:

.. ipython:: python

   buf = pa.serialize(data).to_buffer()
   type(buf)
   buf.size

``pyarrow.serialize`` creates an intermediate object which can be converted to
a buffer (the ``to_buffer`` method) or written directly to an output stream.

``pyarrow.deserialize`` converts a buffer-like object back to the original
Python object:

.. ipython:: python

   restored_data = pa.deserialize(buf)
   restored_data[0]

When dealing with NumPy arrays, ``pyarrow.deserialize`` can be significantly
faster than ``pickle`` because the resulting arrays are zero-copy references
into the input buffer. The larger the arrays, the larger the performance
savings.

Consider this example, we have for ``pyarrow.deserialize``

.. ipython:: python

   %timeit restored_data = pa.deserialize(buf)

And for pickle:

.. ipython:: python

   import pickle
   pickled = pickle.dumps(data)
   %timeit unpickled_data = pickle.loads(pickled)

We aspire to make these functions a high-speed alternative to pickle for
transient serialization in Python big data applications.

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
``to_buffer`` method. To motivate this, support we have a list of NumPy arrays:

.. ipython:: python

   import numpy as np
   data = [np.random.randn(10, 10) for i in range(5)]

The call ``pa.serialize(data)`` does not copy the memory inside each of these
NumPy arrays. This serialized representation can be then decomposed into a
dictionary containing a sequence of ``pyarrow.Buffer`` objects containing
metadata for each array and references to the memory inside the arrays. To do
this, use the ``to_components`` method:

.. ipython:: python

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

   restored_data = pa.deserialize_components(components)
   restored_data[0]

``deserialize_components`` is also available as a method on
``SerializationContext`` objects.

Serializing pandas Objects
--------------------------

The default serialization context has optimized handling of pandas
objects like ``DataFrame`` and ``Series``. Combined with component-based
serialization above, this enables zero-copy transport of pandas DataFrame
objects not containing any Python objects:

.. ipython:: python

   import pandas as pd
   df = pd.DataFrame({'a': [1, 2, 3, 4, 5]})
   context = pa.default_serialization_context()
   serialized_df = context.serialize(df)
   df_components = serialized_df.to_components()
   original_df = context.deserialize_components(df_components)
   original_df

Feather Format
--------------

Feather is a lightweight file-format for data frames that uses the Arrow memory
layout for data representation on disk. It was created early in the Arrow
project as a proof of concept for fast, language-agnostic data frame storage
for Python (pandas) and R.

Compared with Arrow streams and files, Feather has some limitations:

* Only non-nested data types and categorical (dictionary-encoded) types are
  supported
* Supports only a single batch of rows, where general Arrow streams support an
  arbitrary number
* Supports limited scalar value types, adequate only for representing typical
  data found in R and pandas

We would like to continue to innovate in the Feather format, but we must wait
for an R implementation for Arrow to mature.

The ``pyarrow.feather`` module contains the read and write functions for the
format. The input and output are ``pandas.DataFrame`` objects:

.. code-block:: python

   import pyarrow.feather as feather

   feather.write_feather(df, '/path/to/file')
   read_df = feather.read_feather('/path/to/file')

``read_feather`` supports multithreaded reads, and may yield faster performance
on some files:

.. code-block:: python

   read_df = feather.read_feather('/path/to/file', nthreads=4)

These functions can read and write with file-like objects. For example:

.. code-block:: python

   with open('/path/to/file', 'wb') as f:
       feather.write_feather(df, f)

   with open('/path/to/file', 'rb') as f:
       read_df = feather.read_feather(f)

A file input to ``read_feather`` must support seeking.
