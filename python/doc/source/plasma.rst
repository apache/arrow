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

The Plasma In-Memory Object Store
=================================

.. contents:: Contents
  :depth: 3


The Plasma API
--------------

Starting the Plasma store
^^^^^^^^^^^^^^^^^^^^^^^^^

You can start the Plasma store by issuing a terminal command similar to the
following:

.. code-block:: bash

  plasma_store -m 1000000000 -s /tmp/plasma

The -m flag  specifies the size of the store in bytes, and the -s flag specifies
the socket that the store will listen at. Thus, the above command sets the
Plasma store  to use up to 1 GB of memory, and sets the socket to
``/tmp/plasma``.

Leave the current terminal window open as long as Plasma store should keep 
running. Messages, concerning such as disconnecting clients, may occasionally be 
outputted. To stop running the Plasma store, you can press ``CTRL-C`` in the terminal.

Creating a Plasma client
^^^^^^^^^^^^^^^^^^^^^^^^

To start the Plasma client, from within python, the same socket given to
``./plasma_store``  should then be passed into the connect method as shown below:

.. code-block:: python

  import pyarrow.plasma as plasma
  client = plasma.connect("/tmp/plasma", "", 0)

If the following error occurs from running the above Python code, that
means that either the socket given is incorrect, or the ``./plasma_store`` is 
not currently running. Make sure that you are still running the ``./plasma_store`` 
process in your plasma directory.

.. code-block:: shell

  >>> client = plasma.connect("/tmp/plasma", "", 0)
  Connection to socket failed for pathname /tmp/plasma
  Could not connect to socket /tmp/plasma


Object IDs 
^^^^^^^^^^

Each object in the Plasma store should be associated with a unique id. The 
Object ID then serves as a key for any client to fetch that object from 
the Plasma store. You can form an ``ObjectID`` object from a byte string of 
20 bytes.

.. code-block:: shell

  # Create ObjectID of 20 bytes, each byte being the byte (b) encoding of the letter "a"
  >>> id = plasma.ObjectID(20 * b"a")  

  # "a" is encoded as 61
  >>> id
  ObjectID(6161616161616161616161616161616161616161)

Random generation of Object IDs is often good enough to ensure unique ids. 
You can easily create a helper function that randomizes object ids as follows:

.. code-block:: python

  import numpy as np

  def random_object_id():
    return plasma.ObjectID(np.random.bytes(20))


Creating an Object
^^^^^^^^^^^^^^^^^^

Objects are created in Plasma in two stages. First, they are *created*, which 
allocates a buffer for the object. At this point, the client can write to the 
buffer and construct the object within the allocated buffer. 

To create an object for Plasma, you need to create an object id, as well as 
give the object's maximum size in bytes.

.. code-block:: python

  # Create an object.
  object_id = plasma.ObjectID(20 * b"a")  # Note that this is an ObjectID object, not a string
  object_size = 1000
  buffer = memoryview(client.create(object_id, object_size))

  # Write to the buffer.
  for i in range(1000):
    buffer[i] = i % 128

When the client is done, the client *seals* the buffer, making the object 
immutable, and making it available to other Plasma clients.

.. code-block:: python

  # Seal the object. This makes the object immutable and available to other clients.
  client.seal(object_id)


Getting an Object 
^^^^^^^^^^^^^^^^^

After an object has been sealed, any client who knows the object ID can get 
the object.

.. code-block:: python

  # Create a different client. Note that this second client could be
  # created in the same or in a separate, concurrent Python session.
  client2 = plasma.connect("/tmp/plasma", "", 0)

  # Get the object in the second client. This blocks until the object has been sealed.
  object_id2 = plasma.ObjectID(20 * b"a")  
  [buffer2] = client2.get([object_id])  # Note that you pass in as an ObjectID object, not a string

If the object has not been sealed yet, then the call to client.get will block 
until the object has been sealed by the client constructing the object. Using
the ``timeout_ms`` argument to get, you can specify a timeout for this (in
milliseconds). After the timeout, the interpreter will yield control back.

Note that the buffer fetched is not in the same object type as the buffer the
original client created to store the object in the first place. The 
buffer the original client created is a Python ``memoryview`` buffer object,
while the buffer returned from ``client.get`` is a Plasma-specific ``PlasmaBuffer`` 
object. It supports the Python buffer protocol, so you can create a memoryview
from it, which supports slicing and indexing to expose its data.

.. code-block:: shell

  >>> buffer
  <memory at 0x7fdbdc96e708>
  >>> buffer[1]
  1
  >>> buffer2
  <plasma.plasma.PlasmaBuffer object at 0x7fdbf2770e88>
  >>> view2 = memoryview(buffer2)
  >>> view2[1]
  1
  >>> view2[129]
  1
  >>> bytes(buffer[1:4])
  b'\x01\x02\x03'
  >>> bytes(view2[1:4])
  b'\x01\x02\x03'


Using Arrow and Pandas with Plasma
----------------------------------

Storing Arrow Objects in Plasma
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Creating an Arrow object still follows the two steps of *creating* it with 
a buffer, then *sealing* it, however Arrow objects such as ``Tensors`` may be
more complicated to write than simple binary data.

To create the object in Plasma, you still need an ``ObjectID`` and a size to 
pass in. To find out the size of your Arrow object, you can use pyarrow 
API such as ``pyarrow.get_tensor_size``.

.. code-block:: python

  import numpy as np
  import pyarrow as pa

  # Create a pyarrow.Tensor object from a numpy random 2-dimensional array
  data = np.random.randn(10, 4)
  tensor = pa.Tensor.from_numpy(data)

  # Create the object in Plasma
  object_id = plasma.ObjectID(np.random.bytes(20))   
  data_size = pa.get_tensor_size(tensor)
  buf = client.create(object_id, data_size)

To write the Arrow ``Tensor`` object into the buffer, you can use Plasma to 
convert the ``memoryview`` buffer into a ``pyarrow.FixedSizeBufferOutputStream`` 
object. A ``pyarrow.FixedSizeBufferOutputStream`` is a format suitable for Arrow's 
``pyarrow.write_tensor``:

.. code-block:: python

  # Write the tensor into the Plasma-allocated buffer
  stream = pa.FixedSizeBufferOutputStream(buf)
  pa.write_tensor(tensor, stream)  # Writes tensor's 552 bytes to Plasma stream

To finish storing the Arrow object in Plasma, you can seal it just like 
for any other data:

.. code-block:: python

  # Seal the Plasma object
  client.seal(object_id)

Getting Arrow Objects from Plasma
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For reading the object from Plasma to Arrow, you can fetch it as a ``PlasmaBuffer`` 
using its object id as usual. 

.. code-block:: python

  # Get the arrow object by ObjectID.
  [buf2] = client.get([object_id])

To convert the ``PlasmaBuffer`` back into the Arrow ``Tensor``, first you have to 
create a pyarrow ``BufferReader`` object from it. You can then pass the 
``BufferReader`` into ``pyarrow.read_tensor`` to reconstruct the Arrow ``Tensor``
object:

.. code-block:: python

  # Reconstruct the Arrow tensor object.
  reader = pa.BufferReader(buf2)
  tensor2 = pa.read_tensor(reader)

Finally, you can use ``pyarrow.read_tensor`` to convert the Arrow object 
back into numpy data:

.. code-block:: python

  # Convert back to numpy
  array = tensor2.to_numpy()

Storing Pandas DataFrames in Plasma
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Storing a Pandas ``DataFrame`` still follows the *create* then *seal* process 
of storing an object in the Plasma store, however one cannot directly write 
the ``DataFrame`` to Plasma with Pandas alone. Plasma also needs to know the 
size of the ``DataFrame`` to allocate a buffer for. 

One can instead use pyarrow and its supportive API as an intermediary step 
to import the Pandas ``DataFrame`` into Plasma. Arrow has multiple equivalent 
types to the various Pandas structures, see the :ref:`pandas` page for more
information.

You can create the pyarrow equivalent of a Pandas ``DataFrame`` by using 
``pyarrow.from_pandas`` to convert it to a ``RecordBatch``.

.. code-block:: python

  import pyarrow as pa
  import pandas as pd

  # Create a Pandas DataFrame
  d = {'one' : pd.Series([1., 2., 3.], index=['a', 'b', 'c']),
       'two' : pd.Series([1., 2., 3., 4.], index=['a', 'b', 'c', 'd'])}
  df = pd.DataFrame(d)

  # Convert the Pandas DataFrame into a PyArrow RecordBatch
  record_batch = pa.RecordBatch.from_pandas(df)

Creating the Plasma object requires an ``ObjectID`` and the size of the
data. Now that we have converted the Pandas ``DataFrame`` into a PyArrow 
``RecordBatch``, use the ``MockOutputStream`` to determine the
size of the Plasma object.

.. code-block:: python

  # Create the Plasma object from the PyArrow RecordBatch
  object_id = plasma.ObjectID(np.random.bytes(20))
  mock_sink = pa.MockOutputStream()
  stream_writer = pa.RecordBatchStreamWriter(mock_sink, record_batch.schema)
  stream_writer.write_batch(record_batch)
  stream_writer.close()
  data_size = mock_sink.size()
  buf = client.create(object_id, data_size)

Similar to storing an Arrow object, you have to convert the ``memoryview`` 
object into a ``plasma.FixedSizeBufferOutputStream`` object in order to 
work with pyarrow's API. Then convert the ``FixedSizeBufferOutputStream`` 
object into a pyarrow ``RecordBatchStreamWriter`` object to write out 
the PyArrow ``RecordBatch`` into Plasma as follows:

.. code-block:: python

  # Write the PyArrow RecordBatch to Plasma
  stream = pa.FixedSizeBufferOutputStream(buf)
  stream_writer = pa.RecordBatchStreamWriter(stream, record_batch.schema)
  stream_writer.write_batch(record_batch)
  stream_writer.close()

Finally, seal the finished object for use by all clients:

.. code-block:: python

  # Seal the Plasma object
  client.seal(object_id)

Getting Pandas DataFrames from Plasma
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Since we store the Pandas DataFrame as a PyArrow ``RecordBatch`` object, 
to get the object back from the Plasma store, we follow similar steps 
to those specified in `Getting Arrow Objects from Plasma`_. 

We first have to convert the ``PlasmaBuffer`` returned from ``client.get`` 
into an Arrow ``BufferReader`` object. 

.. code-block:: python

  # Fetch the Plasma object
  [data] = client.get([object_id])  # Get PlasmaBuffer from ObjectID
  buffer = pa.BufferReader(data)

From the ``BufferReader``, we can create a specific ``RecordBatchStreamReader`` 
in Arrow to reconstruct the stored PyArrow ``RecordBatch`` object.

.. code-block:: python

  # Convert object back into an Arrow RecordBatch
  reader = pa.RecordBatchStreamReader(buffer)
  record_batch = reader.read_next_batch()

The last step is to convert the PyArrow ``RecordBatch`` object back into 
the original Pandas ``DataFrame`` structure.

.. code-block:: python

  # Convert back into Pandas
  result = record_batch.to_pandas()
