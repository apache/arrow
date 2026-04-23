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

.. _numpy_interop:

NumPy Integration
=================

PyArrow allows converting back and forth from
`NumPy <https://www.numpy.org/>`_ arrays to Arrow :ref:`Arrays <data.array>`.

NumPy to Arrow
--------------

To convert a NumPy array to Arrow, one can simply call the :func:`pyarrow.array`
factory function.

.. code-block:: python

   >>> import numpy as np
   >>> import pyarrow as pa
   >>> data = np.arange(10, dtype='int16')
   >>> arr = pa.array(data)
   >>> arr
   <pyarrow.lib.Int16Array object at ...>
   [
     0,
     1,
     2,
     3,
     4,
     5,
     6,
     7,
     8,
     9
   ]

Converting from NumPy supports a wide range of input dtypes, including
structured dtypes or strings.

Arrow to NumPy
--------------

In the reverse direction, it is possible to produce a view of an Arrow Array
for use with NumPy using the :meth:`~pyarrow.Array.to_numpy` method.
This is limited to primitive types for which NumPy has the same physical
representation as Arrow, and assuming the Arrow data has no nulls.

.. code-block:: python

   >>> import numpy as np
   >>> import pyarrow as pa
   >>> arr = pa.array([4, 5, 6], type=pa.int32())
   >>> view = arr.to_numpy()
   >>> view
   array([4, 5, 6], dtype=int32)

For more complex data types, you have to use the :meth:`~pyarrow.Array.to_pandas`
method (which will construct a Numpy array with Pandas semantics for, e.g.,
representation of null values).

Timezone-aware Timestamps
~~~~~~~~~~~~~~~~~~~~~~~~~

NumPy's ``datetime64`` type does not support timezones. When converting a
timezone-aware Arrow timestamp array to NumPy via :meth:`~pyarrow.Array.to_numpy`,
the timezone information is silently dropped:

.. code-block:: python

   >>> arr = pa.array([1735689600, 1735689600], type=pa.timestamp("s", tz="UTC"))  # doctest: +SKIP
   >>> arr.type  # doctest: +SKIP
   TimestampType(timestamp[s, tz=UTC])
   >>> arr.to_numpy()  # doctest: +SKIP
   array(['2025-01-01T00:00:00', '2025-01-01T00:00:00'],
         dtype='datetime64[s]')

If you need to preserve timezone information, there are two alternatives:

* Convert to a Pandas Series, which supports timezone-aware ``datetime64`` dtypes:

  .. code-block:: python

     >>> arr.to_pandas()  # doctest: +SKIP
     0   2025-01-01 00:00:00+00:00
     1   2025-01-01 00:00:00+00:00
     dtype: datetime64[s, UTC]

  .. note::

     For nested types (e.g., list arrays containing timestamps),
     ``to_pandas()`` may not preserve timezone information. Structs and maps
     do retain timezones, but lists currently do not. See
     `GH-41162 <https://github.com/apache/arrow/issues/41162>`_ for details.

* Convert to Python ``datetime`` objects, which carry ``tzinfo``:

  .. code-block:: python

     >>> arr.to_pylist()  # doctest: +SKIP
     [datetime.datetime(2025, 1, 1, 0, 0, tzinfo=zoneinfo.ZoneInfo(key='UTC')),
      datetime.datetime(2025, 1, 1, 0, 0, tzinfo=zoneinfo.ZoneInfo(key='UTC'))]
