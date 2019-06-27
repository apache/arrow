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

.. _pandas_interop:

Pandas Integration
==================

To interface with `pandas <https://pandas.pydata.org/>`_, PyArrow provides
various conversion routines to consume pandas structures and convert back
to them.

.. note::
   While pandas uses NumPy as a backend, it has enough peculiarities
   (such as a different type system, and support for null values) that this
   is a separate topic from :ref:`numpy_interop`.

To follow examples in this document, make sure to run:

.. ipython:: python

   import pandas as pd
   import pyarrow as pa

DataFrames
----------

The equivalent to a pandas DataFrame in Arrow is a :ref:`Table <data.table>`.
Both consist of a set of named columns of equal length. While pandas only
supports flat columns, the Table also provides nested columns, thus it can
represent more data than a DataFrame, so a full conversion is not always possible.

Conversion from a Table to a DataFrame is done by calling
:meth:`pyarrow.Table.to_pandas`. The inverse is then achieved by using
:meth:`pyarrow.Table.from_pandas`.

.. code-block:: python

    import pyarrow as pa
    import pandas as pd

    df = pd.DataFrame({"a": [1, 2, 3]})
    # Convert from pandas to Arrow
    table = pa.Table.from_pandas(df)
    # Convert back to pandas
    df_new = table.to_pandas()

    # Infer Arrow schema from pandas
    schema = pa.Schema.from_pandas(df)

By default ``pyarrow`` tries to preserve and restore the ``.index``
data as accurately as possible. See the section below for more about
this, and how to disable this logic.

Series
------

In Arrow, the most similar structure to a pandas Series is an Array.
It is a vector that contains data of the same type as linear memory. You can
convert a pandas Series to an Arrow Array using :meth:`pyarrow.Array.from_pandas`.
As Arrow Arrays are always nullable, you can supply an optional mask using
the ``mask`` parameter to mark all null-entries.

Handling pandas Indexes
-----------------------

Methods like :meth:`pyarrow.Table.from_pandas` have a
``preserve_index`` option which defines how to preserve (store) or not
to preserve (to not store) the data in the ``index`` member of the
corresponding pandas object. This data is tracked using schema-level
metadata in the internal ``arrow::Schema`` object.

The default of ``preserve_index`` is ``None``, which behaves as
follows:

* ``RangeIndex`` is stored as metadata-only, not requiring any extra
  storage.
* Other index types are stored as one or more physical data columns in
  the resulting :class:`Table`

To not store the index at all pass ``preserve_index=False``. Since
storing a ``RangeIndex`` can cause issues in some limited scenarios
(such as storing multiple DataFrame objects in a Parquet file), to
force all index data to be serialized in the resulting table, pass
``preserve_index=True``.

Type differences
----------------

With the current design of pandas and Arrow, it is not possible to convert all
column types unmodified. One of the main issues here is that pandas has no
support for nullable columns of arbitrary type. Also ``datetime64`` is currently
fixed to nanosecond resolution. On the other side, Arrow might be still missing
support for some types.

pandas -> Arrow Conversion
~~~~~~~~~~~~~~~~~~~~~~~~~~

+------------------------+--------------------------+
| Source Type (pandas)   | Destination Type (Arrow) |
+========================+==========================+
| ``bool``               | ``BOOL``                 |
+------------------------+--------------------------+
| ``(u)int{8,16,32,64}`` | ``(U)INT{8,16,32,64}``   |
+------------------------+--------------------------+
| ``float32``            | ``FLOAT``                |
+------------------------+--------------------------+
| ``float64``            | ``DOUBLE``               |
+------------------------+--------------------------+
| ``str`` / ``unicode``  | ``STRING``               |
+------------------------+--------------------------+
| ``pd.Categorical``     | ``DICTIONARY``           |
+------------------------+--------------------------+
| ``pd.Timestamp``       | ``TIMESTAMP(unit=ns)``   |
+------------------------+--------------------------+
| ``datetime.date``      | ``DATE``                 |
+------------------------+--------------------------+

Arrow -> pandas Conversion
~~~~~~~~~~~~~~~~~~~~~~~~~~

+-------------------------------------+--------------------------------------------------------+
| Source Type (Arrow)                 | Destination Type (pandas)                              |
+=====================================+========================================================+
| ``BOOL``                            | ``bool``                                               |
+-------------------------------------+--------------------------------------------------------+
| ``BOOL`` *with nulls*               | ``object`` (with values ``True``, ``False``, ``None``) |
+-------------------------------------+--------------------------------------------------------+
| ``(U)INT{8,16,32,64}``              | ``(u)int{8,16,32,64}``                                 |
+-------------------------------------+--------------------------------------------------------+
| ``(U)INT{8,16,32,64}`` *with nulls* | ``float64``                                            |
+-------------------------------------+--------------------------------------------------------+
| ``FLOAT``                           | ``float32``                                            |
+-------------------------------------+--------------------------------------------------------+
| ``DOUBLE``                          | ``float64``                                            |
+-------------------------------------+--------------------------------------------------------+
| ``STRING``                          | ``str``                                                |
+-------------------------------------+--------------------------------------------------------+
| ``DICTIONARY``                      | ``pd.Categorical``                                     |
+-------------------------------------+--------------------------------------------------------+
| ``TIMESTAMP(unit=*)``               | ``pd.Timestamp`` (``np.datetime64[ns]``)               |
+-------------------------------------+--------------------------------------------------------+
| ``DATE``                            | ``object``(with ``datetime.date`` objects)             |
+-------------------------------------+--------------------------------------------------------+

Categorical types
~~~~~~~~~~~~~~~~~

TODO

Datetime (Timestamp) types
~~~~~~~~~~~~~~~~~~~~~~~~~~

TODO

Date types
~~~~~~~~~~

While dates can be handled using the ``datetime64[ns]`` type in
pandas, some systems work with object arrays of Python's built-in
``datetime.date`` object:

.. ipython:: python

   from datetime import date
   s = pd.Series([date(2018, 12, 31), None, date(2000, 1, 1)])
   s

When converting to an Arrow array, the ``date32`` type will be used by
default:

.. ipython:: python

   arr = pa.array(s)
   arr.type
   arr[0]

To use the 64-bit ``date64``, specify this explicitly:

.. ipython:: python

   arr = pa.array(s, type='date64')
   arr.type

When converting back with ``to_pandas``, object arrays of
``datetime.date`` objects are returned:

.. ipython:: python

   arr.to_pandas()

If you want to use NumPy's ``datetime64`` dtype instead, pass
``date_as_object=False``:

.. ipython:: python

   s2 = pd.Series(arr.to_pandas(date_as_object=False))
   s2.dtype

.. warning::

   As of Arrow ``0.13`` the parameter ``date_as_object`` is ``True``
   by default. Older versions must pass ``date_as_object=True`` to
   obtain this behavior

Time types
~~~~~~~~~~

TODO
