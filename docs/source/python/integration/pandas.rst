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

`Pandas categorical <https://pandas.pydata.org/pandas-docs/stable/user_guide/categorical.html>`_
columns are converted to :ref:`Arrow dictionary arrays <data.dictionary>`,
a special array type optimized to handle repeated and limited
number of possible values.

.. ipython:: python

   df = pd.DataFrame({"cat": pd.Categorical(["a", "b", "c", "a", "b", "c"])})
   df.cat.dtype.categories
   df

   table = pa.Table.from_pandas(df)
   table

We can inspect the :class:`~.ChunkedArray` of the created table and see the
same categories of the Pandas DataFrame.

.. ipython:: python

   column = table[0]
   chunk = column.chunk(0)
   chunk.dictionary
   chunk.indices

Datetime (Timestamp) types
~~~~~~~~~~~~~~~~~~~~~~~~~~

`Pandas Timestamps <https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html>`_
use the ``datetime64[ns]`` type in Pandas and are converted to an Arrow
:class:`~.TimestampArray`.

.. ipython:: python

   df = pd.DataFrame({"datetime": pd.date_range("2020-01-01T00:00:00Z", freq="H", periods=3)})
   df.dtypes
   df

   table = pa.Table.from_pandas(df)
   table

In this example the Pandas Timestamp is time zone aware
(``UTC`` on this case), and this information is used to create the Arrow
:class:`~.TimestampArray`.

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

The builtin ``datetime.time`` objects inside Pandas data structures will be
converted to an Arrow ``time64`` and :class:`~.Time64Array` respectively.

.. ipython:: python

   from datetime import time
   s = pd.Series([time(1, 1, 1), time(2, 2, 2)])
   s

   arr = pa.array(s)
   arr.type
   arr

When converting to pandas, arrays of ``datetime.time`` objects are returned:

.. ipython:: python

   arr.to_pandas()

Nullable types
--------------

In Arrow all data types are nullable, meaning they support storing missing
values. In pandas, however, not all data types have support for missing data.
Most notably, the default integer data types do not, and will get casted
to float when missing values are introduced. Therefore, when an Arrow array
or table gets converted to pandas, integer columns will become float when
missing values are present:

.. code-block:: python

   >>> arr = pa.array([1, 2, None])
   >>> arr
   <pyarrow.lib.Int64Array object at 0x7f07d467c640>
   [
     1,
     2,
     null
   ]
   >>> arr.to_pandas()
   0    1.0
   1    2.0
   2    NaN
   dtype: float64

Pandas has experimental nullable data types
(https://pandas.pydata.org/docs/user_guide/integer_na.html). Arrows supports
round trip conversion for those:

.. code-block:: python

   >>> df = pd.DataFrame({'a': pd.Series([1, 2, None], dtype="Int64")})
   >>> df
         a
   0     1
   1     2
   2  <NA>

   >>> table = pa.table(df)
   >>> table
   Out[32]:
   pyarrow.Table
   a: int64
   ----
   a: [[1,2,null]]

   >>> table.to_pandas()
         a
   0     1
   1     2
   2  <NA>

   >>> table.to_pandas().dtypes
   a    Int64
   dtype: object

This roundtrip conversion works because metadata about the original pandas
DataFrame gets stored in the Arrow table. However, if you have Arrow data (or
e.g. a Parquet file) not originating from a pandas DataFrame with nullable
data types, the default conversion to pandas will not use those nullable
dtypes.

The :meth:`pyarrow.Table.to_pandas` method has a ``types_mapper`` keyword
that can be used to override the default data type used for the resulting
pandas DataFrame. This way, you can instruct Arrow to create a pandas
DataFrame using nullable dtypes.

.. code-block:: python

   >>> table = pa.table({"a": [1, 2, None]})
   >>> table.to_pandas()
        a
   0  1.0
   1  2.0
   2  NaN
   >>> table.to_pandas(types_mapper={pa.int64(): pd.Int64Dtype()}.get)
         a
   0     1
   1     2
   2  <NA>

The ``types_mapper`` keyword expects a function that will return the pandas
data type to use given a pyarrow data type. By using the ``dict.get`` method,
we can create such a function using a dictionary.

If you want to use all currently supported nullable dtypes by pandas, this
dictionary becomes:

.. code-block:: python

   dtype_mapping = {
       pa.int8(): pd.Int8Dtype(),
       pa.int16(): pd.Int16Dtype(),
       pa.int32(): pd.Int32Dtype(),
       pa.int64(): pd.Int64Dtype(),
       pa.uint8(): pd.UInt8Dtype(),
       pa.uint16(): pd.UInt16Dtype(),
       pa.uint32(): pd.UInt32Dtype(),
       pa.uint64(): pd.UInt64Dtype(),
       pa.bool_(): pd.BooleanDtype(),
       pa.float32(): pd.Float32Dtype(),
       pa.float64(): pd.Float64Dtype(),
       pa.string(): pd.StringDtype(),
   }

   df = table.to_pandas(types_mapper=dtype_mapping.get)


When using the pandas API for reading Parquet files (``pd.read_parquet(..)``),
this can also be achieved by passing ``use_nullable_dtypes``:

.. code-block:: python

   df = pd.read_parquet(path, use_nullable_dtypes=True)


Memory Usage and Zero Copy
--------------------------

When converting from Arrow data structures to pandas objects using various
``to_pandas`` methods, one must occasionally be mindful of issues related to
performance and memory usage.

Since pandas's internal data representation is generally different from the
Arrow columnar format, zero copy conversions (where no memory allocation or
computation is required) are only possible in certain limited cases.

In the worst case scenario, calling ``to_pandas`` will result in two versions
of the data in memory, one for Arrow and one for pandas, yielding approximately
twice the memory footprint. We have implement some mitigations for this case,
particularly when creating large ``DataFrame`` objects, that we describe below.

Zero Copy Series Conversions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Zero copy conversions from ``Array`` or ``ChunkedArray`` to NumPy arrays or
pandas Series are possible in certain narrow cases:

* The Arrow data is stored in an integer (signed or unsigned ``int8`` through
  ``int64``) or floating point type (``float16`` through ``float64``). This
  includes many numeric types as well as timestamps.
* The Arrow data has no null values (since these are represented using bitmaps
  which are not supported by pandas).
* For ``ChunkedArray``, the data consists of a single chunk,
  i.e. ``arr.num_chunks == 1``. Multiple chunks will always require a copy
  because of pandas's contiguousness requirement.

In these scenarios, ``to_pandas`` or ``to_numpy`` will be zero copy. In all
other scenarios, a copy will be required.

Reducing Memory Use in ``Table.to_pandas``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As of this writing, pandas applies a data management strategy called
"consolidation" to collect like-typed DataFrame columns in two-dimensional
NumPy arrays, referred to internally as "blocks". We have gone to great effort
to construct the precise "consolidated" blocks so that pandas will not perform
any further allocation or copies after we hand off the data to
``pandas.DataFrame``. The obvious downside of this consolidation strategy is
that it forces a "memory doubling".

To try to limit the potential effects of "memory doubling" during
``Table.to_pandas``, we provide a couple of options:

* ``split_blocks=True``, when enabled ``Table.to_pandas`` produces one internal
  DataFrame "block" for each column, skipping the "consolidation" step. Note
  that many pandas operations will trigger consolidation anyway, but the peak
  memory use may be less than the worst case scenario of a full memory
  doubling. As a result of this option, we are able to do zero copy conversions
  of columns in the same cases where we can do zero copy with ``Array`` and
  ``ChunkedArray``.
* ``self_destruct=True``, this destroys the internal Arrow memory buffers in
  each column ``Table`` object as they are converted to the pandas-compatible
  representation, potentially releasing memory to the operating system as soon
  as a column is converted. Note that this renders the calling ``Table`` object
  unsafe for further use, and any further methods called will cause your Python
  process to crash.

Used together, the call

.. code-block:: python

   df = table.to_pandas(split_blocks=True, self_destruct=True)
   del table  # not necessary, but a good practice

will yield significantly lower memory usage in some scenarios. Without these
options, ``to_pandas`` will always double memory.

Note that ``self_destruct=True`` is not guaranteed to save memory. Since the
conversion happens column by column, memory is also freed column by column. But
if multiple columns share an underlying buffer, then no memory will be freed
until all of those columns are converted. In particular, due to implementation
details, data that comes from IPC or Flight is prone to this, as memory will be
laid out as follows::

  Record Batch 0: Allocation 0: array 0 chunk 0, array 1 chunk 0, ...
  Record Batch 1: Allocation 1: array 0 chunk 1, array 1 chunk 1, ...
  ...

In this case, no memory can be freed until the entire table is converted, even
with ``self_destruct=True``.
