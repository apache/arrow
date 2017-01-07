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

Pandas Interface
================

To interface with Pandas, PyArrow provides various conversion routines to
consume Pandas structures and convert back to them.

DataFrames
----------

The equivalent to a Pandas DataFrame in Arrow is a :class:`pyarrow.table.Table`.
Both consist of a set of named columns of equal length. While Pandas only
supports flat columns, the Table also provides nested columns, thus it can
represent more data than a DataFrame, so a full conversion is not always possible.

Conversion from a Table to a DataFrame is done by calling
:meth:`pyarrow.table.Table.to_pandas`. The inverse is then achieved by using
:meth:`pyarrow.Table.from_pandas`. This conversion routine provides the
convience parameter ``timestamps_to_ms``. Although Arrow supports timestamps of
different resolutions, Pandas only supports nanosecond timestamps and most
other systems (e.g. Parquet) only work on millisecond timestamps. This parameter
can be used to already do the time conversion during the Pandas to Arrow
conversion.

.. code-block:: python

    import pyarrow as pa
    import pandas as pd

    df = pd.DataFrame({"a": [1, 2, 3]})
    # Convert from Pandas to Arrow
    table = pa.Table.from_pandas(df)
    # Convert back to Pandas
    df_new = table.to_pandas()


Series
------

In Arrow, the most similar structure to a Pandas Series is an Array.
It is a vector that contains data of the same type as linear memory. You can
convert a Pandas Series to an Arrow Array using :meth:`pyarrow.array.from_pandas_series`.
As Arrow Arrays are always nullable, you can supply an optional mask using
the ``mask`` parameter to mark all null-entries.

Type differences
----------------

With the current design of Pandas and Arrow, it is not possible to convert all
column types unmodified. One of the main issues here is that Pandas has no
support for nullable columns of arbitrary type. Also ``datetime64`` is currently
fixed to nanosecond resolution. On the other side, Arrow might be still missing
support for some types.

Pandas -> Arrow Conversion
~~~~~~~~~~~~~~~~~~~~~~~~~~

+------------------------+--------------------------+
| Source Type (Pandas)   | Destination Type (Arrow) |
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
| ``pd.Timestamp``       | ``TIMESTAMP(unit=ns)``   |
+------------------------+--------------------------+
| ``pd.Categorical``     | *not supported*          |
+------------------------+--------------------------+

Arrow -> Pandas Conversion
~~~~~~~~~~~~~~~~~~~~~~~~~~

+-------------------------------------+--------------------------------------------------------+
| Source Type (Arrow)                 | Destination Type (Pandas)                              |
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
| ``TIMESTAMP(unit=*)``               | ``pd.Timestamp`` (``np.datetime64[ns]``)               |
+-------------------------------------+--------------------------------------------------------+
