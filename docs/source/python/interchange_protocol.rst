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

.. _pyarrow-dataframe-interchange-protocol:

Dataframe Interchange Protocol
==============================

The interchange protocol is implemented for ``pa.Table`` and
``pa.RecordBatch`` and is used to interchange data between
PyArrow and other dataframe libraries that also have the
protocol implemented. The data structures that are supported
in the protocol are primitive data types plus the dictionary
data type. The protocol also has missing data support and
it supports chunking, meaning accessing the
data in “batches” of rows.


The Python dataframe interchange protocol is designed by the
`Consortium for Python Data API Standards <https://data-apis.org/>`_
in order to enable data interchange between dataframe
libraries in the Python ecosystem. See more about the
standard in the
`protocol documentation <https://data-apis.org/dataframe-protocol/latest/index.html>`_.

From PyArrow to other libraries: ``__dataframe__()`` method
-----------------------------------------------------------

The ``__dataframe__()`` method creates a new exchange object that
the consumer library can take and construct an object of it's own.

.. code-block::

    >>> import pyarrow as pa
    >>> table = pa.table({"n_attendees": [100, 10, 1]})
    >>> table.__dataframe__()
    <pyarrow.interchange.dataframe._PyArrowDataFrame object at ...>

This is meant to be used by the consumer library when calling
the ``from_dataframe()`` function and is not meant to be used manually
by the user.

From other libraries to PyArrow: ``from_dataframe()``
-----------------------------------------------------

With the ``from_dataframe()`` function, we can construct a :class:`pyarrow.Table`
from any dataframe object that implements the
``__dataframe__()`` method via the dataframe interchange
protocol.

We can for example take a pandas dataframe and construct a
PyArrow table with the use of the interchange protocol:

.. code-block::

    >>> import pyarrow
    >>> from pyarrow.interchange import from_dataframe

    >>> import pandas as pd
    >>> df = pd.DataFrame({
    ...         "n_attendees": [100, 10, 1],
    ...         "country": ["Italy", "Spain", "Slovenia"],
    ...     })
    >>> df
       n_attendees   country
    0          100     Italy
    1           10     Spain
    2            1  Slovenia
    >>> from_dataframe(df)
    pyarrow.Table
    n_attendees: int64
    country: large_string
    ----
    n_attendees: [[100,10,1]]
    country: [["Italy","Spain","Slovenia"]]

We can do the same with a polars dataframe:

.. code-block::

    >>> import polars as pl
    >>> from datetime import datetime
    >>> arr = [datetime(2023, 5, 20, 10, 0),
    ...        datetime(2023, 5, 20, 11, 0),
    ...        datetime(2023, 5, 20, 13, 30)]
    >>> df = pl.DataFrame({
    ...          'Talk': ['About Polars','Intro into PyArrow','Coding in Rust'],
    ...          'Time': arr,
    ...      })
    >>> df
    shape: (3, 2)
    ┌────────────────────┬─────────────────────┐
    │ Talk               ┆ Time                │
    │ ---                ┆ ---                 │
    │ str                ┆ datetime[μs]        │
    ╞════════════════════╪═════════════════════╡
    │ About Polars       ┆ 2023-05-20 10:00:00 │
    │ Intro into PyArrow ┆ 2023-05-20 11:00:00 │
    │ Coding in Rust     ┆ 2023-05-20 13:30:00 │
    └────────────────────┴─────────────────────┘
    >>> from_dataframe(df)
    pyarrow.Table
    Talk: large_string
    Time: timestamp[us]
    ----
    Talk: [["About Polars","Intro into PyArrow","Coding in Rust"]]
    Time: [[2023-05-20 10:00:00.000000,2023-05-20 11:00:00.000000,2023-05-20 13:30:00.000000]]
