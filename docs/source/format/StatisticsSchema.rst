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

.. _statistics-schema:

=================
Statistics schema
=================

.. warning:: This specification should be considered experimental.

Rationale
=========

Statistics are useful for fast query processing. Many query engines
use statistics to optimize their query plan.

Apache Arrow format doesn't have statistics but other formats that can
be read as Apache Arrow data may have statistics. For example, the
Apache Parquet C++ implementation can read an Apache Parquet file as
Apache Arrow data and the Apache Parquet file may have statistics.

We standardize the representation of statistics as an Apache Arrow
array for ease of exchange.

Use case
--------

One of :ref:`c-stream-interface` use cases is the following:

1. Module A reads Apache Parquet file as Apache Arrow data.
2. Module A passes the read Apache Arrow data to module B through the
   Arrow C stream interface.
3. Module B processes the passed Apache Arrow data.

If module A can pass the statistics associated with the Apache Parquet
file to module B, module B can use the statistics to optimize its
query plan.

For example, DuckDB uses this approach but DuckDB couldn't use
statistics because there wasn't a standardized way to represent
statistics for the Apache Arrow data.

.. seealso::

   `duckdb::ArrowTableFunction::ArrowScanBind() in DuckDB 1.1.3
   <https://github.com/duckdb/duckdb/blob/v1.1.3/src/function/table/arrow.cpp#L373-L403>`_

Goals
-----

* Establish a standard way to represent statistics as an Apache Arrow
  array.

Non-goals
---------

* Establish a standard way to pass an Apache Arrow array that
  represents statistics.
* Establish a standard way to embed statistics into an Apache Arrow
  array itself.

Schema
======

This specification provides only the schema for statistics. This is
the canonical schema to represent statistics about an Apache Arrow
dataset as Apache Arrow data.

Here is the outline of the schema for statistics::

    struct<
      column: int32,
      statistics: map<
        key: dictionary<values: utf8, indices: int32>,
        items: dense_union<...all needed types...>
      >
    >

Here is the details of top-level ``struct``:

.. list-table::
   :header-rows: 1

   * - Name
     - Data type
     - Nullable
     - Notes
   * - ``column``
     - ``int32``
     - ``true``
     - The zero-based column index, or null if the statistics
       describe the whole table or record batch.

       The column index is computed as the same rule used by
       :ref:`ipc-recordbatch-message`.
   * - ``statistics``
     - ``map``
     - ``false``
     - Statistics for the target column, table or record batch. See
       the separate table below for details.

Here is the details of the ``map`` of the ``statistics``:

.. list-table::
   :header-rows: 1

   * - Key or items
     - Data type
     - Nullable
     - Notes
   * - key
     - ``dictionary<values: utf8, indices: int32>``
     - ``false``
     - The string key is the name of the
       statistic. Dictionary-encoding is used for efficiency as the
       same statistic may be repeated for different columns.
       Different keys are assigned for exact and approximate statistic
       values. Each statistic has their own description below.
   * - items
     - ``dense_union``
     - ``false``
     - Statistics value is dense union. It has at least all needed
       types based on statistics kinds in the keys. For example, you
       need at least ``int64`` and ``float64`` types when you have a
       ``int64`` distinct count statistic and a ``float64`` average
       byte width statistic. See the description of each statistic below.

       Dense union arrays have names for each field but we don't standardize
       field names for these because we can access the proper
       field by type code instead. So we can use any valid name for
       the fields.

.. _statistics-schema-name:

Standard statistics
-------------------

Each statistic kind has a name that appears as a key in the statistics
map for each column or entire table. ``dictionary<values: utf8,
indices: int32>`` is used to encode the name for space-efficiency.

We assign different names for variations of the same statistic instead
of using flags. For example, we assign different statistic names for
exact and approximate values of the "distinct count" statistic.

The colon symbol ``:`` is to be used as a namespace separator like
:ref:`format_metadata`. It can be used multiple times in a name.

The ``ARROW`` prefix is a reserved namespace for pre-defined statistic
names in current and future versions of this specification.
User-defined statistics must not use it. For example, you can use your
product name as namespace such as ``MY_PRODUCT:my_statistics:exact``.

Here are pre-defined statistics names:

.. list-table::
   :header-rows: 1

   * - Name
     - Data type
     - Notes
   * - ``ARROW:average_byte_width:exact``
     - ``float64``
     - The average size in bytes of a row in the target
       column. (exact)
   * - ``ARROW:average_byte_width:approximate``
     - ``float64``
     - The average size in bytes of a row in the target
       column. (approximate)
   * - ``ARROW:distinct_count:exact``
     - ``int64``
     - The number of distinct values in the target column. (exact)
   * - ``ARROW:distinct_count:approximate``
     - ``float64``
     - The number of distinct values in the target
       column. (approximate)
   * - ``ARROW:max_byte_width:exact``
     - ``int64``
     - The maximum size in bytes of a row in the target
       column. (exact)
   * - ``ARROW:max_byte_width:approximate``
     - ``float64``
     - The maximum size in bytes of a row in the target
       column. (approximate)
   * - ``ARROW:max_value:exact``
     - Target dependent
     - The maximum value in the target column. (exact)
   * - ``ARROW:max_value:approximate``
     - Target dependent
     - The maximum value in the target column. (approximate)
   * - ``ARROW:min_value:exact``
     - Target dependent
     - The minimum value in the target column. (exact)
   * - ``ARROW:min_value:approximate``
     - Target dependent
     - The minimum value in the target column. (approximate)
   * - ``ARROW:null_count:exact``
     - ``int64``
     - The number of nulls in the target column. (exact)
   * - ``ARROW:null_count:approximate``
     - ``float64``
     - The number of nulls in the target column. (approximate)
   * - ``ARROW:row_count:exact``
     - ``int64``
     - The number of rows in the target table, record batch or
       array. (exact)
   * - ``ARROW:row_count:approximate``
     - ``float64``
     - The number of rows in the target table, record batch or
       array. (approximate)

If you find a statistic that might be useful to multiple systems,
please propose it on the `Apache Arrow development mailing-list
<https://arrow.apache.org/community/>`__.

Interoperability improves when producers and consumers of statistics
follow a previously agreed upon statistic specification.

.. _statistics-schema-examples:

Examples
========

Here are some examples to help you understand.

Simple record batch
-------------------

Schema::

    vendor_id: int32
    passenger_count: int64

Data::

    vendor_id:       [5, 1, 5, 1, 5]
    passenger_count: [1, 1, 2, 0, null]

Statistics:

.. list-table::
   :header-rows: 1

   * - Target
     - Name
     - Value
   * - Record batch
     - The number of rows
     - ``5``
   * - ``vendor_id``
     - The number of nulls
     - ``0``
   * - ``vendor_id``
     - The number of distinct values
     - ``2``
   * - ``vendor_id``
     - The max value
     - ``5``
   * - ``vendor_id``
     - The min value
     - ``1``
   * - ``passenger_count``
     - The number of nulls
     - ``1``
   * - ``passenger_count``
     - The number of distinct values
     - ``3``
   * - ``passenger_count``
     - The max value
     - ``2``
   * - ``passenger_count``
     - The min value
     - ``0``

Column indexes:

.. list-table::
   :header-rows: 1

   * - Index
     - Target
   * - ``0``
     - ``vendor_id``
   * - ``1``
     - ``passenger_count``

Statistics schema::

    struct<
      column: int32,
      statistics: map<
        key: dictionary<values: utf8, indices: int32>,
        items: dense_union<0: int64>
      >
    >

Statistics array::

    column: [
      null, # record batch
      0,    # vendor_id
      0,    # vendor_id
      0,    # vendor_id
      0,    # vendor_id
      1,    # passenger_count
      1,    # passenger_count
      1,    # passenger_count
      1,    # passenger_count
    ]
    statistics:
      key:
        values: [
          "ARROW:row_count:exact",
          "ARROW:null_count:exact",
          "ARROW:distinct_count:exact",
          "ARROW:max_value:exact",
          "ARROW:min_value:exact",
        ],
        indices: [
          0, # "ARROW:row_count:exact"
          1, # "ARROW:null_count:exact"
          2, # "ARROW:distinct_count:exact"
          3, # "ARROW:max_value:exact"
          4, # "ARROW:min_value:exact"
          1, # "ARROW:null_count:exact"
          2, # "ARROW:distinct_count:exact"
          3, # "ARROW:max_value:exact"
          4, # "ARROW:min_value:exact"
        ]
      items:
        children:
          0: [ # int64
            5, # record batch: "ARROW:row_count:exact"
            0, # vendor_id: "ARROW:null_count:exact"
            2, # vendor_id: "ARROW:distinct_count:exact"
            5, # vendor_id: "ARROW:max_value:exact"
            1, # vendor_id: "ARROW:min_value:exact"
            1, # passenger_count: "ARROW:null_count:exact"
            3, # passenger_count: "ARROW:distinct_count:exact"
            2, # passenger_count: "ARROW:max_value:exact"
            0, # passenger_count: "ARROW:min_value:exact"
          ]
        types: [ # all values are int64
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
        ]
        offsets: [
          0,
          1,
          2,
          3,
          4,
          5,
          6,
          7,
          8,
        ]

Complex record batch
--------------------

This uses nested types.

Schema::

    col1: struct<a: int32, b: list<item: int64>, c: float64>
    col2: utf8

Data::

    col1: [
            {a: 1, b: [20, 30, 40], c: 2.9},
            {a: 2, b: null,         c: -2.9},
            {a: 3, b: [99],         c: null},
          ]
    col2: ["x", null, "z"]

Statistics:

.. list-table::
   :header-rows: 1

   * - Target
     - Name
     - Value
   * - Record batch
     - The number of rows
     - ``3``
   * - ``col1``
     - The number of nulls
     - ``0``
   * - ``col1.a``
     - The number of nulls
     - ``0``
   * - ``col1.a``
     - The number of distinct values
     - ``3``
   * - ``col1.a``
     - The approximate max value
     - ``5``
   * - ``col1.a``
     - The approximate min value
     - ``0``
   * - ``col1.b``
     - The number of nulls
     - ``1``
   * - ``col1.b.item``
     - The max value
     - ``99``
   * - ``col1.b.item``
     - The min value
     - ``20``
   * - ``col1.c``
     - The number of nulls
     - ``1``
   * - ``col1.c``
     - The approximate max value
     - ``3.0``
   * - ``col1.c``
     - The approximate min value
     - ``-3.0``
   * - ``col2``
     - The number of nulls
     - ``1``
   * - ``col2``
     - The number of distinct values
     - ``2``

Column indexes:

.. list-table::
   :header-rows: 1

   * - Index
     - Target
   * - ``0``
     - ``col1``
   * - ``1``
     - ``col1.a``
   * - ``2``
     - ``col1.b``
   * - ``3``
     - ``col1.b.item``
   * - ``4``
     - ``col1.c``
   * - ``5``
     - ``col2``

See also :ref:`ipc-recordbatch-message` how to compute column indexes.

Statistics schema::

    struct<
      column: int32,
      statistics: map<
        key: dictionary<values: utf8, indices: int32>,
        items: dense_union<
          # For the number of rows, the number of nulls and so on.
          0: int64,
          # For the max/min values of col1.c.
          1: float64
        >
      >
    >

Statistics array::

    column: [
      null, # record batch
      0,    # col1
      1,    # col1.a
      1,    # col1.a
      1,    # col1.a
      1,    # col1.a
      2,    # col1.b
      3,    # col1.b.item
      3,    # col1.b.item
      4,    # col1.c
      4,    # col1.c
      4,    # col1.c
      5,    # col2
      5,    # col2
    ]
    statistics:
      key:
        values: [
          "ARROW:row_count:exact",
          "ARROW:null_count:exact",
          "ARROW:distinct_count:exact",
          "ARROW:max_value:approximate",
          "ARROW:min_value:approximate",
          "ARROW:max_value:exact",
          "ARROW:min_value:exact",
        ]
        indices: [
          0, # "ARROW:row_count:exact"
          1, # "ARROW:null_count:exact"
          1, # "ARROW:null_count:exact"
          2, # "ARROW:distinct_count:exact"
          3, # "ARROW:max_value:approximate"
          4, # "ARROW:min_value:approximate"
          1, # "ARROW:null_count:exact"
          5, # "ARROW:max_value:exact"
          6, # "ARROW:min_value:exact"
          1, # "ARROW:null_count:exact"
          3, # "ARROW:max_value:approximate"
          4, # "ARROW:min_value:approximate"
          1, # "ARROW:null_count:exact"
          2, # "ARROW:distinct_count:exact"
        ]
      items:
        children:
          0: [ # int64
            3,  # record batch: "ARROW:row_count:exact"
            0,  # col1: "ARROW:null_count:exact"
            0,  # col1.a: "ARROW:null_count:exact"
            3,  # col1.a: "ARROW:distinct_count:exact"
            5,  # col1.a: "ARROW:max_value:approximate"
            0,  # col1.a: "ARROW:min_value:approximate"
            1,  # col1.b: "ARROW:null_count:exact"
            99, # col1.b.item: "ARROW:max_value:exact"
            20, # col1.b.item: "ARROW:min_value:exact"
            1,  # col1.c: "ARROW:null_count:exact"
            1,  # col2: "ARROW:null_count:exact"
            2,  # col2: "ARROW:distinct_count:exact"
          ]
          1: [ # float64
            3.0,  # col1.c: "ARROW:max_value:approximate"
            -3.0, # col1.c: "ARROW:min_value:approximate"
          ]
        types: [
          0, # int64: record batch: "ARROW:row_count:exact"
          0, # int64: col1: "ARROW:null_count:exact"
          0, # int64: col1.a: "ARROW:null_count:exact"
          0, # int64: col1.a: "ARROW:distinct_count:exact"
          0, # int64: col1.a: "ARROW:max_value:approximate"
          0, # int64: col1.a: "ARROW:min_value:approximate"
          0, # int64: col1.b: "ARROW:null_count:exact"
          0, # int64: col1.b.item: "ARROW:max_value:exact"
          0, # int64: col1.b.item: "ARROW:min_value:exact"
          0, # int64: col1.c: "ARROW:null_count:exact"
          1, # float64: col1.c: "ARROW:max_value:approximate"
          1, # float64: col1.c: "ARROW:min_value:approximate"
          0, # int64: col2: "ARROW:null_count:exact"
          0, # int64: col2: "ARROW:distinct_count:exact"
        ]
        offsets: [
          0,  # int64: record batch: "ARROW:row_count:exact"
          1,  # int64: col1: "ARROW:null_count:exact"
          2,  # int64: col1.a: "ARROW:null_count:exact"
          3,  # int64: col1.a: "ARROW:distinct_count:exact"
          4,  # int64: col1.a: "ARROW:max_value:approximate"
          5,  # int64: col1.a: "ARROW:min_value:approximate"
          6,  # int64: col1.b: "ARROW:null_count:exact"
          7,  # int64: col1.b.item: "ARROW:max_value:exact"
          8,  # int64: col1.b.item: "ARROW:min_value:exact"
          9,  # int64: col1.c: "ARROW:null_count:exact"
          0,  # float64: col1.c: "ARROW:max_value:approximate"
          1,  # float64: col1.c: "ARROW:min_value:approximate"
          10, # int64: col2: "ARROW:null_count:exact"
          11, # int64: col2: "ARROW:distinct_count:exact"
        ]

Simple array
------------

Schema::

    int64

Data::

    [1, 1, 2, 0, null]

Statistics:

.. list-table::
   :header-rows: 1

   * - Target
     - Name
     - Value
   * - Array
     - The number of rows
     - ``5``
   * - Array
     - The number of nulls
     - ``1``
   * - Array
     - The number of distinct values
     - ``3``
   * - Array
     - The max value
     - ``2``
   * - Array
     - The min value
     - ``0``

Column indexes:

.. list-table::
   :header-rows: 1

   * - Index
     - Target
   * - ``0``
     - Array

Statistics schema::

    struct<
      column: int32,
      statistics: map<
        key: dictionary<values: utf8, indices: int32>,
        items: dense_union<0: int64>
      >
    >

Statistics array::

    column: [
      0, # array
      0, # array
      0, # array
      0, # array
      0, # array
    ]
    statistics:
      key:
        values: [
          "ARROW:row_count:exact",
          "ARROW:null_count:exact",
          "ARROW:distinct_count:exact",
          "ARROW:max_value:exact",
          "ARROW:min_value:exact",
        ]
        indices: [
          0, # "ARROW:row_count:exact"
          1, # "ARROW:null_count:exact"
          2, # "ARROW:distinct_count:exact"
          3, # "ARROW:max_value:exact"
          4, # "ARROW:min_value:exact"
        ]
      items:
        children:
          0: [ # int64
            5, # array: "ARROW:row_count:exact"
            1, # array: "ARROW:null_count:exact"
            3, # array: "ARROW:distinct_count:exact"
            2, # array: "ARROW:max_value:exact"
            0, # array: "ARROW:min_value:exact"
          ]
        types: [ # all values are int64
          0,
          0,
          0,
          0,
          0,
        ]
        offsets: [
          0,
          1,
          2,
          3,
          4,
        ]

Complex array
-------------

This uses nested types.

Schema::

    struct<a: int32, b: list<item: int64>, c: float64>

Data::

    [
      {a: 1, b: [20, 30, 40], c: 2.9},
      {a: 2, b: null,         c: -2.9},
      {a: 3, b: [99],         c: null},
    ]

Statistics:

.. list-table::
   :header-rows: 1

   * - Target
     - Name
     - Value
   * - Array
     - The number of rows
     - ``3``
   * - Array
     - The number of nulls
     - ``0``
   * - ``a``
     - The number of nulls
     - ``0``
   * - ``a``
     - The number of distinct values
     - ``3``
   * - ``a``
     - The approximate max value
     - ``5``
   * - ``a``
     - The approximate min value
     - ``0``
   * - ``b``
     - The number of nulls
     - ``1``
   * - ``b.item``
     - The max value
     - ``99``
   * - ``b.item``
     - The min value
     - ``20``
   * - ``c``
     - The number of nulls
     - ``1``
   * - ``c``
     - The approximate max value
     - ``3.0``
   * - ``c``
     - The approximate min value
     - ``-3.0``

Column indexes:

.. list-table::
   :header-rows: 1

   * - Index
     - Target
   * - ``0``
     - Array
   * - ``1``
     - ``a``
   * - ``2``
     - ``b``
   * - ``3``
     - ``b.item``
   * - ``4``
     - ``c``

See also :ref:`ipc-recordbatch-message` how to compute column indexes.

Statistics schema::

    struct<
      column: int32,
      statistics: map<
        key: dictionary<values: utf8, indices: int32>,
        items: dense_union<
          # For the number of rows, the number of nulls and so on.
          0: int64,
          # For the max/min values of c.
          1: float64
        >
      >
    >

Statistics array::

    column: [
      0, # array
      0, # array
      1, # a
      1, # a
      1, # a
      1, # a
      2, # b
      3, # b.item
      3, # b.item
      4, # c
      4, # c
      4, # c
    ]
    statistics:
      key:
        values: [
          "ARROW:row_count:exact",
          "ARROW:null_count:exact",
          "ARROW:distinct_count:exact",
          "ARROW:max_value:approximate",
          "ARROW:min_value:approximate",
          "ARROW:max_value:exact",
          "ARROW:min_value:exact",
        ]
        indices: [
          0, # "ARROW:row_count:exact"
          1, # "ARROW:null_count:exact"
          1, # "ARROW:null_count:exact"
          2, # "ARROW:distinct_count:exact"
          3, # "ARROW:max_value:approximate"
          4, # "ARROW:min_value:approximate"
          1, # "ARROW:null_count:exact"
          5, # "ARROW:max_value:exact"
          6, # "ARROW:min_value:exact"
          1, # "ARROW:null_count:exact"
          3, # "ARROW:max_value:approximate"
          4, # "ARROW:min_value:approximate"
        ]
      items:
        children:
          0: [ # int64
            3,  # array: "ARROW:row_count:exact"
            0,  # array: "ARROW:null_count:exact"
            0,  # a: "ARROW:null_count:exact"
            3,  # a: "ARROW:distinct_count:exact"
            5,  # a: "ARROW:max_value:approximate"
            0,  # a: "ARROW:min_value:approximate"
            1,  # b: "ARROW:null_count:exact"
            99, # b.item: "ARROW:max_value:exact"
            20, # b.item: "ARROW:min_value:exact"
            1,  # c: "ARROW:null_count:exact"
          ]
          1: [ # float64
            3.0,  # c: "ARROW:max_value:approximate"
            -3.0, # c: "ARROW:min_value:approximate"
          ]
        types: [
          0, # int64: array: "ARROW:row_count:exact"
          0, # int64: array: "ARROW:null_count:exact"
          0, # int64: a: "ARROW:null_count:exact"
          0, # int64: a: "ARROW:distinct_count:exact"
          0, # int64: a: "ARROW:max_value:approximate"
          0, # int64: a: "ARROW:min_value:approximate"
          0, # int64: b: "ARROW:null_count:exact"
          0, # int64: b.item: "ARROW:max_value:exact"
          0, # int64: b.item: "ARROW:min_value:exact"
          0, # int64: c: "ARROW:null_count:exact"
          1, # float64: c: "ARROW:max_value:approximate"
          1, # float64: c: "ARROW:min_value:approximate"
        ]
        offsets: [
          0, # int64: array: "ARROW:row_count:exact"
          1, # int64: array: "ARROW:null_count:exact"
          2, # int64: a: "ARROW:null_count:exact"
          3, # int64: a: "ARROW:distinct_count:exact"
          4, # int64: a: "ARROW:max_value:approximate"
          5, # int64: a: "ARROW:min_value:approximate"
          6, # int64: b: "ARROW:null_count:exact"
          7, # int64: b.item: "ARROW:max_value:exact"
          8, # int64: b.item: "ARROW:min_value:exact"
          9, # int64: c: "ARROW:null_count:exact"
          0, # float64: c: "ARROW:max_value:approximate"
          1, # float64: c: "ARROW:min_value:approximate"
        ]
