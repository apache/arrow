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

.. _c-data-interface-statistics:

=====================================================
Passing statistics through the Arrow C data interface
=====================================================

Rationale
=========

Statistics are useful for fast query processing. Many query engines
use statistics to optimize their query plan.

Apache Arrow format doesn't have statistics but other formats that can
be read as Apache Arrow data may have statistics. For example, the
Apache Parquet C++ implementation can read an Apache Parquet file as
Apache Arrow data and the Apache Parquet file may have statistics.

One of :ref:`c-data-interface` use cases is the following:

1. Module A reads Apache Parquet file as Apache Arrow data.
2. Module A passes the read Apache Arrow data to module B through the
   Arrow C data interface.
3. Module B processes the passed Apache Arrow data.

If module A can pass the statistics associated with the Apache Parquet
file to module B through the Arrow C data interface, module B can use
the statistics to optimize its query plan.

Goals
-----

* Establish a standard way to pass statistics through the Arrow C data
  interface.
* Provide this in a manner that enables compatibility and ease of
  implementation for existing users of the Arrow C data interface.

Non-goals
---------

* Provide a common way to pass statistics that can be used for
  other interfaces such Arrow Flight too.

For example, ADBC has `the statistics related APIs
<https://arrow.apache.org/adbc/current/format/specification.html#statistics>`__.
This specification doesn't replace them.

This specification may fit some use cases of :ref:`format-ipc` not the
Arrow data interface. But we don't recommend this specification for
the Arrow IPC format for now. Because we may be able to define better
specification for the Arrow IPC format. The Arrow IPC format has some
different features compared with the Arrow C data interface. For
example, the Arrow IPC format can have :ref:`ipc-message-format
metadata for each message`. If you're interested in the specification
for passing statistics through the Arrow IPC format, please start a
discussion on the `Arrow development mailing-list
<https://arrow.apache.org/community/>`__.

.. _c-data-interface-statistics-schema:

Schema
======

This specification provides only the schema for statistics. The
producer passes statistics through the Arrow C data interface as an
Arrow map array that uses this schema.

Here is the outline of the schema for statistics::

    map<
      key: int32,
      items: map<
        key: dictionary<
          indices: int32,
          dictionary: utf8
        >,
        items: dense_union<...all needed types...>,
      >
    >

Here is the details of the top-level ``map``:

.. list-table::
   :header-rows: 1

   * - Key or items
     - Data type
     - Nullable
     - Notes
   * - key
     - ``int32``
     - ``true``
     - The zero-based column index, or null if the statistics
       describe the whole table or record batch.
   * - items
     - ``map``
     - ``false``
     - Statistics for the target column, table or record batch. See
       the separate table below for details.

Here is the details of the nested ``map`` as the items part of the
above ``map``:

.. list-table::
   :header-rows: 1

   * - Key or items
     - Data type
     - Nullable
     - Notes
   * - key
     - ``dictionary<indices: int32, dictionary: utf8>``
     - ``false``
     - Statistics key is string. Dictionary is used for
       efficiency. Different keys are assigned for exact value and
       approximate value. Also see the separate description below for
       statistics key.
   * - items
     - ``dense_union``
     - ``false``
     - Statistics value is dense union. It has at least all needed
       types based on statistics kinds in the keys. For example, you
       need at least ``int64`` and ``float64`` types when you have a
       ``int64`` distinct count statistic and a ``float64`` average
       byte width statistic. Also see the separate description below
       for statistics key.

       We don't standardize field names for the dense union because
       consumers can access to proper field by index not name. So
       producers can use any valid name for fields.

.. _c-data-interface-statistics-key:

Statistics key
--------------

Statistics key is string. ``dictionary<int32, utf8>`` is used for
efficiency.

We assign different statistics keys for variants instead of using
flags. For example, we assign different statistics keys for exact
value and approximate value.

The colon symbol ``:`` is to be used as a namespace separator like
:ref:`format_metadata`. It can be used multiple times in a key.

The ``ARROW`` pattern is a reserved namespace for pre-defined
statistics keys. User-defined statistics must not use it.

Here are pre-defined statistics keys:

.. list-table::
   :header-rows: 1

   * - Key
     - Data type
     - Notes
   * - ``ARROW:average_byte_width:exact``
     - ``float``
     - The average size in bytes of a row in the target. (exact)
   * - ``ARROW:average_byte_width:approximate``
     - ``float64``
     - The average size in bytes of a row in the target. (approximate)
   * - ``ARROW:distinct_count:exact``
     - ``int64``
     - The number of distinct values in the target. (exact)
   * - ``ARROW:distinct_count:approximate``
     - ``float64``
     - The number of distinct values in the target. (approximate)
   * - ``ARROW:max_byte_width:exact``
     - ``int64``
     - The maximum size in bytes of a row in the target. (exact)
   * - ``ARROW:max_byte_width:approximate``
     - ``float64``
     - The maximum size in bytes of a row in the target. (approximate)
   * - ``ARROW:max_value:exact``
     - Target dependent
     - The maximum value in the target. (exact)
   * - ``ARROW:max_value:approximate``
     - Target dependent
     - The maximum value in the target. (approximate)
   * - ``ARROW:min_value:exact``
     - Target dependent
     - The minimum value in the target. (exact)
   * - ``ARROW:min_value:approximate``
     - Target dependent
     - The minimum value in the target. (approximate)
   * - ``ARROW:row_count:exact``
     - ``int64``
     - The number of rows in the target table or record batch. (exact)
   * - ``ARROW:row_count:approximate``
     - ``float64``
     - The number of rows in the target table or record
       batch. (approximate)

If you find a missing statistics key that is usable for multiple
systems, please propose it on the `Arrow development mailing-list
<https://arrow.apache.org/community/>`__.

Examples
--------

TODO: Add at least C++ example.
