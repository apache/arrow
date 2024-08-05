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
be read as Apache Arrow data may have statistics. For example, Apache
Parquet C++ can read Apache Parquet file as Apache Arrow data and
Apache Parquet file may have statistics.

One of the Arrow C data interface use cases is the following:

1. Module A reads Apache Parquet file as Apache Arrow data
2. Module A passes the read Apache Arrow data to module B through the
   Arrow C data interface
3. Module B processes the passed Apache Arrow data

If module A can pass the statistics associated with the Apache Parquet
file to module B through the Arrow C data interface, module B can use
the statistics to optimize its query plan.

Goals
-----

* Provide the standard way to pass statistics through the Arrow C data
  interface to avoid reinventing the wheel.
* The standard way must be easy to use with the Arrow C data interface.

Non-goals
---------

* Provide a common way to pass statistics that can be used for
  other interfaces such Arrow Flight too.

For example, ADBC has the statistics related APIs. This specification
doesn't replace them.

.. _c-data-interface-statistics-schema:

Schema
======

This specification provides only the schema for statistics. Producers
passes statistics as a map Arrow array that uses the schema through
the Arrow C data interface.

Here is the schema for a statistics map Arrow Array:

.. list-table::
   :header-rows: 1

   * - Key or items
     - Data type
     - Nullable
     - Notes
   * - key
     - ``int32``
     - ``true``
     - The column index or null if the statistics refer to whole table
       or record batch.
   * - items
     - ``map``
     - ``false``
     - Statistics for the target column, table or record batch. See
       the separated table for details.

Here is the schema for the statistics map:

.. list-table::
   :header-rows: 1

   * - Key or items
     - Data type
     - Nullable
     - Notes
   * - key
     - ``dictionary<int32, utf8>``
     - ``false``
     - Statistics key is string. Dictionary is used for
       efficiency. Different keys are assigned for exact value and
       approximate value. See also the separated description for
       statistics key.
   * - items
     - ``dense_union``
     - ``false``
     - Statistics value is dense union. It has at least all needed
       types based on statistics kinds in the keys. For example, you
       need at least ``int64`` and ``float64`` types when you have a
       ``int64`` distinct count statistic and a ``float64`` average
       byte width statistic. See also the separated description for
       statistics key.

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
