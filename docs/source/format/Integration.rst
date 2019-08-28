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

.. _format_integration_testing:

Integration Testing
===================

A JSON representation of Arrow columnar data is provided for
cross-language integration testing purposes.

The high level structure of a JSON integration test files is as follows

**Data file** ::

    {
      "schema": /*Schema*/,
      "batches": [ /*RecordBatch*/ ],
      "dictionaries": [ /*DictionaryBatch*/ ],
    }

**Schema** ::

    {
      "fields" : [
        /* Field */
      ]
    }

**Field** ::

    {
      "name" : "name_of_the_field",
      "nullable" : false,
      "type" : /* Type */,
      "children" : [ /* Field */ ],
    }

**RecordBatch**::

    {
      "count": /*length of batch*/,
      "columns": [ /* FieldData */ ]
    }

**FieldData**::

    {
      "name": "field_name",
      "count" "field_length",
      "BUFFER_TYPE": /* BufferData */
      ...
      "BUFFER_TYPE": /* BufferData */
      "children": [ /* FieldData */ ]
    }

Here ``BUFFER_TYPE`` is one of ``VALIDITY``, ``OFFSET`` (for
variable-length types), ``TYPE`` (for unions), or ``DATA``.

``BufferData`` is encoded based on the type of buffer:

* ``VALIDITY``: a JSON array of 1 (valid) and 0 (null)
* ``OFFSET``: a JSON array of integers for 32-bit offsets or
  string-formatted integers for 64-bit offsets
* ``TYPE``: a JSON array of integers
* ``DATA``: a JSON array of encoded values

The value encoding for ``DATA`` is different depending on the logical
type:

* For boolean type: an array of 1 (true) and 0 (false)
* For integer-based types (including timestamps): an array of integers
* For 64-bit integers: an array of integers formatted as JSON strings
  to avoid loss of precision
* For floating point types: as is
* For Binary types, a hex-string is produced to encode a variable- or
  fixed-size binary value

**Type**: ::

    {
      "name" : "null|struct|list|largelist|fixedsizelist|union|int|floatingpoint|utf8|largeutf8|binary|largebinary|fixedsizebinary|bool|decimal|date|time|timestamp|interval|duration|map"
      // fields as defined in the Flatbuffer depending on the type name
    }

Union: ::

    {
      "name" : "union",
      "mode" : "Sparse|Dense",
      "typeIds" : [ /* integer */ ]
    }

The ``typeIds`` field in the Union are the codes used to denote each type, which
may be different from the index of the child array. This is so that the union
type ids do not have to be enumerated from 0.

Int: ::

    {
      "name" : "int",
      "bitWidth" : /* integer */,
      "isSigned" : /* boolean */
    }

FloatingPoint: ::

    {
      "name" : "floatingpoint",
      "precision" : "HALF|SINGLE|DOUBLE"
    }

FixedSizeBinary: ::

    {
      "name" : "fixedsizebinary",
      "byteWidth" : /* byte width */
    }

Decimal: ::

    {
      "name" : "decimal",
      "precision" : /* integer */,
      "scale" : /* integer */
    }

Timestamp: ::

    {
      "name" : "timestamp",
      "unit" : "$TIME_UNIT"
      "timezone": "$timezone" [optional]
    }

``$TIME_UNIT`` is one of ``"SECOND|MILLISECOND|MICROSECOND|NANOSECOND"``

Duration: ::

    {
      "name" : "duration",
      "unit" : "$TIME_UNIT"
    }

Date: ::

    {
      "name" : "date",
      "unit" : "DAY|MILLISECOND"
    }

Time: ::

    {
      "name" : "time",
      "unit" : "$TIME_UNIT",
      "bitWidth": /* integer: 32 or 64 */
    }

Interval: ::

    {
      "name" : "interval",
      "unit" : "YEAR_MONTH"
    }
