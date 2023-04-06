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

Our strategy for integration testing between Arrow implementations is:

* Test datasets are specified in a custom human-readable, JSON-based format
  designed exclusively for Arrow's integration tests
* Each implementation provides a testing executable capable of converting
  between the JSON and the binary Arrow file representation
* Each testing executable is used to generate binary Arrow file representations
  from the JSON-based test datasets. These results are then used to call the
  testing executable of each other implementation to validate the contents
  against the corresponding JSON file.
  - *ie.* the C++ testing executable generates binary arrow files from JSON
  specified datasets. The resulting files are then used as input to the Java
  testing executable for validation, confirming that the Java implementation 
  can correctly read what the C++ implementation wrote.

Running integration tests
-------------------------

The integration test data generator and runner are implemented inside
the :ref:`Archery <archery>` utility.

The integration tests are run using the ``archery integration`` command.

.. code-block:: shell

   archery integration --help

In order to run integration tests, you'll first need to build each component
you want to include. See the respective developer docs for C++, Java, etc.
for instructions on building those.

Some languages may require additional build options to enable integration
testing. For C++, for example, you need to add ``-DARROW_BUILD_INTEGRATION=ON``
to your cmake command.

Depending on which components you have built, you can enable and add them to
the archery test run. For example, if you only have the C++ project built, run:

.. code-block:: shell

   archery integration --with-cpp=1


For Java, it may look like:

.. code-block:: shell

   VERSION=0.11.0-SNAPSHOT
   export ARROW_JAVA_INTEGRATION_JAR=$JAVA_DIR/tools/target/arrow-tools-$VERSION-jar-with-dependencies.jar
   archery integration --with-cpp=1 --with-java=1

To run all tests, including Flight integration tests, do:

.. code-block:: shell

   archery integration --with-all --run-flight

Note that we run these tests in continuous integration, and the CI job uses
docker-compose. You may also run the docker-compose job locally, or at least
refer to it if you have questions about how to build other languages or enable
certain tests.

See :ref:`docker-builds` for more information about the project's
``docker-compose`` configuration.

JSON test data format
---------------------

A JSON representation of Arrow columnar data is provided for
cross-language integration testing purposes.
This representation is `not canonical <https://lists.apache.org/thread.html/6947fb7666a0f9cc27d9677d2dad0fb5990f9063b7cf3d80af5e270f%40%3Cdev.arrow.apache.org%3E>`_
but it provides a human-readable way of verifying language implementations.

See `here <https://github.com/apache/arrow/tree/main/docs/source/format/integration_json_examples>`_
for some examples of this JSON data.

.. can we check in more examples, e.g. from the generated_*.json test files?

The high level structure of a JSON integration test files is as follows:

**Data file** ::

    {
      "schema": /*Schema*/,
      "batches": [ /*RecordBatch*/ ],
      "dictionaries": [ /*DictionaryBatch*/ ],
    }

All files contain ``schema`` and ``batches``, while ``dictionaries`` is only
present if there are dictionary type fields in the schema.

**Schema** ::

    {
      "fields" : [
        /* Field */
      ],
      "metadata" : /* Metadata */
    }

**Field** ::

    {
      "name" : "name_of_the_field",
      "nullable" : /* boolean */,
      "type" : /* Type */,
      "children" : [ /* Field */ ],
      "dictionary": {
        "id": /* integer */,
        "indexType": /* Type */,
        "isOrdered": /* boolean */
      },
      "metadata" : /* Metadata */
    }

The ``dictionary`` attribute is present if and only if the ``Field`` corresponds to a
dictionary type, and its ``id`` maps onto a column in the ``DictionaryBatch``. In this
case the ``type`` attribute describes the value type of the dictionary.

For primitive types, ``children`` is an empty array.

**Metadata** ::

    null |
    [ {
      "key": /* string */,
      "value": /* string */
    } ]

A key-value mapping of custom metadata. It may be omitted or null, in which case it is
considered equivalent to ``[]`` (no metadata). Duplicated keys are not forbidden here.

**Type**: ::

    {
      "name" : "null|struct|list|largelist|fixedsizelist|union|int|floatingpoint|utf8|largeutf8|binary|largebinary|fixedsizebinary|bool|decimal|date|time|timestamp|interval|duration|map"
    }

A ``Type`` will have other fields as defined in
`Schema.fbs <https://github.com/apache/arrow/tree/main/format/Schema.fbs>`_
depending on its name.

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
      "unit" : "$TIME_UNIT",
      "timezone": "$timezone"
    }

``$TIME_UNIT`` is one of ``"SECOND|MILLISECOND|MICROSECOND|NANOSECOND"``

"timezone" is an optional string.

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
      "unit" : "YEAR_MONTH|DAY_TIME"
    }

Union: ::

    {
      "name" : "union",
      "mode" : "SPARSE|DENSE",
      "typeIds" : [ /* integer */ ]
    }

The ``typeIds`` field in ``Union`` are the codes used to denote which member of
the union is active in each array slot. Note that in general these discriminants are not identical
to the index of the corresponding child array.

List: ::

    {
      "name": "list"
    }

The type that the list is a "list of" will be included in the ``Field``'s
"children" member, as a single ``Field`` there. For example, for a list of
``int32``, ::

    {
      "name": "list_nullable",
      "type": {
        "name": "list"
      },
      "nullable": true,
      "children": [
        {
          "name": "item",
          "type": {
            "name": "int",
            "isSigned": true,
            "bitWidth": 32
          },
          "nullable": true,
          "children": []
        }
      ]
    }

FixedSizeList: ::

    {
      "name": "fixedsizelist",
      "listSize": /* integer */
    }

This type likewise comes with a length-1 "children" array.

Struct: ::

    {
      "name": "struct"
    }

The ``Field``'s "children" contains an array of ``Fields`` with meaningful
names and types.

Map: ::

    {
      "name": "map",
      "keysSorted": /* boolean */
    }

The ``Field``'s "children" contains a single ``struct`` field, which itself
contains 2 children, named "key" and "value".

Null: ::

    {
      "name": "null"
    }

RunEndEncoded: ::

    {
      "name": "runendencoded"
    }

The ``Field``'s "children" should be exactly two child fields. The first
child must be named "run_ends", be non-nullable and be either an ``int16``,
``int32``, or ``int64`` type field. The second child must be named "values",
but can be of any type.

Extension types are, as in the IPC format, represented as their underlying
storage type plus some dedicated field metadata to reconstruct the extension
type.  For example, assuming a "uuid" extension type backed by a
FixedSizeBinary(16) storage, here is how a "uuid" field would be represented::

    {
      "name" : "name_of_the_field",
      "nullable" : /* boolean */,
      "type" : {
         "name" : "fixedsizebinary",
         "byteWidth" : 16
      },
      "children" : [],
      "metadata" : [
         {"key": "ARROW:extension:name", "value": "uuid"},
         {"key": "ARROW:extension:metadata", "value": "uuid-serialized"}
      ]
    }

**RecordBatch**::

    {
      "count": /* integer number of rows */,
      "columns": [ /* FieldData */ ]
    }

**DictionaryBatch**::

    {
      "id": /* integer */,
      "data": [ /* RecordBatch */ ]
    }

**FieldData**::

    {
      "name": "field_name",
      "count" "field_length",
      "$BUFFER_TYPE": /* BufferData */
      ...
      "$BUFFER_TYPE": /* BufferData */
      "children": [ /* FieldData */ ]
    }

The "name" member of a ``Field`` in the ``Schema`` corresponds to the "name"
of a ``FieldData`` contained in the "columns" of a ``RecordBatch``.
For nested types (list, struct, etc.), ``Field``'s "children" each have a
"name" that corresponds to the "name" of a ``FieldData`` inside the
"children" of that ``FieldData``.
For ``FieldData`` inside of a ``DictionaryBatch``, the "name" field does not
correspond to anything.

Here ``$BUFFER_TYPE`` is one of ``VALIDITY``, ``OFFSET`` (for
variable-length types, such as strings and lists), ``TYPE_ID`` (for unions),
or ``DATA``.

``BufferData`` is encoded based on the type of buffer:

* ``VALIDITY``: a JSON array of 1 (valid) and 0 (null). Data for  non-nullable
  ``Field`` still has a ``VALIDITY`` array, even though all values are 1.
* ``OFFSET``: a JSON array of integers for 32-bit offsets or
  string-formatted integers for 64-bit offsets
* ``TYPE_ID``: a JSON array of integers
* ``DATA``: a JSON array of encoded values

The value encoding for ``DATA`` is different depending on the logical
type:

* For boolean type: an array of 1 (true) and 0 (false).
* For integer-based types (including timestamps): an array of JSON numbers.
* For 64-bit integers: an array of integers formatted as JSON strings,
  so as to avoid loss of precision.
* For floating point types: an array of JSON numbers. Values are limited
  to 3 decimal places to avoid loss of precision.
* For binary types, an array of uppercase hex-encoded strings, so as
  to represent arbitrary binary data.
* For UTF-8 string types, an array of JSON strings.

For "list" and "largelist" types, ``BufferData`` has ``VALIDITY`` and
``OFFSET``, and the rest of the data is inside "children". These child
``FieldData`` contain all of the same attributes as non-child data, so in
the example of a list of ``int32``, the child data has ``VALIDITY`` and
``DATA``.

For "fixedsizelist", there is no ``OFFSET`` member because the offsets are
implied by the field's "listSize".

Note that the "count" for these child data may not match the parent "count".
For example, if a ``RecordBatch`` has 7 rows and contains a ``FixedSizeList``
of ``listSize`` 4, then the data inside the "children" of that ``FieldData``
will have count 28.

For "null" type, ``BufferData`` does not contain any buffers.

Archery Integration Test Cases
--------------------------------------

This list can make it easier to understand what manual testing may need to
be done for any future Arrow Format changes by knowing what cases the automated
integration testing actually tests.

There are two types of integration test cases: the ones populated on the fly
by the data generator in the Archery utility, and *gold* files that exist
in the `arrow-testing <https://github.com/apache/arrow-testing/tree/master/data/arrow-ipc-stream/integration>` 
repository.

Data Generator Tests
~~~~~~~~~~~~~~~~~~~~

This is the high-level description of the cases which are generated and
tested using the ``archery integration`` command (see ``get_generated_json_files`` 
in ``datagen.py``):

* Primitive Types
  - No Batches
  - Various Primitive Values
  - Batches with Zero Length
  - String and Binary Large offset cases
* Null Type
  * Trivial Null batches
* Decimal128
* Decimal256
* DateTime with various units
* Durations with various units
* Intervals
  - MonthDayNano interval is a separate case
* Map Types
  - Non-Canonical Maps
* Nested Types
  - Lists
  - Structs
  - Lists with Large Offsets
* Unions
* Custom Metadata
* Schemas with Duplicate Field Names
* Dictionary Types
  - Signed indices
  - Unsigned indices
  - Nested dictionaries
* Extension Types


Gold File Integration Tests
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pre-generated json and arrow IPC files (both file and stream format) exist
in the `arrow-testing <https://github.com/apache/arrow-testing>`__ repository
in the ``data/arrow-ipc-stream/integration`` directory. These serve as
*gold* files that are assumed to be correct for use in testing. They are 
referenced by ``runner.py`` in the code for the :ref:`Archery <archery>`
utility. Below are the test cases which are covered by them:

* Backwards Compatibility

  - The following cases are tested using the 0.14.1 format:

    + datetime
    + decimals
    + dictionaries
    + intervals
    + maps
    + nested types (list, struct)
    + primitives 
    + primitive with no batches
    + primitive with zero length batches

  - The following is tested for 0.17.1 format:

    + unions

* Endianness

  - The following cases are tested with both Little Endian and Big Endian versions for auto conversion

    + custom metadata
    + datetime
    + decimals
    + decimal256
    + dictionaries
    + dictionaries with unsigned indices
    + record batches with duplicate fieldnames
    + extension types
    + interval types
    + map types
    + non-canonical map data
    + nested types (lists, structs)
    + nested dictionaries
    + nested large offset types
    + nulls
    + primitive data
    + large offset binary and strings
    + primitives with no batches included
    + primitive batches with zero length
    + recursive nested types
    + union types

* Compression tests

  - LZ4
  - ZSTD

* Batches with Shared Dictionaries
