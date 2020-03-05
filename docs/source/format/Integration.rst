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
  designed for Arrow
* Each implementation provides a testing executable capable of converting
  between the JSON and the binary Arrow file representation
* The test executable is also capable of validating the contents of a binary
  file against a corresponding JSON file

Running integration tests
-------------------------

The integration test data generator and runner uses ``archery``, a Python script
that requires Python 3.6 or higher. You can create a standalone Python
distribution and environment for running the tests by using
`miniconda <https://conda.io/miniconda.html>`_. On Linux this is:

.. code-block:: shell
   MINICONDA_URL=https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
   wget -O miniconda.sh $MINICONDA_URL
   bash miniconda.sh -b -p miniconda
   export PATH=`pwd`/miniconda/bin:$PATH

   conda create -n arrow-integration python=3.6 nomkl numpy six
   conda activate arrow-integration


If you are on macOS, instead use the URL:

.. code-block:: shell
   MINICONDA_URL=https://repo.continuum.io/miniconda/Miniconda3-latest-MacOSX-x86_64.sh

Once you have Python, you can install archery

.. code-block:: shell
   pip install -e dev/archery

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

JSON test data format
---------------------

A JSON representation of Arrow columnar data is provided for
cross-language integration testing purposes.
This representation is `not canonical <https://lists.apache.org/thread.html/6947fb7666a0f9cc27d9677d2dad0fb5990f9063b7cf3d80af5e270f%40%3Cdev.arrow.apache.org%3E>`_
but it provides a human-readable way of verifying language implementations.

See `here <https://github.com/apache/arrow/tree/master/integration/data>`_
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
      ]
    }

**Field** ::

    {
      "name" : "name_of_the_field",
      "nullable" : /* boolean */,
      "type" : /* Type */,
      "children" : [ /* Field */ ],
    }

If the Field corresponds to a dictionary type, the "type" attribute
corresponds to the dictionary values, and the Field includes an additional
"dictionary" member, the "id" of which maps onto a column in the
``DictionaryBatch`` : ::

    "dictionary": {
      "id": /* integer */,
      "indexType": /* Type */,
      "isOrdered": /* boolean */
    }

For primitive types, "children" is an empty array.

**RecordBatch**::

    {
      "count": /* integer number of rows in the batch */,
      "columns": [ /* FieldData */ ]
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

Here ``$BUFFER_TYPE`` is one of ``VALIDITY``, ``OFFSET`` (for
variable-length types, such as strings), ``TYPE`` (for unions), or ``DATA``.

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

Union: ::

    {
      "name" : "union",
      "mode" : "Sparse|Dense",
      "typeIds" : [ /* integer */ ]
    }

The ``typeIds`` field in the Union are the codes used to denote each type, which
may be different from the index of the child array. This is so that the union
type ids do not have to be enumerated from 0.
