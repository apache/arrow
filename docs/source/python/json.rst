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

.. currentmodule:: pyarrow.json
.. _json:

Reading JSON files
==================

Arrow supports reading columnar data from JSON files.  In this context, a
JSON file consists of multiple JSON objects, one per line, representing
individual data rows.  For example, this file represents two rows of data
with four columns "a", "b", "c", "d":

.. code-block:: json

   {"a": 1, "b": 2.0, "c": "foo", "d": false}
   {"a": 4, "b": -5.5, "c": null, "d": true}

The features currently offered are the following:

* multi-threaded or single-threaded reading
* automatic decompression of input files (based on the filename extension,
  such as ``my_data.json.gz``)
* sophisticated type inference (see below)


Usage
-----

JSON reading functionality is available through the :mod:`pyarrow.json` module.
In many cases, you will simply call the :func:`read_json` function
with the file path you want to read from::

   >>> from pyarrow import json
   >>> fn = 'my_data.json'
   >>> table = json.read_json(fn)
   >>> table
   pyarrow.Table
   a: int64
   b: double
   c: string
   d: bool
   >>> table.to_pandas()
      a    b     c      d
   0  1  2.0   foo  False
   1  4 -5.5  None   True


Automatic Type Inference
------------------------

Arrow :ref:`data types <data.types>` are inferred from the JSON types and
values of each column:

* JSON null values convert to the ``null`` type, but can fall back to any
  other type.
* JSON booleans convert to ``bool_``.
* JSON numbers convert to ``int64``, falling back to ``float64`` if a
  non-integer is encountered.
* JSON strings of the kind "YYYY-MM-DD" and "YYYY-MM-DD hh:mm:ss" convert
  to ``timestamp[s]``, falling back to ``utf8`` if a conversion error occurs.
* JSON arrays convert to a ``list`` type, and inference proceeds recursively
  on the JSON arrays' values.
* Nested JSON objects convert to a ``struct`` type, and inference proceeds
  recursively on the JSON objects' values.

Thus, reading this JSON file:

.. code-block:: json

   {"a": [1, 2], "b": {"c": true, "d": "1991-02-03"}}
   {"a": [3, 4, 5], "b": {"c": false, "d": "2019-04-01"}}

returns the following data::

   >>> table = json.read_json("my_data.json")
   >>> table
   pyarrow.Table
   a: list<item: int64>
     child 0, item: int64
   b: struct<c: bool, d: timestamp[s]>
     child 0, c: bool
     child 1, d: timestamp[s]
   >>> table.to_pandas()
              a                                       b
   0     [1, 2]   {'c': True, 'd': 1991-02-03 00:00:00}
   1  [3, 4, 5]  {'c': False, 'd': 2019-04-01 00:00:00}


Customized parsing
------------------

To alter the default parsing settings in case of reading JSON files with an
unusual structure, you should create a :class:`ParseOptions` instance
and pass it to :func:`read_json`.  For example, you can pass an explicit
:ref:`schema <data.schema>` in order to bypass automatic type inference.

Similarly, you can choose performance settings by passing a
:class:`ReadOptions` instance to :func:`read_json`.
