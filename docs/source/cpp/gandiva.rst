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

.. default-domain:: cpp
.. highlight:: cpp
.. cpp:namespace:: arrow::compute

===============================
The Gandiva Expression Compiler
===============================

Gandiva is a runtime expression compiler that uses `LLVM`_ to generate
efficient native code for projections and filters on Arrow record batches.
Gandiva only handles projections and filters. For other transformation, see
:ref:`Compute Functions <compute-cpp>`.

Gandiva was designed to take advantage of the Arrow memory format and modern
hardware. Compiling expressions using LLVM allows the execution to be optimized
to the local runtime environment and hardware, including available SIMD
instructions. To minimize optimization overhead, all Gandiva functions are
pre-compiled into LLVM IR (intermediate representation).

.. _LLVM: https://llvm.org/


Building Expressions
====================

Gandiva provides a general expression representation where expressions are
represented by a tree of nodes. The expression trees are built using
:class:`gandiva::TreeExprBuilder`. The leaves of the expression tree are typically
field references, created by :func:`gandiva::TreeExprBuilder::MakeField`, and
literal values, created by :func:`gandiva::TreeExprBuilder::MakeLiteral`. Nodes
can be combined into more complex expression trees using:

 * :func:`gandiva::TreeExprBuilder::MakeFunction` to create a function
   node. :ref:`See available functions below <gandiva-function-list>`.
 * :func:`gandiva::TreeExprBuilder::MakeIf` to create if-else logic.
 * :func:`gandiva::TreeExprBuilder::MakeAnd` and :func:`gandiva::TreeExprBuilder::MakeOr`
   to create boolean expressions. (For "not", use the ``not(bool)`` function in ``MakeFunction``.)
 * :func:`gandiva::TreeExprBuilder::MakeInExpressionInt32` and the other "in expression"
   functions to create set membership tests.

Once an expression tree is built, they are wrapped in either :class:`gandiva::Expression`
or :class:`gandiva::Condition`, depending on how they will be used.
``Expression`` is used in projections while ``Condition`` is used filters.

As an example, here is how to create an Expression representing ``x + 3`` and a
Condition representing ``x < 3``:

.. code-block:: cpp

   auto field_x_raw = arrow::field("x", arrow::int32());
   auto field_x = TreeExprBuilder::MakeField(field_x_raw);
   auto literal_3 = TreeExprBuilder::MakeLiteral(3);
   auto field_result = arrow::field("result", arrow::int32());

   auto add_node = TreeExprBuilder::MakeFunction("add", {field_x, literal_3}, arrow::int32());
   auto expression = TreeExprBuilder::MakeExpression(add_node, field_result);

   auto less_than_node = TreeExprBuilder::MakeFunction("less_than", {field_x, literal_3},
                                                       boolean());
   auto condition = TreeExprBuilder::MakeCondition(less_than_node);

For simpler expressions, there are also convenience functions that allow you to
use functions directly in ``MakeExpression`` and ``MakeCondition``:

.. code-block:: cpp

   auto expression = TreeExprBuilder::MakeExpression("add", {field_x, literal_3}, field_result);

   auto condition = TreeExprBuilder::MakeCondition("less_than", {field_x, literal_3});


Projectors and Filters
======================

Gandiva's two execution kernels are :class:`gandiva::Projector` and
:class:`gandiva::Filter`. ``Projector`` consumes a record batch and projects
into a new record batch. ``Filter`` consumes a record batch and produces a
:class:`gandiva::SelectionVector` containing the indices that matched the condition.

For both ``Projector`` and ``Filter``, optimization of the expression IR happens
when creating instances. They are compiled against a static schema, so the
schema of the record batches must be known at this point.

Continuing with the ``expression`` and ``condition`` created in the previous
section, here is an example of creating a Projector and a Filter:

.. code-block:: cpp

   auto schema = arrow::schema({field_x});
   std::shared_ptr<Projector> projector;
   auto status = Projector::Make(schema, {expression}, &projector);
   ARROW_CHECK_OK(status);

   std::shared_ptr<Filter> filter;
   status = Filter::Make(schema, condition, &filter);
   ARROW_CHECK_OK(status);


Once a Projector or Filter is created, it can be evaluated on Arrow record batches.
These execution kernels are single-threaded on their own, but are designed to be
reused to process record batches in parallel.

Execution is performed with :func:`gandiva::Projector::Evaluate` and
:func:`gandiva::Filter::Evaluate`. Filters produce :class:`gandiva::SelectionVector`,
a vector of row indices that matched the filter condition. When filtering and
projecting record batches, you can pass the selection vector into the projector
so that the projection is only evaluated on matching rows.

Here is an example of evaluating the Filter and Projector created above:

.. code-block:: cpp

   auto pool = arrow::default_memory_pool();
   int num_records = 4;
   auto array = MakeArrowArrayInt32({1, 2, 3, 4}, {true, true, true, true});
   auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array});

   // Just project
   arrow::ArrayVector outputs;
   status = projector->Evaluate(*in_batch, pool, &outputs);
   ARROW_CHECK_OK(status);

   // Evaluate filter
   gandiva::SelectionVector result_indices;
   status = filter->Evaluate(*in_batch, &result_indices);
   ARROW_CHECK_OK(status);

   // Project with filter
   arrow::ArrayVector outputs_filtered;
   status = projector->Evaluate(*in_batch, selection_vector.get(),
                                pool, &outputs_filtered);


.. _gandiva-function-list:

.. TODO: Describe responsibility on Decimal precisions

Available Gandiva Functions
===========================

Common Types
------------

To be succint, we describe the types in the following groups:

 * Integer: ``int8``, ``int16``, ``int32``, ``int64``, ``uint8``, ``uint16``, ``uint32``, ``uint64``
 * Float: ``float``, ``double``
 * Decimal: ``decimal128``
 * Numeric: Integer or Float or Decimal
 * Date: ``date64[ms]``, ``date32[day]``
 * Time: ``time32[ms]``
 * Timestamp: ``timestamp[ms]``


Comparisons
-----------

.. list-table::
  :header-rows: 1

  * - Function names
    - Arity
    - Input types
    - Output type
    - Notes
  * - not
    - Unary
    - ``bool``
    - ``bool``
    -
  * - isnull, isnotnull
    - Unary
    - Any
    - ``bool``
    -
  * - equal, eq, same,
    - Binary
    - Any
    - ``bool``
    - \(1)
  * - is_distinct_from, is_not_distinct_from
    - Binary
    - Any
    - ``bool``
    - \(2)
  * - less_than, less_than_or_equal_to, greater_than, greater_than, greater_than_or_equal_to
    - Binary
    - Any
    - ``bool``
    -

* \(1) ``eq`` and ``same`` are aliases for ``equal``.

* \(2) ``is_not_distinct_from`` is the "NULL-safe" version of ``equal``, meaning it
  will treat two NULL values as equal, while ``equal`` considers NULL values as
  unknown and never equal.


Cast
----


Casts convert values between types. These may raise errors, for example if casting
between numeric types causes overflow or if attempting to cast an invalid date
string to a date type.

.. list-table::
  :header-rows: 1

  * - Function names
    - Arity
    - Input types
    - Output type
    - Notes
  * - castBIGINT
    - Unary
    - ``int32``, ``decimal128``, ``day_time_interval``, ``string``
    - ``int64``
    - \(1)
  * - castINT
    - Unary
    - ``int64``, ``string``
    - ``int32``
    -
  * - castFLOAT4
    - Unary
    - ``int32``, ``int64``, ``double``, ``string``
    - ``float``
    -
  * - castFLOAT8
    - Unary
    - ``int32``, ``int64``, ``float``, ``decimal128``, ``string``
    - ``double``
    -
  * - castDECIMAL, castDECIMALNullOnOverflow
    - Unary
    - ``int32``, ``int64``, Float, ``decimal128``, ``string``
    - ``decimal128``
    -
  * - castDATE
    - Unary
    - ``int64``, ``date32[day]``, ``timestamp[ms]``, ``string``
    - ``date64[ms]``
    - \(2)
  * - castDATE
    - Unary
    - ``int32``
    - ``date32[day]``
    - \(3)
  * - castTIME
    - Unary
    - ``timestamp[ms]``
    - ``time32[ms]``
    -
  * - castTIMESTAMP
    - Unary
    - ``string``, ``date64[ms]``, ``int64``
    - ``timestamp[ms]``
    -
  * - castVARCHAR
    - Binary
    - (Any, ``int64``)
    - ``string``
    - \(4)
  * - castVARBINARY
    - Binary
    - (Any, ``int64``)
    - ``binary``
    - \(4)
  * - castBIT, castBOOLEAN
    - Unary
    - ``string``
    - ``bool``
    - \(5)
  * - to_time
    - Unary
    - Numeric
    - ``time32[ms]``
    - \(6)
  * - to_timestamp
    - Unary
    - Numeric
    - ``timestamp[ms]``
    - \(7)
  * - to_date
    - Unary
    - ``timestamp[ms]``
    - ``date64[ms]``
    - \(7)
  * - to_date
    - Binary
    - (``string``, ``string``)
    - ``date64[ms]``
    - \(8)
  * - to_date
    - Ternary
    - (``string``, ``string``, ``int32``)
    - ``date64[ms]``
    - \(8)
  * - convert_toDOUBLE, convert_toDOUBLE_be, convert_toFLOAT, convert_toFLOAT_be, convert_toINT,
      convert_toINT_be, convert_toBIGINT, convert_toBIGINT_be, convert_toBOOLEAN_BYTE,
      convert_toTIME_EPOCH, convert_toTIME_EPOCH_be, convert_toTIMESTAMP_EPOCH, convert_toTIMESTAMP_EPOCH_be,
      convert_toDATE_EPOCH, convert_toDATE_EPOCH_be, convert_toUTF8
    - Unary
    - ``double``, ``float``, ``int32``, ``int64``, ``bool``, ``string``, ``date[ms]``, ``timestamp[ms]``
    - ``binary``
    - \(9)
  * - convert_fromUTF8, convert_fromutf8
    - Unary
    - ``binary``
    - ``string``
    -
  * - convert_replaceUTF8, convert_replaceutf8
    - Binary
    - (``binary``, ``string``)
    - ``string``
    - \(10)


* \(1) ``castBIGINT(day_time_interval) -> int64`` returns the number of milliseconds
  in interval.
* \(2) ``castDATE(int64) -> date64[ms]`` returns the date using input as milliseconds
  since UNIX epoch 1970-01-01.
* \(3) ``castDATE(int32) -> date32[ms]`` returns the date using input as days since
  UNIX epoch 1970-01-01.
* \(4) For ``castVARCHAR`` and ``castVARBINARY``, the second parameter (of type
  ``int64``) represents the maximum number of bytes to return. If the string
  representation of the value is larger then that specified max, the result will
  be truncated. For example, ``castVARCHAR("12345", 3)`` would return ``123``.
* \(5) ``castBOOLEAN`` is an alias for ``castBIT``. Converts ``"true"`` or ``"1"``
  to ``true`` and ``"false"`` or ``"0"`` to ``false``.
* \(6) ``to_time`` takes a timestamp in seconds and converts into a time, dropping
  the date information.
* \(7) ``to_timestamp`` returns the timestamp using input as milliseconds
  since UNIX epoch 1970-01-01.
* \(8) ``to_date(string, string [, int32])`` parses the first string into a date
  based on the format string specified in the string parameter. The optional ``int32``
  parameter indicates to suppress errors, which is turned on with value ``1``.
* \(9) variants that end in ``_be`` return bytes in big endian order, while main variant
  returns in platform-native endianness.
* \(10) The "replace" variations take a second string parameter which is the
  character to replace any bytes that are not valid Unicode with.


Arithmetic
----------

.. list-table::
  :header-rows: 1

  * - Function names
    - Arity
    - Input types
    - Output type
    - Notes
  * - add
    - Binary
    - Numeric
    - Numeric
    -
  * - subtract
    - Binary
    - Numeric
    - Numeric
    -
  * - multiply
    - Binary
    - Numeric
    - Numeric
    -
  * - divide
    - Binary
    - Numeric
    - Numeric
    -
  * - mod, modulo
    - Binary
    - Integer, ``double``, ``decimal128``
    - Integer, ``double``, ``decimal128``
    - \(1)
  * - div
    - Binary
    - Integer, Float
    - Integer, Float
    - \(2)
  * - bitwise_and, bitwise_or, bitwise_xor
    - Binary
    - Integer
    - Integer
    -
  * - bitwise_not
    - Unary
    - Integer
    - Integer
    -


* \(1) ``modulo`` is an alias for ``mod``.
* \(2) ``div`` performs integer division, which for Integer types is identical
  to ``divide``, but for float types will truncate to the closest integer it is
  not greater than.



Math
----


.. list-table::
  :header-rows: 1

  * - Function names
    - Arity
    - Input types
    - Output type
    - Notes
  * - cbrt, exp, log, log10
    - Unary
    - Integer, Float
    - ``double``
    -
  * - log
    - Binary
    - (Integer or Float, Integer or Float)
    - ``double``
    - \(1)
  * - power, pow
    - Binary
    - (``double``, ``double``)
    - ``double``
    - \(2)
  * - sin, cos, tan, asin, acos, atan, sinh, cosh, tanh, cot, atan2
    - Unary
    - Integer, Float
    - ``double``
    -
  * - radians, degrees
    - Unary
    - Integer, Float
    - ``double``
    - \(3)
  * - random, rand
    - Nullary
    - None
    - ``double``
    - \(4)
  * - random, rand
    - Unary
    - ``int32``
    - ``double``
    - \(4)



* \(1) The binary log function uses the first parameter as the base and the
  second as the operand. In other words ``log(a, b) = log(b) / log(a)``.
* \(2) ``pow`` is an alias for ``power``.
* \(3) ``radians`` converts degrees to radians and ``degrees`` converts the other
  way.
* \(4) ``rand`` is an alias for ``random``. The unary version takes an ``int32``
  seed. Both versions return a 64-bit float in the range of [0, 1).


Rounding
--------

.. list-table::
  :header-rows: 1

  * - Function names
    - Arity
    - Input types
    - Output type
    - Notes
  * - round
    - Unary
    - Numeric
    - Numeric
    -
  * - round
    - Binary
    - (Numeric, ``int32``)
    - Numeric
    - \(1)
  * - abs, ceil, floor
    - Unary
    - Decimal
    - Decimal
    -
  * - truncate, trunc
    - Unary
    - Decimal
    - Decimal
    - \(2)
  * - truncate, trunc
    - Binary
    - (Decimal or ``int64``, ``int32``)
    - ``int32``, Decimal
    - \(2) \(3)

* \(1) The second parameter (of type ``int32``) is the precision, with positive
  values rounding to places right of the decimal and negative to the left. For
  example, ``round(123.456, 2)`` returns ``123.46`` and ``round(123.456, -2)``
  returns ``100.0``.
* \(2) ``trunc`` is an alias for ``truncate``.
* \(3) The second parameter (of type ``int32``) is the precision, which works
  similarly to the precision parameter of ``round``.


Date and Time
-------------

.. list-table::
  :header-rows: 1

  * - Function names
    - Arity
    - Input types
    - Output type
    - Notes
  * - add, date_add
    - Binary
    - Integer, ``date64[ms]``, ``timestamp[ms]``
    - ``date64[ms]``, ``timestamp[ms]``
    - \(1)
  * - subtract, date_sub, date_diff
    - Binary
    - (``date64[ms]`` or ``timestamp[ms]``, Integer)
    - ``date64[ms]``, ``timestamp[ms]``
    - \(1)
  * - extractMillennium, extractCentury, extractDecade, extractYear, extractQuarter,
      extractMonth, extractWeek, extractDay, extractHour, extractMinute, extractSecond,
      extractDoy, extractDow, extractEpoch
    - Unary
    - ``date64[ms]``, ``timestamp[ms]``
    - ``int64``
    - \(2)
  * - date_trunc_Millennium, date_trunc_Century, date_trunc_Decade, date_trunc_Year, date_trunc_Quarter,
      date_trunc_Month, date_trunc_Week, date_trunc_Day, date_trunc_Hour, date_trunc_Minute, date_trunc_Second
    - Unary
    - ``date64[ms]``, ``timestamp[ms]``
    - ``int64``
    -
  * - last_day
    - Unary
    - ``date64[ms]``, ``timestamp[ms]``
    - ``date64[ms]``
    -
  * - months_between
    - Binary
    - ``date64[ms]``, ``timestamp[ms]``
    - ``double``
    -
  * - timestampdiffSecond, timestampdiffMinute, timestampdiffHour, timestampdiffDay,
      timestampdiffWeek, timestampdiffMonth, timestampdiffQuarter, timestampdiffYear
    - Binary
    - ``timestamp[ms]``
    - ``double``
    -
  * - timestampaddSecond, timestampaddMinute, timestampaddHour, timestampaddDay,
      timestampaddWeek, timestampaddMonth, timestampaddQuarter, timestampaddYear
    - Binary
    - ``timestamp[ms]``
    - ``double``
    -


* \(1) In ``add`` and ``subtract``, the integer parameter represents the number of days
  to add or subtract from the give date or timestamp. ``date_add`` is an alias for ``add``
  and ``date_sub`` and ``date_diff`` are aliases for ``subtract``.
* \(2) ``year`` is an alias for ``extractYear``, ``month`` an alias for ``extractMonth``,
  ``weekofyear`` and ``yearweek`` aliases for ``extractWeek``, ``day`` and ``dayofmonth``
  aliases of ``extractDay``, ``hour`` an alias for ``extractHour``, ``minute`` an alias
  for ``extractMinute``, and ``second`` an alias for ``extractSecond``.


String Manipulation
-------------------

.. list-table::
  :header-rows: 1

  * - Function names
    - Arity
    - Input types
    - Output type
    - Notes
  * - bin
    - Unary
    - ``int32``, ``int64``
    - ``string``
    - \(1)
  * - space
    - Unary
    - ``int32``, ``int64``
    - ``string``
    - \(2)
  * - starts_with, ends_with, is_substr
    - Binary
    - ``string``
    - ``bool``
    -
  * - like, ilike
    - Binary
    - ``string``
    - ``bool``
    -
  * - like
    - Ternary
    - ``string``
    - ``bool``
    - \(3)
  * - locate, position
    - Binary
    - ``string``
    - ``int32``
    - \(4)
  * - locate, position
    - Ternary
    - (``string``, ``string``, ``int32``)
    - ``int32``
    - \(4)
  * - octet_length, bit_length
    - Unary
    - ``string``, ``binary``
    - ``int32``
    -
  * - char_length, length
    - Unary
    - ``string``
    - ``int32``
    -
  * - lengthUtf8
    - Unary
    - ``binary``
    - ``int32``
    -
  * - reverse, ltrim, rtrim, btrim
    - Unary
    - ``string``
    - ``string``
    -
  * - ltrim, rtrim, btrim
    - Binary
    - ``string``
    - ``string``
    - \(5)
  * - ascii
    - Unary
    - ``string``
    - ``int32``
    -
  * - base64
    - Unary
    - ``binary``
    - ``string``
    -
  * - unbase64
    - Unary
    - ``string``
    - ``binary``
    -
  * - upper, lower, initcap
    - Unary
    - ``string``
    - ``string``
    -
  * - substr, substring
    - Binary
    - (``string``, ``int64``)
    - ``string``
    - \(6)
  * - substr, substring
    - Ternary
    - (``string``, ``int64``, ``int64``)
    - ``string``
    - \(6)
  * - byte_substr, bytesubstring
    - Ternary
    - (``binary``, ``int32``, ``int32``)
    - ``binary``
    -
  * - left, right
    - Binary
    - (``string``, ``int32``)
    - ``string``
    -
  * - lpad, rpad
    - Binary
    - (``string``, ``int32``)
    - ``string``
   -
  * - lpad, rpad
    - Ternary
    - (``string``, ``int32``, ``string``)
    - ``string``
    -
  * - concat, concatOperator
    - 2 to 10
    - ``string``
    - ``string``
    - \(7)
  * - binary_string
    - Unary
    - ``string``
    - ``binary``
    -
  * - split_part
    - Ternary
    - (``string``, ``string``, ``int32``)
    - ``string``
    -
  * - replace
    - Ternary
    - (``string``, ``string``, ``string``)
    - ``string``
    -


* \(1) ``bin`` converts integers to their binary representation as a string. For
  example, ``bin(7) = "111"``.
* \(2) ``space`` creates a string that is a sequence of space whose length is
  the given integer.
* \(3) ``like`` has a ternary variation where the third parameter is an escape
  character, making it possible to match patterns with ``%`` in them.
* \(4) ``locate`` returns the starting index of the first instance of the first string
  parameter in the second string parameter. Not that the index is 1-indexed. The
  optional ``int32`` argument allows you to provide a start position to skip a portion
  of the string. ``position`` is an alias for ``locate``.
* \(5) The binary variations of ``ltrim``, ``rtrim``, and ``btrim`` take a second
  parameter a string containing the list of characters to trim.
* \(6) ``substr`` returns a substring of the original string, with the integer
  parameters controlling the position and length. In the binary variation, the
  second parameter is the length from the start of the string (if positive) or
  the length from the end of the string (if negative). In the ternary variation,
  the second parameter is the starting position (1-indexed) and the third parameter
  is acts as the offset like the second parameter in the binary variation.
* \(7) ``concat`` treats null inputs as empty strings whereas ``concatOperator``
  returns null if one of the inputs is null

Hash
----

.. list-table::
  :header-rows: 1

  * - Function names
    - Arity
    - Input types
    - Output type
    - Notes
  * - hash
    - Unary
    - Any
    - ``int32``
    - \(1)
  * - hash32, hash32AsDouble
    - Unary
    - Any
    - ``int32``
    - \(2)
  * - hash32
    - Binary
    - (Any, ``int32``)
    - ``int32``
    - \(2) \(3)
  * - hash64, hash64AsDouble
    - Unary
    - Any
    - ``int64``
    - \(2)
  * - hash64
    - Binary
    - (Any, ``int64``)
    - ``int64``
    - \(2) \(3)
  * - hashSHA1, sha1, sha
    - Unary
    - Any
    - ``string``
    - \(4)
  * - hashSHA256, sha256
    - Unary
    - Any
    - ``string``
    - \(5)

* \(1) Uses hash function from C++ ``std:hash``.
* \(2) Uses MurmurHash3. ``hash32`` is an alias for ``hash32AsDouble`` and ``hash64``
  is an alias for ``hash64AsDouble``.
* \(3) Second parameter is a seed.
* \(4) ``sha1`` and ``sha`` are aliases for ``hashSHA1``.
* \(5) ``sha256`` is an alias for ``hashSHA256``.

