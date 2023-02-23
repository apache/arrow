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

.. _compute-cpp:

=================
Compute Functions
=================

The generic Compute API
=======================

.. TODO: describe API and how to invoke compute functions

Functions and function registry
-------------------------------

Functions represent compute operations over inputs of possibly varying
types.  Internally, a function is implemented by one or several
"kernels", depending on the concrete input types (for example, a function
adding values from two inputs can have different kernels depending on
whether the inputs are integral or floating-point).

Functions are stored in a global :class:`FunctionRegistry` where
they can be looked up by name.

Input shapes
------------

Computation inputs are represented as a general :class:`Datum` class,
which is a tagged union of several shapes of data such as :class:`Scalar`,
:class:`Array` and :class:`ChunkedArray`.  Many compute functions support
both array (chunked or not) and scalar inputs, however some will mandate
either.  For example, while ``sort_indices`` requires its first and only
input to be an array.

.. _invoking-compute-functions:

Invoking functions
------------------

Compute functions can be invoked by name using
:func:`arrow::compute::CallFunction`::

   std::shared_ptr<arrow::Array> numbers_array = ...;
   std::shared_ptr<arrow::Scalar> increment = ...;
   arrow::Datum incremented_datum;

   ARROW_ASSIGN_OR_RAISE(incremented_datum,
                         arrow::compute::CallFunction("add", {numbers_array, increment}));
   std::shared_ptr<Array> incremented_array = std::move(incremented_datum).make_array();

(note this example uses implicit conversion from ``std::shared_ptr<Array>``
to ``Datum``)

Many compute functions are also available directly as concrete APIs, here
:func:`arrow::compute::Add`::

   std::shared_ptr<arrow::Array> numbers_array = ...;
   std::shared_ptr<arrow::Scalar> increment = ...;
   arrow::Datum incremented_datum;

   ARROW_ASSIGN_OR_RAISE(incremented_datum,
                         arrow::compute::Add(numbers_array, increment));
   std::shared_ptr<Array> incremented_array = std::move(incremented_datum).make_array();

Some functions accept or require an options structure that determines the
exact semantics of the function::

   ScalarAggregateOptions scalar_aggregate_options;
   scalar_aggregate_options.skip_nulls = false;

   std::shared_ptr<arrow::Array> array = ...;
   arrow::Datum min_max;

   ARROW_ASSIGN_OR_RAISE(min_max,
                         arrow::compute::CallFunction("min_max", {array},
                                                      &scalar_aggregate_options));

   // Unpack struct scalar result (a two-field {"min", "max"} scalar)
   std::shared_ptr<arrow::Scalar> min_value, max_value;
   min_value = min_max.scalar_as<arrow::StructScalar>().value[0];
   max_value = min_max.scalar_as<arrow::StructScalar>().value[1];

However, :ref:`Grouped Aggregations <grouped-aggregations-group-by>` are
not invocable via ``CallFunction``.

.. seealso::
   :doc:`Compute API reference <api/compute>`

Implicit casts
==============

Functions may require conversion of their arguments before execution if a
kernel does not match the argument types precisely. For example comparison
of dictionary encoded arrays is not directly supported by any kernel, but an
implicit cast can be made allowing comparison against the decoded array.

Each function may define implicit cast behaviour as appropriate. For example
comparison and arithmetic kernels require identically typed arguments, and
support execution against differing numeric types by promoting their arguments
to numeric type which can accommodate any value from either input.

.. _common-numeric-type:

Common numeric type
-------------------

The common numeric type of a set of input numeric types is the smallest numeric
type which can accommodate any value of any input. If any input is a floating
point type the common numeric type is the widest floating point type among the
inputs. Otherwise the common numeric type is integral and is signed if any input
is signed. For example:

+-------------------+----------------------+------------------------------------------------+
| Input types       | Common numeric type  | Notes                                          |
+===================+======================+================================================+
| int32, int32      | int32                |                                                |
+-------------------+----------------------+------------------------------------------------+
| int16, int32      | int32                | Max width is 32, promote LHS to int32          |
+-------------------+----------------------+------------------------------------------------+
| uint16, int32     | int32                | One input signed, override unsigned            |
+-------------------+----------------------+------------------------------------------------+
| uint32, int32     | int64                | Widen to accommodate range of uint32           |
+-------------------+----------------------+------------------------------------------------+
| uint16, uint32    | uint32               | All inputs unsigned, maintain unsigned         |
+-------------------+----------------------+------------------------------------------------+
| int16, uint32     | int64                |                                                |
+-------------------+----------------------+------------------------------------------------+
| uint64, int16     | int64                | int64 cannot accommodate all uint64 values     |
+-------------------+----------------------+------------------------------------------------+
| float32, int32    | float32              | Promote RHS to float32                         |
+-------------------+----------------------+------------------------------------------------+
| float32, float64  | float64              |                                                |
+-------------------+----------------------+------------------------------------------------+
| float32, int64    | float32              | int64 is wider, still promotes to float32      |
+-------------------+----------------------+------------------------------------------------+

In particulary, note that comparing a ``uint64`` column to an ``int16`` column
may emit an error if one of the ``uint64`` values cannot be expressed as the
common type ``int64`` (for example, ``2 ** 63``).

.. _compute-function-list:

Available functions
===================

Type categories
---------------

To avoid exhaustively listing supported types, the tables below use a number
of general type categories:

* "Numeric": Integer types (Int8, etc.) and Floating-point types (Float32,
  Float64, sometimes Float16).  Some functions also accept Decimal128 and
  Decimal256 input.

* "Temporal": Date types (Date32, Date64), Time types (Time32, Time64),
  Timestamp, Duration, Interval.

* "Binary-like": Binary, LargeBinary, sometimes also FixedSizeBinary.

* "String-like": String, LargeString.

* "List-like": List, LargeList, sometimes also FixedSizeList.

* "Nested": List-likes (including FixedSizeList), Struct, Union, and
  related types like Map.

If you are unsure whether a function supports a concrete input type, we
recommend you try it out.  Unsupported input types return a ``TypeError``
:class:`Status`.

.. _aggregation-option-list:

Aggregations
------------

Scalar aggregations operate on a (chunked) array or scalar value and reduce
the input to a single output value.

+--------------------+---------+------------------+------------------------+----------------------------------+-------+
| Function name      | Arity   | Input types      | Output type            | Options class                    | Notes |
+====================+=========+==================+========================+==================================+=======+
| all                | Unary   | Boolean          | Scalar Boolean         | :struct:`ScalarAggregateOptions` | \(1)  |
+--------------------+---------+------------------+------------------------+----------------------------------+-------+
| any                | Unary   | Boolean          | Scalar Boolean         | :struct:`ScalarAggregateOptions` | \(1)  |
+--------------------+---------+------------------+------------------------+----------------------------------+-------+
| approximate_median | Unary   | Numeric          | Scalar Float64         | :struct:`ScalarAggregateOptions` |       |
+--------------------+---------+------------------+------------------------+----------------------------------+-------+
| count              | Unary   | Any              | Scalar Int64           | :struct:`CountOptions`           | \(2)  |
+--------------------+---------+------------------+------------------------+----------------------------------+-------+
| count_all          | Nullary |                  | Scalar Int64           |                                  |       |
+--------------------+---------+------------------+------------------------+----------------------------------+-------+
| count_distinct     | Unary   | Non-nested types | Scalar Int64           | :struct:`CountOptions`           | \(2)  |
+--------------------+---------+------------------+------------------------+----------------------------------+-------+
| index              | Unary   | Any              | Scalar Int64           | :struct:`IndexOptions`           | \(3)  |
+--------------------+---------+------------------+------------------------+----------------------------------+-------+
| max                | Unary   | Non-nested types | Scalar Input type      | :struct:`ScalarAggregateOptions` |       |
+--------------------+---------+------------------+------------------------+----------------------------------+-------+
| mean               | Unary   | Numeric          | Scalar Decimal/Float64 | :struct:`ScalarAggregateOptions` | \(4)  |
+--------------------+---------+------------------+------------------------+----------------------------------+-------+
| min                | Unary   | Non-nested types | Scalar Input type      | :struct:`ScalarAggregateOptions` |       |
+--------------------+---------+------------------+------------------------+----------------------------------+-------+
| min_max            | Unary   | Non-nested types | Scalar Struct          | :struct:`ScalarAggregateOptions` | \(5)  |
+--------------------+---------+------------------+------------------------+----------------------------------+-------+
| mode               | Unary   | Numeric          | Struct                 | :struct:`ModeOptions`            | \(6)  |
+--------------------+---------+------------------+------------------------+----------------------------------+-------+
| product            | Unary   | Numeric          | Scalar Numeric         | :struct:`ScalarAggregateOptions` | \(7)  |
+--------------------+---------+------------------+------------------------+----------------------------------+-------+
| quantile           | Unary   | Numeric          | Scalar Numeric         | :struct:`QuantileOptions`        | \(8)  |
+--------------------+---------+------------------+------------------------+----------------------------------+-------+
| stddev             | Unary   | Numeric          | Scalar Float64         | :struct:`VarianceOptions`        | \(9)  |
+--------------------+---------+------------------+------------------------+----------------------------------+-------+
| sum                | Unary   | Numeric          | Scalar Numeric         | :struct:`ScalarAggregateOptions` | \(7)  |
+--------------------+---------+------------------+------------------------+----------------------------------+-------+
| tdigest            | Unary   | Numeric          | Float64                | :struct:`TDigestOptions`         | \(10) |
+--------------------+---------+------------------+------------------------+----------------------------------+-------+
| variance           | Unary   | Numeric          | Scalar Float64         | :struct:`VarianceOptions`        | \(9)  |
+--------------------+---------+------------------+------------------------+----------------------------------+-------+

* \(1) If null values are taken into account, by setting the
  ScalarAggregateOptions parameter skip_nulls = false, then `Kleene logic`_
  logic is applied. The min_count option is not respected.

* \(2) CountMode controls whether only non-null values are counted (the
  default), only null values are counted, or all values are counted.

* \(3) Returns -1 if the value is not found. The index of a null value
  is always -1, regardless of whether there are nulls in the input.

* \(4) For decimal inputs, the resulting decimal will have the same
  precision and scale. The result is rounded away from zero.

* \(5) Output is a ``{"min": input type, "max": input type}`` Struct.

  Of the interval types, only the month interval is supported, as the day-time
  and month-day-nano types are not sortable.

* \(6) Output is an array of ``{"mode": input type, "count": Int64}`` Struct.
  It contains the *N* most common elements in the input, in descending
  order, where *N* is given in :member:`ModeOptions::n`.
  If two values have the same count, the smallest one comes first.
  Note that the output can have less than *N* elements if the input has
  less than *N* distinct values.

* \(7) Output is Int64, UInt64, Float64, or Decimal128/256, depending on the
  input type.

* \(8) Output is Float64 or input type, depending on QuantileOptions.

* \(9) Decimal arguments are cast to Float64 first.

* \(10) tdigest/t-digest computes approximate quantiles, and so only needs a
  fixed amount of memory. See the `reference implementation
  <https://github.com/tdunning/t-digest>`_ for details.

  Decimal arguments are cast to Float64 first.

.. _grouped-aggregations-group-by:

Grouped Aggregations ("group by")
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Grouped aggregations are not directly invokable, but are used as part of a
SQL-style "group by" operation. Like scalar aggregations, grouped aggregations
reduce multiple input values to a single output value. Instead of aggregating
all values of the input, however, grouped aggregations partition the input
values on some set of "key" columns, then aggregate each group individually,
emitting one output value per input group.

As an example, for the following table:

+------------------+-----------------+
| Column ``key``   | Column ``x``    |
+==================+=================+
| "a"              | 2               |
+------------------+-----------------+
| "a"              | 5               |
+------------------+-----------------+
| "b"              | null            |
+------------------+-----------------+
| "b"              | null            |
+------------------+-----------------+
| null             | null            |
+------------------+-----------------+
| null             | 9               |
+------------------+-----------------+

we can compute a sum of the column ``x``, grouped on the column ``key``.
This gives us three groups, with the following results. Note that null is
treated as a distinct key value.

+------------------+-----------------------+
| Column ``key``   | Column ``sum(x)``     |
+==================+=======================+
| "a"              | 7                     |
+------------------+-----------------------+
| "b"              | null                  |
+------------------+-----------------------+
| null             | 9                     |
+------------------+-----------------------+

The supported aggregation functions are as follows. All function names are
prefixed with ``hash_``, which differentiates them from their scalar
equivalents above and reflects how they are implemented internally.

+-------------------------+---------+------------------------------------+------------------------+----------------------------------+-----------+
| Function name           | Arity   | Input types                        | Output type            | Options class                    | Notes     |
+=========================+=========+====================================+========================+==================================+===========+
| hash_all                | Unary   | Boolean                            | Boolean                | :struct:`ScalarAggregateOptions` | \(1)      |
+-------------------------+---------+------------------------------------+------------------------+----------------------------------+-----------+
| hash_any                | Unary   | Boolean                            | Boolean                | :struct:`ScalarAggregateOptions` | \(1)      |
+-------------------------+---------+------------------------------------+------------------------+----------------------------------+-----------+
| hash_approximate_median | Unary   | Numeric                            | Float64                | :struct:`ScalarAggregateOptions` |           |
+-------------------------+---------+------------------------------------+------------------------+----------------------------------+-----------+
| hash_count              | Unary   | Any                                | Int64                  | :struct:`CountOptions`           | \(2)      |
+-------------------------+---------+------------------------------------+------------------------+----------------------------------+-----------+
| hash_count_all          | Nullary |                                    | Int64                  |                                  |           |
+-------------------------+---------+------------------------------------+------------------------+----------------------------------+-----------+
| hash_count_distinct     | Unary   | Any                                | Int64                  | :struct:`CountOptions`           | \(2)      |
+-------------------------+---------+------------------------------------+------------------------+----------------------------------+-----------+
| hash_distinct           | Unary   | Any                                | List of input type     | :struct:`CountOptions`           | \(2) \(3) |
+-------------------------+---------+------------------------------------+------------------------+----------------------------------+-----------+
| hash_list               | Unary   | Any                                | List of input type     |                                  | \(3)      |
+-------------------------+---------+------------------------------------+------------------------+----------------------------------+-----------+
| hash_max                | Unary   | Non-nested, non-binary/string-like | Input type             | :struct:`ScalarAggregateOptions` |           |
+-------------------------+---------+------------------------------------+------------------------+----------------------------------+-----------+
| hash_mean               | Unary   | Numeric                            | Decimal/Float64        | :struct:`ScalarAggregateOptions` | \(4)      |
+-------------------------+---------+------------------------------------+------------------------+----------------------------------+-----------+
| hash_min                | Unary   | Non-nested, non-binary/string-like | Input type             | :struct:`ScalarAggregateOptions` |           |
+-------------------------+---------+------------------------------------+------------------------+----------------------------------+-----------+
| hash_min_max            | Unary   | Non-nested types                   | Struct                 | :struct:`ScalarAggregateOptions` | \(5)      |
+-------------------------+---------+------------------------------------+------------------------+----------------------------------+-----------+
| hash_one                | Unary   | Any                                | Input type             |                                  | \(6)      |
+-------------------------+---------+------------------------------------+------------------------+----------------------------------+-----------+
| hash_product            | Unary   | Numeric                            | Numeric                | :struct:`ScalarAggregateOptions` | \(7)      |
+-------------------------+---------+------------------------------------+------------------------+----------------------------------+-----------+
| hash_stddev             | Unary   | Numeric                            | Float64                | :struct:`VarianceOptions`        | \(8)      |
+-------------------------+---------+------------------------------------+------------------------+----------------------------------+-----------+
| hash_sum                | Unary   | Numeric                            | Numeric                | :struct:`ScalarAggregateOptions` | \(7)      |
+-------------------------+---------+------------------------------------+------------------------+----------------------------------+-----------+
| hash_tdigest            | Unary   | Numeric                            | FixedSizeList[Float64] | :struct:`TDigestOptions`         | \(9)      |
+-------------------------+---------+------------------------------------+------------------------+----------------------------------+-----------+
| hash_variance           | Unary   | Numeric                            | Float64                | :struct:`VarianceOptions`        | \(8)      |
+-------------------------+---------+------------------------------------+------------------------+----------------------------------+-----------+

* \(1) If null values are taken into account, by setting the
  :member:`ScalarAggregateOptions::skip_nulls` to false, then `Kleene logic`_
  logic is applied. The min_count option is not respected.

* \(2) CountMode controls whether only non-null values are counted
  (the default), only null values are counted, or all values are
  counted. For hash_distinct, it instead controls whether null values
  are emitted. This never affects the grouping keys, only group values
  (i.e. you may get a group where the key is null).

* \(3) ``hash_distinct`` and ``hash_list`` gather the grouped values
  into a list array.

* \(4) For decimal inputs, the resulting decimal will have the same
  precision and scale. The result is rounded away from zero.

* \(5) Output is a ``{"min": input type, "max": input type}`` Struct array.

  Of the interval types, only the month interval is supported, as the day-time
  and month-day-nano types are not sortable.

* \(6) ``hash_one`` returns one arbitrary value from the input for each
  group. The function is biased towards non-null values: if there is at least
  one non-null value for a certain group, that value is returned, and only if
  all the values are ``null`` for the group will the function return ``null``.

* \(7) Output is Int64, UInt64, Float64, or Decimal128/256, depending on the
  input type.

* \(8) Decimal arguments are cast to Float64 first.

* \(9) T-digest computes approximate quantiles, and so only needs a
  fixed amount of memory. See the `reference implementation
  <https://github.com/tdunning/t-digest>`_ for details.

  Decimal arguments are cast to Float64 first.

Element-wise ("scalar") functions
---------------------------------

All element-wise functions accept both arrays and scalars as input.  The
semantics for unary functions are as follow:

* scalar inputs produce a scalar output
* array inputs produce an array output

Binary functions have the following semantics (which is sometimes called
"broadcasting" in other systems such as NumPy):

* ``(scalar, scalar)`` inputs produce a scalar output
* ``(array, array)`` inputs produce an array output (and both inputs must
  be of the same length)
* ``(scalar, array)`` and ``(array, scalar)`` produce an array output.
  The scalar input is handled as if it were an array of the same length N
  as the other input, with the same value repeated N times.

Arithmetic functions
~~~~~~~~~~~~~~~~~~~~

These functions expect inputs of numeric type and apply a given arithmetic
operation to each element(s) gathered from the input(s).  If any of the
input element(s) is null, the corresponding output element is null.
For binary functions, input(s) will be cast to the
:ref:`common numeric type <common-numeric-type>`
(and dictionary decoded, if applicable) before the operation is applied.

The default variant of these functions does not detect overflow (the result
then typically wraps around).  Most functions are also available in an
overflow-checking variant, suffixed ``_checked``, which returns
an ``Invalid`` :class:`Status` when overflow is detected.

For functions which support decimal inputs (currently ``add``, ``subtract``,
``multiply``, and ``divide`` and their checked variants), decimals of different
precisions/scales will be promoted appropriately. Mixed decimal and
floating-point arguments will cast all arguments to floating-point, while mixed
decimal and integer arguments will cast all arguments to decimals.
Mixed time resolution temporal inputs will be cast to finest input resolution.

+------------------+--------+-------------------------+----------------------+-------+
| Function name    | Arity  | Input types             | Output type          | Notes |
+==================+========+=========================+======================+=======+
| abs              | Unary  | Numeric                 | Numeric              |       |
+------------------+--------+-------------------------+----------------------+-------+
| abs_checked      | Unary  | Numeric                 | Numeric              |       |
+------------------+--------+-------------------------+----------------------+-------+
| add              | Binary | Numeric/Temporal        | Numeric/Temporal     | \(1)  |
+------------------+--------+-------------------------+----------------------+-------+
| add_checked      | Binary | Numeric/Temporal        | Numeric/Temporal     | \(1)  |
+------------------+--------+-------------------------+----------------------+-------+
| divide           | Binary | Numeric/Temporal        | Numeric/Temporal     | \(1)  |
+------------------+--------+-------------------------+----------------------+-------+
| divide_checked   | Binary | Numeric/Temporal        | Numeric/Temporal     | \(1)  |
+------------------+--------+-------------------------+----------------------+-------+
| exp              | Unary  | Numeric                 | Float32/Float64      |       |
+------------------+--------+-------------------------+----------------------+-------+
| multiply         | Binary | Numeric/Temporal        | Numeric/Temporal     | \(1)  |
+------------------+--------+-------------------------+----------------------+-------+
| multiply_checked | Binary | Numeric/Temporal        | Numeric/Temporal     | \(1)  |
+------------------+--------+-------------------------+----------------------+-------+
| negate           | Unary  | Numeric                 | Numeric              |       |
+------------------+--------+-------------------------+----------------------+-------+
| negate_checked   | Unary  | Signed Numeric          | Signed Numeric       |       |
+------------------+--------+-------------------------+----------------------+-------+
| power            | Binary | Numeric                 | Numeric              |       |
+------------------+--------+-------------------------+----------------------+-------+
| power_checked    | Binary | Numeric                 | Numeric              |       |
+------------------+--------+-------------------------+----------------------+-------+
| sign             | Unary  | Numeric                 | Int8/Float32/Float64 | \(2)  |
+------------------+--------+-------------------------+----------------------+-------+
| sqrt             | Unary  | Numeric                 | Numeric              |       |
+------------------+--------+-------------------------+----------------------+-------+
| sqrt_checked     | Unary  | Numeric                 | Numeric              |       |
+------------------+--------+-------------------------+----------------------+-------+
| subtract         | Binary | Numeric/Temporal        | Numeric/Temporal     | \(1)  |
+------------------+--------+-------------------------+----------------------+-------+
| subtract_checked | Binary | Numeric/Temporal        | Numeric/Temporal     | \(1)  |
+------------------+--------+-------------------------+----------------------+-------+

* \(1) Precision and scale of computed DECIMAL results

  +------------+---------------------------------------------+
  | Operation  | Result precision and scale                  |
  +============+=============================================+
  | | add      | | scale = max(s1, s2)                       |
  | | subtract | | precision = max(p1-s1, p2-s2) + 1 + scale |
  +------------+---------------------------------------------+
  | multiply   | | scale = s1 + s2                           |
  |            | | precision = p1 + p2 + 1                   |
  +------------+---------------------------------------------+
  | divide     | | scale = max(4, s1 + p2 - s2 + 1)          |
  |            | | precision = p1 - s1 + s2 + scale          |
  +------------+---------------------------------------------+

  It's compatible with Redshift's decimal promotion rules. All decimal digits
  are preserved for `add`, `subtract` and `multiply` operations. The result
  precision of `divide` is at least the sum of precisions of both operands with
  enough scale kept. Error is returned if the result precision is beyond the
  decimal value range.

* \(2) Output is any of (-1,1) for nonzero inputs and 0 for zero input.  NaN
  values return NaN.  Integral and decimal values return signedness as Int8 and
  floating-point values return it with the same type as the input values.

Bit-wise functions
~~~~~~~~~~~~~~~~~~

+--------------------------+------------+--------------------+---------------------+
| Function name            | Arity      | Input types        | Output type         |
+==========================+============+====================+=====================+
| bit_wise_and             | Binary     | Numeric            | Numeric             |
+--------------------------+------------+--------------------+---------------------+
| bit_wise_not             | Unary      | Numeric            | Numeric             |
+--------------------------+------------+--------------------+---------------------+
| bit_wise_or              | Binary     | Numeric            | Numeric             |
+--------------------------+------------+--------------------+---------------------+
| bit_wise_xor             | Binary     | Numeric            | Numeric             |
+--------------------------+------------+--------------------+---------------------+
| shift_left               | Binary     | Numeric            | Numeric             |
+--------------------------+------------+--------------------+---------------------+
| shift_left_checked       | Binary     | Numeric            | Numeric (1)         |
+--------------------------+------------+--------------------+---------------------+
| shift_right              | Binary     | Numeric            | Numeric             |
+--------------------------+------------+--------------------+---------------------+
| shift_right_checked      | Binary     | Numeric            | Numeric (1)         |
+--------------------------+------------+--------------------+---------------------+

* \(1) An error is emitted if the shift amount (i.e. the second input) is
  out of bounds for the data type.  However, an overflow when shifting the
  first input is not error (truncated bits are silently discarded).

Rounding functions
~~~~~~~~~~~~~~~~~~

Rounding functions displace numeric inputs to an approximate value with a simpler
representation based on the rounding criterion.

+-------------------+------------+-------------+-------------------------+----------------------------------+--------+
| Function name     | Arity      | Input types | Output type             | Options class                    | Notes  |
+===================+============+=============+=========================+==================================+========+
| ceil              | Unary      | Numeric     | Float32/Float64/Decimal |                                  |        |
+-------------------+------------+-------------+-------------------------+----------------------------------+--------+
| floor             | Unary      | Numeric     | Float32/Float64/Decimal |                                  |        |
+-------------------+------------+-------------+-------------------------+----------------------------------+--------+
| round             | Unary      | Numeric     | Float32/Float64/Decimal | :struct:`RoundOptions`           | (1)(2) |
+-------------------+------------+-------------+-------------------------+----------------------------------+--------+
| round_to_multiple | Unary      | Numeric     | Float32/Float64/Decimal | :struct:`RoundToMultipleOptions` | (1)(3) |
+-------------------+------------+-------------+-------------------------+----------------------------------+--------+
| trunc             | Unary      | Numeric     | Float32/Float64/Decimal |                                  |        |
+-------------------+------------+-------------+-------------------------+----------------------------------+--------+

* \(1) Output value is a 64-bit floating-point for integral inputs and the
  retains the same type for floating-point and decimal inputs.  By default
  rounding functions displace a value to the nearest integer using
  HALF_TO_EVEN to resolve ties.  Options are available to control the rounding
  criterion.  Both ``round`` and ``round_to_multiple`` have the ``round_mode``
  option to set the rounding mode.
* \(2) Round to a number of digits where the ``ndigits`` option of
  :struct:`RoundOptions` specifies the rounding precision in terms of number
  of digits.  A negative value corresponds to digits in the non-fractional
  part.  For example, -2 corresponds to rounding to the nearest multiple of
  100 (zeroing the ones and tens digits).  Default value of ``ndigits`` is 0
  which rounds to the nearest integer.
* \(3) Round to a multiple where the ``multiple`` option of
  :struct:`RoundToMultipleOptions` specifies the rounding scale.  The rounding
  multiple has to be a positive value.  For example, 100 corresponds to
  rounding to the nearest multiple of 100 (zeroing the ones and tens digits).
  Default value of ``multiple`` is 1 which rounds to the nearest integer.

For ``round`` and ``round_to_multiple``, the following rounding modes are available.
Tie-breaking modes are prefixed with HALF and round non-ties to the nearest integer.
The example values are given for default values of ``ndigits`` and ``multiple``.

+-----------------------+--------------------------------------------------------------+---------------------------+
| ``round_mode``        | Operation performed                                          | Example values            |
+=======================+==============================================================+===========================+
| DOWN                  | Round to nearest integer less than or equal in magnitude;    | 3.2 -> 3, 3.7 -> 3,       |
|                       | also known as ``floor(x)``                                   | -3.2 -> -4, -3.7 -> -4    |
+-----------------------+--------------------------------------------------------------+---------------------------+
| UP                    | Round to nearest integer greater than or equal in magnitude; | 3.2 -> 4, 3.7 -> 4,       |
|                       | also known as ``ceil(x)``                                    | -3.2 -> -3, -3.7 -> -3    |
+-----------------------+--------------------------------------------------------------+---------------------------+
| TOWARDS_ZERO          | Get the integral part without fractional digits;             | 3.2 -> 3, 3.7 -> 3,       |
|                       | also known as ``trunc(x)``                                   | -3.2 -> -3, -3.7 -> -3    |
+-----------------------+--------------------------------------------------------------+---------------------------+
| TOWARDS_INFINITY      | Round negative values with ``DOWN`` rule,                    | 3.2 -> 4, 3.7 -> 4,       |
|                       | round positive values with ``UP`` rule                       | -3.2 -> -4, -3.7 -> -4    |
+-----------------------+--------------------------------------------------------------+---------------------------+
| HALF_DOWN             | Round ties with ``DOWN`` rule                                | 3.5 -> 3, 4.5 -> 4,       |
|                       |                                                              | -3.5 -> -4, -4.5 -> -5    |
+-----------------------+--------------------------------------------------------------+---------------------------+
| HALF_UP               | Round ties with ``UP`` rule                                  | 3.5 -> 4, 4.5 -> 5,       |
|                       |                                                              | -3.5 -> -3, -4.5 -> -4    |
+-----------------------+--------------------------------------------------------------+---------------------------+
| HALF_TOWARDS_ZERO     | Round ties with ``TOWARDS_ZERO`` rule                        | 3.5 -> 3, 4.5 -> 4,       |
|                       |                                                              | -3.5 -> -3, -4.5 -> -4    |
+-----------------------+--------------------------------------------------------------+---------------------------+
| HALF_TOWARDS_INFINITY | Round ties with ``TOWARDS_INFINITY`` rule                    | 3.5 -> 4, 4.5 -> 5,       |
|                       |                                                              | -3.5 -> -4, -4.5 -> -5    |
+-----------------------+--------------------------------------------------------------+---------------------------+
| HALF_TO_EVEN          | Round ties to nearest even integer                           | 3.5 -> 4, 4.5 -> 4,       |
|                       |                                                              | -3.5 -> -4, -4.5 -> -4    |
+-----------------------+--------------------------------------------------------------+---------------------------+
| HALF_TO_ODD           | Round ties to nearest odd integer                            | 3.5 -> 3, 4.5 -> 5,       |
|                       |                                                              | -3.5 -> -3, -4.5 -> -5    |
+-----------------------+--------------------------------------------------------------+---------------------------+

The following table gives examples of how ``ndigits`` (for the ``round``
function) and ``multiple`` (for ``round_to_multiple``) influence the operance
performed, respectively.

+--------------------+-------------------+---------------------------+
| Round ``multiple`` | Round ``ndigits`` | Operation performed       |
+====================+===================+===========================+
| 1                  | 0                 | Round to integer          |
+--------------------+-------------------+---------------------------+
| 0.001              | 3                 | Round to 3 decimal places |
+--------------------+-------------------+---------------------------+
| 10                 | -1                | Round to multiple of 10   |
+--------------------+-------------------+---------------------------+
| 2                  | NA                | Round to multiple of 2    |
+--------------------+-------------------+---------------------------+

Logarithmic functions
~~~~~~~~~~~~~~~~~~~~~

Logarithmic functions are also supported, and also offer ``_checked``
variants that check for domain errors if needed.

Decimal values are accepted, but are cast to Float64 first.

+--------------------------+------------+-------------------------+---------------------+
| Function name            | Arity      | Input types             | Output type         |
+==========================+============+=========================+=====================+
| ln                       | Unary      | Float32/Float64/Decimal | Float32/Float64     |
+--------------------------+------------+-------------------------+---------------------+
| ln_checked               | Unary      | Float32/Float64/Decimal | Float32/Float64     |
+--------------------------+------------+-------------------------+---------------------+
| log10                    | Unary      | Float32/Float64/Decimal | Float32/Float64     |
+--------------------------+------------+-------------------------+---------------------+
| log10_checked            | Unary      | Float32/Float64/Decimal | Float32/Float64     |
+--------------------------+------------+-------------------------+---------------------+
| log1p                    | Unary      | Float32/Float64/Decimal | Float32/Float64     |
+--------------------------+------------+-------------------------+---------------------+
| log1p_checked            | Unary      | Float32/Float64/Decimal | Float32/Float64     |
+--------------------------+------------+-------------------------+---------------------+
| log2                     | Unary      | Float32/Float64/Decimal | Float32/Float64     |
+--------------------------+------------+-------------------------+---------------------+
| log2_checked             | Unary      | Float32/Float64/Decimal | Float32/Float64     |
+--------------------------+------------+-------------------------+---------------------+
| logb                     | Binary     | Float32/Float64/Decimal | Float32/Float64     |
+--------------------------+------------+-------------------------+---------------------+
| logb_checked             | Binary     | Float32/Float64/Decimal | Float32/Float64     |
+--------------------------+------------+-------------------------+---------------------+

Trigonometric functions
~~~~~~~~~~~~~~~~~~~~~~~

Trigonometric functions are also supported, and also offer ``_checked``
variants that check for domain errors if needed.

Decimal values are accepted, but are cast to Float64 first.

+--------------------------+------------+-------------------------+---------------------+
| Function name            | Arity      | Input types             | Output type         |
+==========================+============+=========================+=====================+
| acos                     | Unary      | Float32/Float64/Decimal | Float32/Float64     |
+--------------------------+------------+-------------------------+---------------------+
| acos_checked             | Unary      | Float32/Float64/Decimal | Float32/Float64     |
+--------------------------+------------+-------------------------+---------------------+
| asin                     | Unary      | Float32/Float64/Decimal | Float32/Float64     |
+--------------------------+------------+-------------------------+---------------------+
| asin_checked             | Unary      | Float32/Float64/Decimal | Float32/Float64     |
+--------------------------+------------+-------------------------+---------------------+
| atan                     | Unary      | Float32/Float64/Decimal | Float32/Float64     |
+--------------------------+------------+-------------------------+---------------------+
| atan2                    | Binary     | Float32/Float64/Decimal | Float32/Float64     |
+--------------------------+------------+-------------------------+---------------------+
| cos                      | Unary      | Float32/Float64/Decimal | Float32/Float64     |
+--------------------------+------------+-------------------------+---------------------+
| cos_checked              | Unary      | Float32/Float64/Decimal | Float32/Float64     |
+--------------------------+------------+-------------------------+---------------------+
| sin                      | Unary      | Float32/Float64/Decimal | Float32/Float64     |
+--------------------------+------------+-------------------------+---------------------+
| sin_checked              | Unary      | Float32/Float64/Decimal | Float32/Float64     |
+--------------------------+------------+-------------------------+---------------------+
| tan                      | Unary      | Float32/Float64/Decimal | Float32/Float64     |
+--------------------------+------------+-------------------------+---------------------+
| tan_checked              | Unary      | Float32/Float64/Decimal | Float32/Float64     |
+--------------------------+------------+-------------------------+---------------------+

Comparisons
~~~~~~~~~~~

These functions expect two inputs of numeric type (in which case they will be
cast to the :ref:`common numeric type <common-numeric-type>` before comparison),
or two inputs of Binary- or String-like types, or two inputs of Temporal types.
If any input is dictionary encoded it will be expanded for the purposes of
comparison. If any of the input elements in a pair is null, the corresponding
output element is null. Decimal arguments will be promoted in the same way as
for ``add`` and ``subtract``.

+----------------+------------+---------------------------------------------+---------------------+
| Function names | Arity      | Input types                                 | Output type         |
+================+============+=============================================+=====================+
| equal          | Binary     | Numeric, Temporal, Binary- and String-like  | Boolean             |
+----------------+------------+---------------------------------------------+---------------------+
| greater        | Binary     | Numeric, Temporal, Binary- and String-like  | Boolean             |
+----------------+------------+---------------------------------------------+---------------------+
| greater_equal  | Binary     | Numeric, Temporal, Binary- and String-like  | Boolean             |
+----------------+------------+---------------------------------------------+---------------------+
| less           | Binary     | Numeric, Temporal, Binary- and String-like  | Boolean             |
+----------------+------------+---------------------------------------------+---------------------+
| less_equal     | Binary     | Numeric, Temporal, Binary- and String-like  | Boolean             |
+----------------+------------+---------------------------------------------+---------------------+
| not_equal      | Binary     | Numeric, Temporal, Binary- and String-like  | Boolean             |
+----------------+------------+---------------------------------------------+---------------------+

These functions take any number of inputs of numeric type (in which case they
will be cast to the :ref:`common numeric type <common-numeric-type>` before
comparison) or of temporal types. If any input is dictionary encoded it will be
expanded for the purposes of comparison.

+------------------+------------+---------------------------------------------+---------------------+---------------------------------------+-------+
| Function names   | Arity      | Input types                                 | Output type         | Options class                         | Notes |
+==================+============+=============================================+=====================+=======================================+=======+
| max_element_wise | Varargs    | Numeric, Temporal, Binary- and String-like  | Numeric or Temporal | :struct:`ElementWiseAggregateOptions` | \(1)  |
+------------------+------------+---------------------------------------------+---------------------+---------------------------------------+-------+
| min_element_wise | Varargs    | Numeric, Temporal, Binary- and String-like  | Numeric or Temporal | :struct:`ElementWiseAggregateOptions` | \(1)  |
+------------------+------------+---------------------------------------------+---------------------+---------------------------------------+-------+

* \(1) By default, nulls are skipped (but the kernel can be configured to propagate nulls).
  For floating point values, NaN will be taken over null but not over any other value.
  For binary- and string-like values, only identical type parameters are supported.

Logical functions
~~~~~~~~~~~~~~~~~~

The normal behaviour for these functions is to emit a null if any of the
inputs is null (similar to the semantics of ``NaN`` in floating-point
computations).

Some of them are also available in a `Kleene logic`_ variant (suffixed
``_kleene``) where null is taken to mean "undefined".  This is the
interpretation of null used in SQL systems as well as R and Julia,
for example.

For the Kleene logic variants, therefore:

* "true AND null", "null AND true" give "null" (the result is undefined)
* "true OR null", "null OR true" give "true"
* "false AND null", "null AND false" give "false"
* "false OR null", "null OR false" give "null" (the result is undefined)

+--------------------------+------------+--------------------+---------------------+
| Function name            | Arity      | Input types        | Output type         |
+==========================+============+====================+=====================+
| and                      | Binary     | Boolean            | Boolean             |
+--------------------------+------------+--------------------+---------------------+
| and_kleene               | Binary     | Boolean            | Boolean             |
+--------------------------+------------+--------------------+---------------------+
| and_not                  | Binary     | Boolean            | Boolean             |
+--------------------------+------------+--------------------+---------------------+
| and_not_kleene           | Binary     | Boolean            | Boolean             |
+--------------------------+------------+--------------------+---------------------+
| invert                   | Unary      | Boolean            | Boolean             |
+--------------------------+------------+--------------------+---------------------+
| or                       | Binary     | Boolean            | Boolean             |
+--------------------------+------------+--------------------+---------------------+
| or_kleene                | Binary     | Boolean            | Boolean             |
+--------------------------+------------+--------------------+---------------------+
| xor                      | Binary     | Boolean            | Boolean             |
+--------------------------+------------+--------------------+---------------------+

.. _Kleene logic: https://en.wikipedia.org/wiki/Three-valued_logic#Kleene_and_Priest_logics

String predicates
~~~~~~~~~~~~~~~~~

These functions classify the input string elements according to their character
contents.  An empty string element emits false in the output.  For ASCII
variants of the functions (prefixed ``ascii_``), a string element with non-ASCII
characters emits false in the output.

The first set of functions operates on a character-per-character basis,
and emit true in the output if the input contains only characters of a
given class:

+--------------------+-------+-------------+-------------+-------------------------+-------+
| Function name      | Arity | Input types | Output type | Matched character class | Notes |
+====================+=======+=============+=============+=========================+=======+
| ascii_is_alnum     | Unary | String-like | Boolean     | Alphanumeric ASCII      |       |
+--------------------+-------+-------------+-------------+-------------------------+-------+
| ascii_is_alpha     | Unary | String-like | Boolean     | Alphabetic ASCII        |       |
+--------------------+-------+-------------+-------------+-------------------------+-------+
| ascii_is_decimal   | Unary | String-like | Boolean     | Decimal ASCII           | \(1)  |
+--------------------+-------+-------------+-------------+-------------------------+-------+
| ascii_is_lower     | Unary | String-like | Boolean     | Lowercase ASCII         | \(2)  |
+--------------------+-------+-------------+-------------+-------------------------+-------+
| ascii_is_printable | Unary | String-like | Boolean     | Printable ASCII         |       |
+--------------------+-------+-------------+-------------+-------------------------+-------+
| ascii_is_space     | Unary | String-like | Boolean     | Whitespace ASCII        |       |
+--------------------+-------+-------------+-------------+-------------------------+-------+
| ascii_is_upper     | Unary | String-like | Boolean     | Uppercase ASCII         | \(2)  |
+--------------------+-------+-------------+-------------+-------------------------+-------+
| utf8_is_alnum      | Unary | String-like | Boolean     | Alphanumeric Unicode    |       |
+--------------------+-------+-------------+-------------+-------------------------+-------+
| utf8_is_alpha      | Unary | String-like | Boolean     | Alphabetic Unicode      |       |
+--------------------+-------+-------------+-------------+-------------------------+-------+
| utf8_is_decimal    | Unary | String-like | Boolean     | Decimal Unicode         |       |
+--------------------+-------+-------------+-------------+-------------------------+-------+
| utf8_is_digit      | Unary | String-like | Boolean     | Unicode digit           | \(3)  |
+--------------------+-------+-------------+-------------+-------------------------+-------+
| utf8_is_lower      | Unary | String-like | Boolean     | Lowercase Unicode       | \(2)  |
+--------------------+-------+-------------+-------------+-------------------------+-------+
| utf8_is_numeric    | Unary | String-like | Boolean     | Numeric Unicode         | \(4)  |
+--------------------+-------+-------------+-------------+-------------------------+-------+
| utf8_is_printable  | Unary | String-like | Boolean     | Printable Unicode       |       |
+--------------------+-------+-------------+-------------+-------------------------+-------+
| utf8_is_space      | Unary | String-like | Boolean     | Whitespace Unicode      |       |
+--------------------+-------+-------------+-------------+-------------------------+-------+
| utf8_is_upper      | Unary | String-like | Boolean     | Uppercase Unicode       | \(2)  |
+--------------------+-------+-------------+-------------+-------------------------+-------+

* \(1) Also matches all numeric ASCII characters and all ASCII digits.

* \(2) Non-cased characters, such as punctuation, do not match.

* \(3) This is currently the same as ``utf8_is_decimal``.

* \(4) Unlike ``utf8_is_decimal``, non-decimal numeric characters also match.

The second set of functions also consider the character order in a string
element:

+--------------------------+------------+--------------------+---------------------+---------+
| Function name            | Arity      | Input types        | Output type         | Notes   |
+==========================+============+====================+=====================+=========+
| ascii_is_title           | Unary      | String-like        | Boolean             | \(1)    |
+--------------------------+------------+--------------------+---------------------+---------+
| utf8_is_title            | Unary      | String-like        | Boolean             | \(1)    |
+--------------------------+------------+--------------------+---------------------+---------+

* \(1) Output is true iff the input string element is title-cased, i.e. any
  word starts with an uppercase character, followed by lowercase characters.
  Word boundaries are defined by non-cased characters.

The third set of functions examines string elements on a byte-per-byte basis:

+--------------------------+------------+--------------------+---------------------+---------+
| Function name            | Arity      | Input types        | Output type         | Notes   |
+==========================+============+====================+=====================+=========+
| string_is_ascii          | Unary      | String-like        | Boolean             | \(1)    |
+--------------------------+------------+--------------------+---------------------+---------+

* \(1) Output is true iff the input string element contains only ASCII characters,
  i.e. only bytes in [0, 127].

String transforms
~~~~~~~~~~~~~~~~~

+-------------------------+--------+-----------------------------------------+------------------------+-----------------------------------+-------+
| Function name           | Arity  | Input types                             | Output type            | Options class                     | Notes |
+=========================+========+=========================================+========================+===================================+=======+
| ascii_capitalize        | Unary  | String-like                             | String-like            |                                   | \(1)  |
+-------------------------+--------+-----------------------------------------+------------------------+-----------------------------------+-------+
| ascii_lower             | Unary  | String-like                             | String-like            |                                   | \(1)  |
+-------------------------+--------+-----------------------------------------+------------------------+-----------------------------------+-------+
| ascii_reverse           | Unary  | String-like                             | String-like            |                                   | \(2)  |
+-------------------------+--------+-----------------------------------------+------------------------+-----------------------------------+-------+
| ascii_swapcase          | Unary  | String-like                             | String-like            |                                   | \(1)  |
+-------------------------+--------+-----------------------------------------+------------------------+-----------------------------------+-------+
| ascii_title             | Unary  | String-like                             | String-like            |                                   | \(1)  |
+-------------------------+--------+-----------------------------------------+------------------------+-----------------------------------+-------+
| ascii_upper             | Unary  | String-like                             | String-like            |                                   | \(1)  |
+-------------------------+--------+-----------------------------------------+------------------------+-----------------------------------+-------+
| binary_length           | Unary  | Binary- or String-like                  | Int32 or Int64         |                                   | \(3)  |
+-------------------------+--------+-----------------------------------------+------------------------+-----------------------------------+-------+
| binary_repeat           | Binary | Binary/String (Arg 0); Integral (Arg 1) | Binary- or String-like |                                   | \(4)  |
+-------------------------+--------+-----------------------------------------+------------------------+-----------------------------------+-------+
| binary_replace_slice    | Unary  | String-like                             | Binary- or String-like | :struct:`ReplaceSliceOptions`     | \(5)  |
+-------------------------+--------+-----------------------------------------+------------------------+-----------------------------------+-------+
| binary_reverse          | Unary  | Binary                                  | Binary                 |                                   | \(6)  |
+-------------------------+--------+-----------------------------------------+------------------------+-----------------------------------+-------+
| replace_substring       | Unary  | String-like                             | String-like            | :struct:`ReplaceSubstringOptions` | \(7)  |
+-------------------------+--------+-----------------------------------------+------------------------+-----------------------------------+-------+
| replace_substring_regex | Unary  | String-like                             | String-like            | :struct:`ReplaceSubstringOptions` | \(8)  |
+-------------------------+--------+-----------------------------------------+------------------------+-----------------------------------+-------+
| utf8_capitalize         | Unary  | String-like                             | String-like            |                                   | \(9)  |
+-------------------------+--------+-----------------------------------------+------------------------+-----------------------------------+-------+
| utf8_length             | Unary  | String-like                             | Int32 or Int64         |                                   | \(10) |
+-------------------------+--------+-----------------------------------------+------------------------+-----------------------------------+-------+
| utf8_lower              | Unary  | String-like                             | String-like            |                                   | \(9)  |
+-------------------------+--------+-----------------------------------------+------------------------+-----------------------------------+-------+
| utf8_replace_slice      | Unary  | String-like                             | String-like            | :struct:`ReplaceSliceOptions`     | \(7)  |
+-------------------------+--------+-----------------------------------------+------------------------+-----------------------------------+-------+
| utf8_reverse            | Unary  | String-like                             | String-like            |                                   | \(11) |
+-------------------------+--------+-----------------------------------------+------------------------+-----------------------------------+-------+
| utf8_swapcase           | Unary  | String-like                             | String-like            |                                   | \(9)  |
+-------------------------+--------+-----------------------------------------+------------------------+-----------------------------------+-------+
| utf8_title              | Unary  | String-like                             | String-like            |                                   | \(9)  |
+-------------------------+--------+-----------------------------------------+------------------------+-----------------------------------+-------+
| utf8_upper              | Unary  | String-like                             | String-like            |                                   | \(9)  |
+-------------------------+--------+-----------------------------------------+------------------------+-----------------------------------+-------+

* \(1) Each ASCII character in the input is converted to lowercase or
  uppercase.  Non-ASCII characters are left untouched.

* \(2) ASCII input is reversed to the output. If non-ASCII characters
  are present, ``Invalid`` :class:`Status` will be returned.

* \(3) Output is the physical length in bytes of each input element.  Output
  type is Int32 for Binary/String, Int64 for LargeBinary/LargeString.

* \(4) Repeat the input binary string a given number of times.

* \(5) Replace the slice of the substring from :member:`ReplaceSliceOptions::start`
  (inclusive) to :member:`ReplaceSliceOptions::stop` (exclusive) by
  :member:`ReplaceSubstringOptions::replacement`. The binary kernel measures the
  slice in bytes, while the UTF8 kernel measures the slice in codeunits.

* \(6) Perform a byte-level reverse.

* \(7) Replace non-overlapping substrings that match to
  :member:`ReplaceSubstringOptions::pattern` by
  :member:`ReplaceSubstringOptions::replacement`. If
  :member:`ReplaceSubstringOptions::max_replacements` != -1, it determines the
  maximum number of replacements made, counting from the left.

* \(8) Replace non-overlapping substrings that match to the regular expression
  :member:`ReplaceSubstringOptions::pattern` by
  :member:`ReplaceSubstringOptions::replacement`, using the Google RE2 library. If
  :member:`ReplaceSubstringOptions::max_replacements` != -1, it determines the
  maximum number of replacements made, counting from the left. Note that if the
  pattern contains groups, backreferencing can be used.

* \(9) Each UTF8-encoded character in the input is converted to lowercase or
  uppercase.

* \(10) Output is the number of characters (not bytes) of each input element.
  Output type is Int32 for String, Int64 for LargeString.

* \(11) Each UTF8-encoded code unit is written in reverse order to the output.
  If the input is not valid UTF8, then the output is undefined (but the size of output
  buffers will be preserved).

String padding
~~~~~~~~~~~~~~

These functions append/prepend a given padding byte (ASCII) or codepoint (UTF8) in
order to center (center), right-align (lpad), or left-align (rpad) a string.

+--------------------------+------------+-------------------------+---------------------+----------------------------------------+
| Function name            | Arity      | Input types             | Output type         | Options class                          |
+==========================+============+=========================+=====================+========================================+
| ascii_center             | Unary      | String-like             | String-like         | :struct:`PadOptions`                   |
+--------------------------+------------+-------------------------+---------------------+----------------------------------------+
| ascii_lpad               | Unary      | String-like             | String-like         | :struct:`PadOptions`                   |
+--------------------------+------------+-------------------------+---------------------+----------------------------------------+
| ascii_rpad               | Unary      | String-like             | String-like         | :struct:`PadOptions`                   |
+--------------------------+------------+-------------------------+---------------------+----------------------------------------+
| utf8_center              | Unary      | String-like             | String-like         | :struct:`PadOptions`                   |
+--------------------------+------------+-------------------------+---------------------+----------------------------------------+
| utf8_lpad                | Unary      | String-like             | String-like         | :struct:`PadOptions`                   |
+--------------------------+------------+-------------------------+---------------------+----------------------------------------+
| utf8_rpad                | Unary      | String-like             | String-like         | :struct:`PadOptions`                   |
+--------------------------+------------+-------------------------+---------------------+----------------------------------------+

String trimming
~~~~~~~~~~~~~~~

These functions trim off characters on both sides (trim), or the left (ltrim) or right side (rtrim).

+--------------------------+------------+-------------------------+---------------------+----------------------------------------+---------+
| Function name            | Arity      | Input types             | Output type         | Options class                          | Notes   |
+==========================+============+=========================+=====================+========================================+=========+
| ascii_ltrim              | Unary      | String-like             | String-like         | :struct:`TrimOptions`                  | \(1)    |
+--------------------------+------------+-------------------------+---------------------+----------------------------------------+---------+
| ascii_ltrim_whitespace   | Unary      | String-like             | String-like         |                                        | \(2)    |
+--------------------------+------------+-------------------------+---------------------+----------------------------------------+---------+
| ascii_rtrim              | Unary      | String-like             | String-like         | :struct:`TrimOptions`                  | \(1)    |
+--------------------------+------------+-------------------------+---------------------+----------------------------------------+---------+
| ascii_rtrim_whitespace   | Unary      | String-like             | String-like         |                                        | \(2)    |
+--------------------------+------------+-------------------------+---------------------+----------------------------------------+---------+
| ascii_trim               | Unary      | String-like             | String-like         | :struct:`TrimOptions`                  | \(1)    |
+--------------------------+------------+-------------------------+---------------------+----------------------------------------+---------+
| ascii_trim_whitespace    | Unary      | String-like             | String-like         |                                        | \(2)    |
+--------------------------+------------+-------------------------+---------------------+----------------------------------------+---------+
| utf8_ltrim               | Unary      | String-like             | String-like         | :struct:`TrimOptions`                  | \(3)    |
+--------------------------+------------+-------------------------+---------------------+----------------------------------------+---------+
| utf8_ltrim_whitespace    | Unary      | String-like             | String-like         |                                        | \(4)    |
+--------------------------+------------+-------------------------+---------------------+----------------------------------------+---------+
| utf8_rtrim               | Unary      | String-like             | String-like         | :struct:`TrimOptions`                  | \(3)    |
+--------------------------+------------+-------------------------+---------------------+----------------------------------------+---------+
| utf8_rtrim_whitespace    | Unary      | String-like             | String-like         |                                        | \(4)    |
+--------------------------+------------+-------------------------+---------------------+----------------------------------------+---------+
| utf8_trim                | Unary      | String-like             | String-like         | :struct:`TrimOptions`                  | \(3)    |
+--------------------------+------------+-------------------------+---------------------+----------------------------------------+---------+
| utf8_trim_whitespace     | Unary      | String-like             | String-like         |                                        | \(4)    |
+--------------------------+------------+-------------------------+---------------------+----------------------------------------+---------+

* \(1) Only characters specified in :member:`TrimOptions::characters` will be
  trimmed off. Both the input string and the `characters` argument are
  interpreted as ASCII characters.

* \(2) Only trim off ASCII whitespace characters (``'\t'``, ``'\n'``, ``'\v'``,
  ``'\f'``, ``'\r'``  and ``' '``).

* \(3) Only characters specified in :member:`TrimOptions::characters` will be
  trimmed off.

* \(4) Only trim off Unicode whitespace characters.

String splitting
~~~~~~~~~~~~~~~~

These functions split strings into lists of strings.  All kernels can optionally
be configured with a ``max_splits`` and a ``reverse`` parameter, where
``max_splits == -1`` means no limit (the default).  When ``reverse`` is true,
the splitting is done starting from the end of the string; this is only relevant
when a positive ``max_splits`` is given.

+--------------------------+------------+-------------------------+-------------------+----------------------------------+---------+
| Function name            | Arity      | Input types             | Output type       | Options class                    | Notes   |
+==========================+============+=========================+===================+==================================+=========+
| ascii_split_whitespace   | Unary      | String-like             | List-like         | :struct:`SplitOptions`           | \(1)    |
+--------------------------+------------+-------------------------+-------------------+----------------------------------+---------+
| split_pattern            | Unary      | Binary- or String-like  | List-like         | :struct:`SplitPatternOptions`    | \(2)    |
+--------------------------+------------+-------------------------+-------------------+----------------------------------+---------+
| split_pattern_regex      | Unary      | Binary- or String-like  | List-like         | :struct:`SplitPatternOptions`    | \(3)    |
+--------------------------+------------+-------------------------+-------------------+----------------------------------+---------+
| utf8_split_whitespace    | Unary      | String-like             | List-like         | :struct:`SplitOptions`           | \(4)    |
+--------------------------+------------+-------------------------+-------------------+----------------------------------+---------+

* \(1) A non-zero length sequence of ASCII defined whitespace bytes
  (``'\t'``, ``'\n'``, ``'\v'``, ``'\f'``, ``'\r'``  and ``' '``) is seen
  as separator.

* \(2) The string is split when an exact pattern is found (the pattern itself
  is not included in the output).

* \(3) The string is split when a regex match is found (the matched
  substring itself is not included in the output).

* \(4) A non-zero length sequence of Unicode defined whitespace codepoints
  is seen as separator.

String component extraction
~~~~~~~~~~~~~~~~~~~~~~~~~~~

+---------------+-------+------------------------+-------------+-------------------------------+-------+
| Function name | Arity | Input types            | Output type | Options class                 | Notes |
+===============+=======+========================+=============+===============================+=======+
| extract_regex | Unary | Binary- or String-like | Struct      | :struct:`ExtractRegexOptions` | \(1)  |
+---------------+-------+------------------------+-------------+-------------------------------+-------+

* \(1) Extract substrings defined by a regular expression using the Google RE2
  library.  The output struct field names refer to the named capture groups,
  e.g. 'letter' and 'digit' for the regular expression
  ``(?P<letter>[ab])(?P<digit>\\d)``.

String joining
~~~~~~~~~~~~~~

These functions do the inverse of string splitting.

+--------------------------+---------+----------------------------------+------------------------+------------------------+-----------------------+---------+
| Function name            | Arity   | Input type 1                     | Input type 2           | Output type            | Options class         | Notes   |
+==========================+=========+==================================+========================+========================+=======================+=========+
| binary_join              | Binary  | List of Binary- or String-like   | String-like            | String-like            |                       | \(1)    |
+--------------------------+---------+----------------------------------+------------------------+------------------------+-----------------------+---------+
| binary_join_element_wise | Varargs | Binary- or String-like (varargs) | Binary- or String-like | Binary- or String-like | :struct:`JoinOptions` | \(2)    |
+--------------------------+---------+----------------------------------+------------------------+------------------------+-----------------------+---------+

* \(1) The first input must be an array, while the second can be a scalar or array.
  Each list of values in the first input is joined using each second input
  as separator.  If any input list is null or contains a null, the corresponding
  output will be null.

* \(2) All arguments are concatenated element-wise, with the last argument treated
  as the separator (scalars are recycled in either case). Null separators emit
  null. If any other argument is null, by default the corresponding output will be
  null, but it can instead either be skipped or replaced with a given string.

String Slicing
~~~~~~~~~~~~~~

This function transforms each sequence of the array to a subsequence, according
to start and stop indices, and a non-zero step (defaulting to 1).  Slicing
semantics follow Python slicing semantics: the start index is inclusive,
the stop index exclusive; if the step is negative, the sequence is followed
in reverse order.

+--------------------------+------------+-------------------------+-------------------------+--------------------------+---------+
| Function name            | Arity      | Input types             | Output type             | Options class            | Notes   |
+==========================+============+=========================+=========================+==========================+=========+
| binary_slice             | Unary      | Binary-like             | Binary-like             | :struct:`SliceOptions`   | \(1)    |
+--------------------------+------------+-------------------------+-------------------------+--------------------------+---------+
| utf8_slice_codeunits     | Unary      | String-like             | String-like             | :struct:`SliceOptions`   | \(2)    |
+--------------------------+------------+-------------------------+-------------------------+--------------------------+---------+

* \(1) Slice string into a substring defined by (``start``, ``stop``, ``step``)
  as given by :struct:`SliceOptions` where ``start`` and ``stop`` are measured
  in bytes. Null inputs emit null.
* \(2) Slice string into a substring defined by (``start``, ``stop``, ``step``)
  as given by :struct:`SliceOptions` where ``start`` and ``stop`` are measured
  in codeunits. Null inputs emit null.

Containment tests
~~~~~~~~~~~~~~~~~

+-----------------------+-------+-----------------------------------+----------------+---------------------------------+-------+
| Function name         | Arity | Input types                       | Output type    | Options class                   | Notes |
+=======================+=======+===================================+================+=================================+=======+
| count_substring       | Unary | Binary- or String-like            | Int32 or Int64 | :struct:`MatchSubstringOptions` | \(1)  |
+-----------------------+-------+-----------------------------------+----------------+---------------------------------+-------+
| count_substring_regex | Unary | Binary- or String-like            | Int32 or Int64 | :struct:`MatchSubstringOptions` | \(1)  |
+-----------------------+-------+-----------------------------------+----------------+---------------------------------+-------+
| ends_with             | Unary | Binary- or String-like            | Boolean        | :struct:`MatchSubstringOptions` | \(2)  |
+-----------------------+-------+-----------------------------------+----------------+---------------------------------+-------+
| find_substring        | Unary | Binary- and String-like           | Int32 or Int64 | :struct:`MatchSubstringOptions` | \(3)  |
+-----------------------+-------+-----------------------------------+----------------+---------------------------------+-------+
| find_substring_regex  | Unary | Binary- and String-like           | Int32 or Int64 | :struct:`MatchSubstringOptions` | \(3)  |
+-----------------------+-------+-----------------------------------+----------------+---------------------------------+-------+
| index_in              | Unary | Boolean, Null, Numeric, Temporal, | Int32          | :struct:`SetLookupOptions`      | \(4)  |
|                       |       | Binary- and String-like           |                |                                 |       |
+-----------------------+-------+-----------------------------------+----------------+---------------------------------+-------+
| is_in                 | Unary | Boolean, Null, Numeric, Temporal, | Boolean        | :struct:`SetLookupOptions`      | \(5)  |
|                       |       | Binary- and String-like           |                |                                 |       |
+-----------------------+-------+-----------------------------------+----------------+---------------------------------+-------+
| match_like            | Unary | Binary- or String-like            | Boolean        | :struct:`MatchSubstringOptions` | \(6)  |
+-----------------------+-------+-----------------------------------+----------------+---------------------------------+-------+
| match_substring       | Unary | Binary- or String-like            | Boolean        | :struct:`MatchSubstringOptions` | \(7)  |
+-----------------------+-------+-----------------------------------+----------------+---------------------------------+-------+
| match_substring_regex | Unary | Binary- or String-like            | Boolean        | :struct:`MatchSubstringOptions` | \(8)  |
+-----------------------+-------+-----------------------------------+----------------+---------------------------------+-------+
| starts_with           | Unary | Binary- or String-like            | Boolean        | :struct:`MatchSubstringOptions` | \(2)  |
+-----------------------+-------+-----------------------------------+----------------+---------------------------------+-------+

* \(1) Output is the number of occurrences of
  :member:`MatchSubstringOptions::pattern` in the corresponding input
  string. Output type is Int32 for Binary/String, Int64
  for LargeBinary/LargeString.

* \(2) Output is true iff :member:`MatchSubstringOptions::pattern`
  is a suffix/prefix of the corresponding input.

* \(3) Output is the index of the first occurrence of
  :member:`MatchSubstringOptions::pattern` in the corresponding input
  string, otherwise -1. Output type is Int32 for Binary/String, Int64
  for LargeBinary/LargeString.

* \(4) Output is the index of the corresponding input element in
  :member:`SetLookupOptions::value_set`, if found there.  Otherwise,
  output is null.

* \(5) Output is true iff the corresponding input element is equal to one
  of the elements in :member:`SetLookupOptions::value_set`.

* \(6) Output is true iff the SQL-style LIKE pattern
  :member:`MatchSubstringOptions::pattern` fully matches the
  corresponding input element. That is, ``%`` will match any number of
  characters, ``_`` will match exactly one character, and any other
  character matches itself. To match a literal percent sign or
  underscore, precede the character with a backslash.

* \(7) Output is true iff :member:`MatchSubstringOptions::pattern`
  is a substring of the corresponding input element.

* \(8) Output is true iff :member:`MatchSubstringOptions::pattern`
  matches the corresponding input element at any position.

Categorizations
~~~~~~~~~~~~~~~

+-------------------+------------+-------------------------+---------------------+------------------------+---------+
| Function name     | Arity      | Input types             | Output type         | Options class          | Notes   |
+===================+============+=========================+=====================+========================+=========+
| is_finite         | Unary      | Null, Numeric           | Boolean             |                        | \(1)    |
+-------------------+------------+-------------------------+---------------------+------------------------+---------+
| is_inf            | Unary      | Null, Numeric           | Boolean             |                        | \(2)    |
+-------------------+------------+-------------------------+---------------------+------------------------+---------+
| is_nan            | Unary      | Null, Numeric           | Boolean             |                        | \(3)    |
+-------------------+------------+-------------------------+---------------------+------------------------+---------+
| is_null           | Unary      | Any                     | Boolean             | :struct:`NullOptions`  | \(4)    |
+-------------------+------------+-------------------------+---------------------+------------------------+---------+
| is_valid          | Unary      | Any                     | Boolean             |                        | \(5)    |
+-------------------+------------+-------------------------+---------------------+------------------------+---------+
| true_unless_null  | Unary      | Any                     | Boolean             |                        | \(6)    |
+-------------------+------------+-------------------------+---------------------+------------------------+---------+

* \(1) Output is true iff the corresponding input element is finite (neither Infinity,
  -Infinity, nor NaN). Hence, for Decimal and integer inputs this always returns true.

* \(2) Output is true iff the corresponding input element is Infinity/-Infinity.
  Hence, for Decimal and integer inputs this always returns false.

* \(3) Output is true iff the corresponding input element is NaN.
  Hence, for Decimal and integer inputs this always returns false.

* \(4) Output is true iff the corresponding input element is null. NaN values
  can also be considered null by setting :member:`NullOptions::nan_is_null`.

* \(5) Output is true iff the corresponding input element is non-null, else false.

* \(6) Output is true iff the corresponding input element is non-null, else null.
       Mostly intended for expression simplification/guarantees.

.. _cpp-compute-scalar-selections:

Selecting / multiplexing
~~~~~~~~~~~~~~~~~~~~~~~~

For each "row" of input values, these functions emit one of the input values,
depending on a condition.

+------------------+------------+---------------------------------------------------+---------------------+---------+
| Function name    | Arity      | Input types                                       | Output type         | Notes   |
+==================+============+===================================================+=====================+=========+
| case_when        | Varargs    | Struct of Boolean (Arg 0), Any (rest)             | Input type          | \(1)    |
+------------------+------------+---------------------------------------------------+---------------------+---------+
| choose           | Varargs    | Integral (Arg 0), Fixed-width/Binary-like (rest)  | Input type          | \(2)    |
+------------------+------------+---------------------------------------------------+---------------------+---------+
| coalesce         | Varargs    | Any                                               | Input type          | \(3)    |
+------------------+------------+---------------------------------------------------+---------------------+---------+
| if_else          | Ternary    | Boolean (Arg 0), Any (rest)                       | Input type          | \(4)    |
+------------------+------------+---------------------------------------------------+---------------------+---------+

* \(1) This function acts like a SQL "case when" statement or switch-case. The
  input is a "condition" value, which is a struct of Booleans, followed by the
  values for each "branch". There must be either exactly one value argument for
  each child of the condition struct, or one more value argument than children
  (in which case we have an "else" or "default" value). The output is of the
  same type as the value inputs; each row will be the corresponding value from
  the first value datum for which the corresponding Boolean is true, or the
  corresponding value from the "default" input, or null otherwise.

  Note that currently, while all types are supported, dictionaries will be
  unpacked.

* \(2) The first input must be an integral type. The rest of the arguments can be
  any type, but must all be the same type or promotable to a common type. Each
  value of the first input (the 'index') is used as a zero-based index into the
  remaining arguments (i.e. index 0 is the second argument, index 1 is the third
  argument, etc.), and the value of the output for that row will be the
  corresponding value of the selected input at that row. If the index is null,
  then the output will also be null.

* \(3) Each row of the output will be the corresponding value of the first
  input which is non-null for that row, otherwise null.

* \(4) First input must be a Boolean scalar or array. Second and third inputs
  could be scalars or arrays and must be of the same type. Output is an array
  (or scalar if all inputs are scalar) of the same type as the second/ third
  input. If the nulls present on the first input, they will be promoted to the
  output, otherwise nulls will be chosen based on the first input values.

  Also see: :ref:`replace_with_mask <cpp-compute-vector-structural-transforms>`.

Structural transforms
~~~~~~~~~~~~~~~~~~~~~

+---------------------+------------+-------------+------------------+------------------------------+--------+
| Function name       | Arity      | Input types | Output type      | Options class                | Notes  |
+=====================+============+=============+==================+==============================+========+
| list_value_length   | Unary      | List-like   | Int32 or Int64   |                              | \(1)   |
+---------------------+------------+-------------+------------------+------------------------------+--------+
| make_struct         | Varargs    | Any         | Struct           | :struct:`MakeStructOptions`  | \(2)   |
+---------------------+------------+-------------+------------------+------------------------------+--------+

* \(1) Each output element is the length of the corresponding input element
  (null if input is null).  Output type is Int32 for List and FixedSizeList,
  Int64 for LargeList.

* \(2) The output struct's field types are the types of its arguments. The
  field names are specified using an instance of :struct:`MakeStructOptions`.
  The output shape will be scalar if all inputs are scalar, otherwise any
  scalars will be broadcast to arrays.

Conversions
~~~~~~~~~~~

A general conversion function named ``cast`` is provided which accepts a large
number of input and output types.  The type to cast to can be passed in a
:struct:`CastOptions` instance.  As an alternative, the same service is
provided by a concrete function :func:`~arrow::compute::Cast`.

+-----------------+------------+--------------------+------------------+--------------------------------+-------+
| Function name   | Arity      | Input types        | Output type      | Options class                  | Notes |
+=================+============+====================+==================+================================+=======+
| ceil_temporal   | Unary      | Temporal           | Temporal         | :struct:`RoundTemporalOptions` |       |
+-----------------+------------+--------------------+------------------+--------------------------------+-------+
| floor_temporal  | Unary      | Temporal           | Temporal         | :struct:`RoundTemporalOptions` |       |
+-----------------+------------+--------------------+------------------+--------------------------------+-------+
| round_temporal  | Unary      | Temporal           | Temporal         | :struct:`RoundTemporalOptions` |       |
+-----------------+------------+--------------------+------------------+--------------------------------+-------+
| cast            | Unary      | Many               | Variable         | :struct:`CastOptions`          |       |
+-----------------+------------+--------------------+------------------+--------------------------------+-------+
| strftime        | Unary      | Temporal           | String           | :struct:`StrftimeOptions`      | \(1)  |
+-----------------+------------+--------------------+------------------+--------------------------------+-------+
| strptime        | Unary      | String-like        | Timestamp        | :struct:`StrptimeOptions`      |       |
+-----------------+------------+--------------------+------------------+--------------------------------+-------+

The conversions available with ``cast`` are listed below.  In all cases, a
null input value is converted into a null output value.

* \(1) Output precision of ``%S`` (seconds) flag depends on the input timestamp
  precision. Timestamps with second precision are represented as integers while
  milliseconds, microsecond and nanoseconds are represented as fixed floating
  point numbers with 3, 6 and 9 decimal places respectively. To obtain integer
  seconds, cast to timestamp with second resolution.
  The character for the decimal point is localized according to the locale.
  See `detailed formatting documentation`_ for descriptions of other flags.

.. _detailed formatting documentation: https://howardhinnant.github.io/date/date.html#to_stream_formatting

**Truth value extraction**

+-----------------------------+------------------------------------+--------------+
| Input type                  | Output type                        | Notes        |
+=============================+====================================+==============+
| Binary- and String-like     | Boolean                            | \(1)         |
+-----------------------------+------------------------------------+--------------+
| Numeric                     | Boolean                            | \(2)         |
+-----------------------------+------------------------------------+--------------+

* \(1) Output is true iff the corresponding input value has non-zero length.

* \(2) Output is true iff the corresponding input value is non-zero.

**Same-kind conversion**

+-----------------------------+------------------------------------+--------------+
| Input type                  | Output type                        | Notes        |
+=============================+====================================+==============+
| Int32                       | 32-bit Temporal                    | \(1)         |
+-----------------------------+------------------------------------+--------------+
| Int64                       | 64-bit Temporal                    | \(1)         |
+-----------------------------+------------------------------------+--------------+
| (Large)Binary               | (Large)String                      | \(2)         |
+-----------------------------+------------------------------------+--------------+
| (Large)String               | (Large)Binary                      | \(3)         |
+-----------------------------+------------------------------------+--------------+
| Numeric                     | Numeric                            | \(4) \(5)    |
+-----------------------------+------------------------------------+--------------+
| 32-bit Temporal             | Int32                              | \(1)         |
+-----------------------------+------------------------------------+--------------+
| 64-bit Temporal             | Int64                              | \(1)         |
+-----------------------------+------------------------------------+--------------+
| Temporal                    | Temporal                           | \(4) \(5)    |
+-----------------------------+------------------------------------+--------------+

* \(1) No-operation cast: the raw values are kept identical, only
  the type is changed.

* \(2) Validates the contents if :member:`CastOptions::allow_invalid_utf8`
  is false.

* \(3) No-operation cast: only the type is changed.

* \(4) Overflow and truncation checks are enabled depending on
  the given :struct:`CastOptions`.

* \(5) Not all such casts have been implemented.

**String representations**

+-----------------------------+------------------------------------+---------+
| Input type                  | Output type                        | Notes   |
+=============================+====================================+=========+
| Boolean                     | String-like                        |         |
+-----------------------------+------------------------------------+---------+
| Numeric                     | String-like                        |         |
+-----------------------------+------------------------------------+---------+

**Generic conversions**

+-----------------------------+------------------------------------+---------+
| Input type                  | Output type                        | Notes   |
+=============================+====================================+=========+
| Dictionary                  | Dictionary value type              | \(1)    |
+-----------------------------+------------------------------------+---------+
| Extension                   | Extension storage type             |         |
+-----------------------------+------------------------------------+---------+
| Struct                      | Struct                             | \(2)    |
+-----------------------------+------------------------------------+---------+
| List-like                   | List-like                          | \(3)    |
+-----------------------------+------------------------------------+---------+
| Map                         | Map or List of two-field struct    | \(4)    |
+-----------------------------+------------------------------------+---------+
| Null                        | Any                                |         |
+-----------------------------+------------------------------------+---------+
| Any                         | Extension                          | \(5)    |
+-----------------------------+------------------------------------+---------+

* \(1) The dictionary indices are unchanged, the dictionary values are
  cast from the input value type to the output value type (if a conversion
  is available).

* \(2) The field names of the output type must be the same or a subset of the
  field names of the input type; they also must have the same order. Casting to
  a subset of field names "selects" those fields such that each output field
  matches the data of the input field with the same name.

* \(3) The list offsets are unchanged, the list values are cast from the
  input value type to the output value type (if a conversion is
  available).

* \(4) Offsets are unchanged, the keys and values are cast from respective input
  to output types (if a conversion is available). If output type is a list of
  struct, the key field is output as the first field and the value field the 
  second field, regardless of field names chosen.

* \(5) Any input type that can be cast to the resulting extension's storage type.
  This excludes extension types, unless being cast to the same extension type.

Temporal component extraction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These functions extract datetime components (year, month, day, etc) from temporal types.
For timestamps inputs with non-empty timezone, localized timestamp components will be returned.

+--------------------+------------+-------------------+---------------+----------------------------+-------+
| Function name      | Arity      | Input types       | Output type   | Options class              | Notes |
+====================+============+===================+===============+============================+=======+
| day                | Unary      | Temporal          | Int64         |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| day_of_week        | Unary      | Temporal          | Int64         | :struct:`DayOfWeekOptions` | \(1)  |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| day_of_year        | Unary      | Temporal          | Int64         |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| hour               | Unary      | Timestamp, Time   | Int64         |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| is_dst             | Unary      | Timestamp         | Boolean       |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| iso_week           | Unary      | Temporal          | Int64         |                            | \(2)  |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| iso_year           | Unary      | Temporal          | Int64         |                            | \(2)  |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| iso_calendar       | Unary      | Temporal          | Struct        |                            | \(3)  |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| is_leap_year       | Unary      | Timestamp, Date   | Boolean       |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| microsecond        | Unary      | Timestamp, Time   | Int64         |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| millisecond        | Unary      | Timestamp, Time   | Int64         |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| minute             | Unary      | Timestamp, Time   | Int64         |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| month              | Unary      | Temporal          | Int64         |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| nanosecond         | Unary      | Timestamp, Time   | Int64         |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| quarter            | Unary      | Temporal          | Int64         |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| second             | Unary      | Timestamp, Time   | Int64         |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| subsecond          | Unary      | Timestamp, Time   | Float64       |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| us_week            | Unary      | Temporal          | Int64         |                            | \(4)  |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| us_year            | Unary      | Temporal          | Int64         |                            | \(4)  |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| week               | Unary      | Timestamp         | Int64         | :struct:`WeekOptions`      | \(5)  |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| year               | Unary      | Temporal          | Int64         |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| year_month_day     | Unary      | Temporal          | Struct        |                            | \(6)  |
+--------------------+------------+-------------------+---------------+----------------------------+-------+

* \(1) Outputs the number of the day of the week. By default week begins on Monday
  represented by 0 and ends on Sunday represented by 6. Day numbering can start with 0 or 1 based on
  :member:`DayOfWeekOptions::count_from_zero` parameter. :member:`DayOfWeekOptions::week_start` can be
  used to set the starting day of the week using ISO convention (Monday=1, Sunday=7).
  :member:`DayOfWeekOptions::week_start` parameter is not affected by :member:`DayOfWeekOptions::count_from_zero`.

* \(2) First ISO week has the majority (4 or more) of it's days in January. ISO year
  starts with the first ISO week. ISO week starts on Monday.
  See `ISO 8601 week date definition`_ for more details.

* \(3) Output is a ``{"iso_year": output type, "iso_week": output type, "iso_day_of_week":  output type}`` Struct.

* \(4) First US week has the majority (4 or more) of its days in January. US year
  starts with the first US week. US week starts on Sunday.

* \(5) Returns week number allowing for setting several parameters.
  If :member:`WeekOptions::week_starts_monday` is true, the week starts with Monday, else Sunday if false.
  If :member:`WeekOptions::count_from_zero` is true, dates from the current year that fall into the last ISO week
  of the previous year are numbered as week 0, else week 52 or 53 if false.
  If :member:`WeekOptions::first_week_is_fully_in_year` is true, the first week (week 1) must fully be in January;
  else if false, a week that begins on December 29, 30, or 31 is considered the first week of the new year.

* \(6) Output is a ``{"year": int64(), "month": int64(), "day": int64()}`` Struct.

.. _ISO 8601 week date definition: https://en.wikipedia.org/wiki/ISO_week_date#First_week

Temporal difference
~~~~~~~~~~~~~~~~~~~

These functions compute the difference between two timestamps in the
specified unit. The difference is determined by the number of
boundaries crossed, not the span of time. For example, the difference
in days between 23:59:59 on one day and 00:00:01 on the next day is
one day (since midnight was crossed), not zero days (even though less
than 24 hours elapsed). Additionally, if the timestamp has a defined
timezone, the difference is calculated in the local timezone. For
instance, the difference in years between "2019-12-31 18:00:00-0500"
and "2019-12-31 23:00:00-0500" is zero years, because the local year
is the same, even though the UTC years would be different.

+---------------------------------+------------+-------------------+-----------------------+----------------------------+
| Function name                   | Arity      | Input types       | Output type           | Options class              |
+=================================+============+===================+=======================+============================+
| day_time_interval_between       | Binary     | Temporal          | DayTime interval      |                            |
+---------------------------------+------------+-------------------+-----------------------+----------------------------+
| days_between                    | Binary     | Timestamp, Date   | Int64                 |                            |
+---------------------------------+------------+-------------------+-----------------------+----------------------------+
| hours_between                   | Binary     | Temporal          | Int64                 |                            |
+---------------------------------+------------+-------------------+-----------------------+----------------------------+
| microseconds_between            | Binary     | Temporal          | Int64                 |                            |
+---------------------------------+------------+-------------------+-----------------------+----------------------------+
| milliseconds_between            | Binary     | Temporal          | Int64                 |                            |
+---------------------------------+------------+-------------------+-----------------------+----------------------------+
| minutes_between                 | Binary     | Temporal          | Int64                 |                            |
+---------------------------------+------------+-------------------+-----------------------+----------------------------+
| month_day_nano_interval_between | Binary     | Temporal          | MonthDayNano interval |                            |
+---------------------------------+------------+-------------------+-----------------------+----------------------------+
| month_interval_between          | Binary     | Timestamp, Date   | Month interval        |                            |
+---------------------------------+------------+-------------------+-----------------------+----------------------------+
| nanoseconds_between             | Binary     | Temporal          | Int64                 |                            |
+---------------------------------+------------+-------------------+-----------------------+----------------------------+
| quarters_between                | Binary     | Timestamp, Date   | Int64                 |                            |
+---------------------------------+------------+-------------------+-----------------------+----------------------------+
| seconds_between                 | Binary     | Temporal          | Int64                 |                            |
+---------------------------------+------------+-------------------+-----------------------+----------------------------+
| weeks_between                   | Binary     | Timestamp, Date   | Int64                 | :struct:`DayOfWeekOptions` |
+---------------------------------+------------+-------------------+-----------------------+----------------------------+
| years_between                   | Binary     | Timestamp, Date   | Int64                 |                            |
+---------------------------------+------------+-------------------+-----------------------+----------------------------+

Timezone handling
~~~~~~~~~~~~~~~~~

`assume_timezone` function is meant to be used when an external system produces
"timezone-naive" timestamps which need to be converted to "timezone-aware"
timestamps (see for example the `definition
<https://docs.python.org/3/library/datetime.html#aware-and-naive-objects>`__
in the Python documentation).

Input timestamps are assumed to be relative to the timezone given in
:member:`AssumeTimezoneOptions::timezone`. They are converted to
UTC-relative timestamps with the timezone metadata set to the above value.
An error is returned if the timestamps already have the timezone metadata set.

`local_timestamp` function converts UTC-relative timestamps to local "timezone-naive"
timestamps. The timezone is taken from the timezone metadata of the input
timestamps. This function is the inverse of `assume_timezone`. Please note:
**all temporal functions already operate on timestamps as if they were in local
time of the metadata provided timezone**. Using `local_timestamp` is only meant to be
used when an external system expects local timestamps.

+-----------------+-------+-------------+---------------+---------------------------------+-------+
| Function name   | Arity | Input types | Output type   | Options class                   | Notes |
+=================+=======+=============+===============+=================================+=======+
| assume_timezone | Unary | Timestamp   | Timestamp     | :struct:`AssumeTimezoneOptions` | \(1)  |
+-----------------+-------+-------------+---------------+---------------------------------+-------+
| local_timestamp | Unary | Timestamp   | Timestamp     |                                 | \(2)  |
+-----------------+-------+-------------+---------------+---------------------------------+-------+

* \(1) In addition to the timezone value, :struct:`AssumeTimezoneOptions`
  allows choosing the behaviour when a timestamp is ambiguous or nonexistent
  in the given timezone (because of DST shifts).

Random number generation
~~~~~~~~~~~~~~~~~~~~~~~~

This function generates an array of uniformly-distributed double-precision numbers
in range [0, 1). The options provide the length of the output and the algorithm for
generating the random numbers, using either a seed or a system-provided, platform-specific
random generator.

+--------------------+------------+---------------+-------------------------+
| Function name      | Arity      | Output type   | Options class           |
+====================+============+===============+=========================+
| random             | Nullary    | Float64       | :struct:`RandomOptions` |
+--------------------+------------+---------------+-------------------------+


Array-wise ("vector") functions
-------------------------------

Cumulative Functions
~~~~~~~~~~~~~~~~~~~~

Cumulative functions are vector functions that perform a running total on their
input using an given binary associatve operation and output an array containing
the corresponding intermediate running values. The input is expected to be of
numeric type. By default these functions do not detect overflow. They are also
available in an overflow-checking variant, suffixed ``_checked``, which returns
an ``Invalid`` :class:`Status` when overflow is detected.

+------------------------+-------+-------------+-------------+--------------------------------+-------+
| Function name          | Arity | Input types | Output type | Options class                  | Notes |
+========================+=======+=============+=============+================================+=======+
| cumulative_sum         | Unary | Numeric     | Numeric     | :struct:`CumulativeSumOptions` | \(1)  |
+------------------------+-------+-------------+-------------+--------------------------------+-------+
| cumulative_sum_checked | Unary | Numeric     | Numeric     | :struct:`CumulativeSumOptions` | \(1)  |
+------------------------+-------+-------------+-------------+--------------------------------+-------+

* \(1) CumulativeSumOptions has two optional parameters. The first parameter
  :member:`CumulativeSumOptions::start` is a starting value for the running
  sum. It has a default value of 0. Specified values of ``start`` must have the
  same type as the input. The second parameter 
  :member:`CumulativeSumOptions::skip_nulls` is a boolean. When set to
  false (the default), the first encountered null is propagated. When set to
  true, each null in the input produces a corresponding null in the output.

Associative transforms
~~~~~~~~~~~~~~~~~~~~~~

+-------------------+-------+-----------------------------------+-------------+-------+
| Function name     | Arity | Input types                       | Output type | Notes |
+===================+=======+===================================+=============+=======+
| dictionary_encode | Unary | Boolean, Null, Numeric,           | Dictionary  | \(1)  |
|                   |       | Temporal, Binary- and String-like |             |       |
+-------------------+-------+-----------------------------------+-------------+-------+
| unique            | Unary | Boolean, Null, Numeric,           | Input type  | \(2)  |
|                   |       | Temporal, Binary- and String-like |             |       |
+-------------------+-------+-----------------------------------+-------------+-------+
| value_counts      | Unary | Boolean, Null, Numeric,           | Input type  | \(3)  |
|                   |       | Temporal, Binary- and String-like |             |       |
+-------------------+-------+-----------------------------------+-------------+-------+

* \(1) Output is ``Dictionary(Int32, input type)``.

* \(2) Duplicates are removed from the output while the original order is
  maintained.

* \(3) Output is a ``{"values": input type, "counts": Int64}`` Struct.
  Each output element corresponds to a unique value in the input, along
  with the number of times this value has appeared.

Selections
~~~~~~~~~~

These functions select and return a subset of their input.

+---------------+--------+--------------+--------------+--------------+-------------------------+-----------+
| Function name | Arity  | Input type 1 | Input type 2 | Output type  | Options class           | Notes     |
+===============+========+==============+==============+==============+=========================+===========+
| array_filter  | Binary | Any          | Boolean      | Input type 1 | :struct:`FilterOptions` | \(1) \(3) |
+---------------+--------+--------------+--------------+--------------+-------------------------+-----------+
| array_take    | Binary | Any          | Integer      | Input type 1 | :struct:`TakeOptions`   | \(1) \(4) |
+---------------+--------+--------------+--------------+--------------+-------------------------+-----------+
| drop_null     | Unary  | Any          | -            | Input type 1 |                         | \(1) \(2) |
+---------------+--------+--------------+--------------+--------------+-------------------------+-----------+
| filter        | Binary | Any          | Boolean      | Input type 1 | :struct:`FilterOptions` | \(1) \(3) |
+---------------+--------+--------------+--------------+--------------+-------------------------+-----------+
| take          | Binary | Any          | Integer      | Input type 1 | :struct:`TakeOptions`   | \(1) \(4) |
+---------------+--------+--------------+--------------+--------------+-------------------------+-----------+

* \(1) Sparse unions are unsupported.

* \(2) Each element in the input is appended to the output iff it is non-null.
  If the input is a record batch or table, any null value in a column drops
  the entire row.

* \(3) Each element in input 1 (the values) is appended to the output iff
  the corresponding element in input 2 (the filter) is true.  How
  nulls in the filter are handled can be configured using FilterOptions.

* \(4) For each element *i* in input 2 (the indices), the *i*'th element
  in input 1 (the values) is appended to the output.

Containment tests
~~~~~~~~~~~~~~~~~

This function returns the indices at which array elements are non-null and non-zero.

+-----------------------+-------+-----------------------------------+----------------+---------------------------------+-------+
| Function name         | Arity | Input types                       | Output type    | Options class                   | Notes |
+=======================+=======+===================================+================+=================================+=======+
| indices_nonzero       | Unary | Boolean, Null, Numeric, Decimal   | UInt64         |                                 |       |
+-----------------------+-------+-----------------------------------+----------------+---------------------------------+-------+

Sorts and partitions
~~~~~~~~~~~~~~~~~~~~

By default, in these functions, nulls are considered greater than any other value
(they will be sorted or partitioned at the end of the array).  Floating-point
NaN values are considered greater than any other non-null value, but smaller
than nulls.  This behaviour can be changed using the ``null_placement`` setting
in the respective option classes.

.. note::
   Binary- and String-like inputs are ordered lexicographically as bytestrings,
   even for String types.

+-----------------------+------------+---------------------------------------------------------+-------------------+--------------------------------+----------------+
| Function name         | Arity      | Input types                                             | Output type       | Options class                  | Notes          |
+=======================+============+=========================================================+===================+================================+================+
| array_sort_indices    | Unary      | Boolean, Numeric, Temporal, Binary- and String-like     | UInt64            | :struct:`ArraySortOptions`     | \(1) \(2)      |
+-----------------------+------------+---------------------------------------------------------+-------------------+--------------------------------+----------------+
| partition_nth_indices | Unary      | Boolean, Numeric, Temporal, Binary- and String-like     | UInt64            | :struct:`PartitionNthOptions`  | \(3)           |
+-----------------------+------------+---------------------------------------------------------+-------------------+--------------------------------+----------------+
| rank                  | Unary      | Boolean, Numeric, Temporal, Binary- and String-like     | UInt64            | :struct:`RankOptions`          | \(4)           |
+-----------------------+------------+---------------------------------------------------------+-------------------+--------------------------------+----------------+
| select_k_unstable     | Unary      | Boolean, Numeric, Temporal, Binary- and String-like     | UInt64            | :struct:`SelectKOptions`       | \(5) \(6)      |
+-----------------------+------------+---------------------------------------------------------+-------------------+--------------------------------+----------------+
| sort_indices          | Unary      | Boolean, Numeric, Temporal, Binary- and String-like     | UInt64            | :struct:`SortOptions`          | \(1) \(5)      |
+-----------------------+------------+---------------------------------------------------------+-------------------+--------------------------------+----------------+


* \(1) The output is an array of indices into the input, that define a
  stable sort of the input.

* \(2) The input must be an array. The default order is ascending.

* \(3) The output is an array of indices into the input array, that define
  a partial non-stable sort such that the *N*'th index points to the *N*'th
  element in sorted order, and all indices before the *N*'th point to
  elements less or equal to elements at or after the *N*'th (similar to
  :func:`std::nth_element`).  *N* is given in
  :member:`PartitionNthOptions::pivot`.

* \(4) The output is a one-based numerical array of ranks

* \(5) The input can be an array, chunked array, record batch or
  table. If the input is a record batch or table, one or more sort
  keys must be specified.

* \(6) The output is an array of indices into the input, that define a
  non-stable sort of the input.

.. _cpp-compute-vector-structural-transforms:

Structural transforms
~~~~~~~~~~~~~~~~~~~~~

+---------------------+------------+-------------------------------------+------------------+------------------------------+--------+
| Function name       | Arity      | Input types                         | Output type      | Options class                | Notes  |
+=====================+============+=====================================+==================+==============================+========+
| list_element        | Binary     | List-like (Arg 0), Integral (Arg 1) | List value type  |                              | \(1)   |
+---------------------+------------+-------------------------------------+------------------+------------------------------+--------+
| list_flatten        | Unary      | List-like                           | List value type  |                              | \(2)   |
+---------------------+------------+-------------------------------------+------------------+------------------------------+--------+
| list_parent_indices | Unary      | List-like                           | Int64            |                              | \(3)   |
+---------------------+------------+-------------------------------------+------------------+------------------------------+--------+
| list_slice          | Unary      | List-like                           | List-like        | :struct:`ListSliceOptions`   | \(4)   |
+---------------------+------------+-------------------------------------+------------------+------------------------------+--------+
| map_lookup          | Unary      | Map                                 | Computed         | :struct:`MapLookupOptions`   | \(5)   |
+---------------------+------------+-------------------------------------+------------------+------------------------------+--------+
| struct_field        | Unary      | Struct or Union                     | Computed         | :struct:`StructFieldOptions` | \(6)   |
+---------------------+------------+-------------------------------------+------------------+------------------------------+--------+

* \(1) Output is an array of the same length as the input list array. The
  output values are the values at the specified index of each child list.

* \(2) The top level of nesting is removed: all values in the list child array,
  including nulls, are appended to the output.  However, nulls in the parent
  list array are discarded.

* \(3) For each value in the list child array, the index at which it is found
  in the list array is appended to the output.  Nulls in the parent list array
  are discarded.

* \(4) For each list element, compute the slice of that list element, then
  return another list-like array of those slices. Can return either a
  fixed or variable size list-like array, as determined by options provided.

* \(5) Extract either the ``FIRST``, ``LAST`` or ``ALL`` items from a
  map whose key match the given query key passed via options.
  The output type is an Array of items for the ``FIRST``/``LAST`` options
  and an Array of List of items for the ``ALL`` option.

* \(6) Extract a child value based on a sequence of indices passed in
  the options. The validity bitmap of the result will be the
  intersection of all intermediate validity bitmaps. For example, for
  an array with type ``struct<a: int32, b: struct<c: int64, d:
  float64>>``:

  * An empty sequence of indices yields the original value unchanged.
  * The index ``0`` yields an array of type ``int32`` whose validity
    bitmap is the intersection of the bitmap for the outermost struct
    and the bitmap for the child ``a``.
  * The index ``1, 1`` yields an array of type ``float64`` whose
    validity bitmap is the intersection of the bitmaps for the
    outermost struct, for struct ``b``, and for the child ``d``.

  For unions, a validity bitmap is synthesized based on the type
  codes. Also, the index is always the child index and not a type code.
  Hence for array with type ``sparse_union<2: int32, 7: utf8>``:

  * The index ``0`` yields an array of type ``int32``, which is valid
    at an index *n* if and only if the child array ``a`` is valid at
    index *n* and the type code at index *n* is 2.
  * The indices ``2`` and ``7`` are invalid.

These functions create a copy of the first input with some elements
replaced, based on the remaining inputs.

+--------------------------+------------+-----------------------+--------------+--------------+--------------+-------+
| Function name            | Arity      | Input type 1          | Input type 2 | Input type 3 | Output type  | Notes |
+==========================+============+=======================+==============+==============+==============+=======+
| fill_null_backward       | Unary      | Fixed-width or binary | N/A          | N/A          | N/A          | \(1)  |
+--------------------------+------------+-----------------------+--------------+--------------+--------------+-------+
| fill_null_forward        | Unary      | Fixed-width or binary | N/A          | N/A          | N/A          | \(1)  |
+--------------------------+------------+-----------------------+--------------+--------------+--------------+-------+
| replace_with_mask        | Ternary    | Fixed-width or binary | Boolean      | Input type 1 | Input type 1 | \(2)  |
+--------------------------+------------+-----------------------+--------------+--------------+--------------+-------+

* \(1) Valid values are carried forward/backward to fill null values.
* \(2) Each element in input 1 for which the corresponding Boolean in input 2
  is true is replaced with the next value from input 3. A null in input 2
  results in a corresponding null in the output.

  Also see: :ref:`if_else <cpp-compute-scalar-selections>`.
