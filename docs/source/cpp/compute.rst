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
either.  For example, the ``fill_null`` function requires its second input
to be a scalar, while ``sort_indices`` requires its first and only input to
be an array.

Invoking functions
------------------

Compute functions can be invoked by name using
:func:`arrow::compute::CallFunction`::

   std::shared_ptr<arrow::Array> numbers_array = ...;
   std::shared_ptr<arrow::Scalar> increment = ...;
   arrow::Datum incremented_datum;

   ARROW_ASSIGN_OR_RAISE(incremented_datum,
                         arrow::compute::CallFunction("add", {numbers_array, increment}));
   std::shared_ptr<Array> incremented_array = std::move(incremented_datum).array();

(note this example uses implicit conversion from ``std::shared_ptr<Array>``
to ``Datum``)

Many compute functions are also available directly as concrete APIs, here
:func:`arrow::compute::Add`::

   std::shared_ptr<arrow::Array> numbers_array = ...;
   std::shared_ptr<arrow::Scalar> increment = ...;
   arrow::Datum incremented_datum;

   ARROW_ASSIGN_OR_RAISE(incremented_datum,
                         arrow::compute::Add(numbers_array, increment));
   std::shared_ptr<Array> incremented_array = std::move(incremented_datum).array();

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

If you are unsure whether a function supports a concrete input type, we
recommend you try it out.  Unsupported input types return a ``TypeError``
:class:`Status`.

Aggregations
------------

+---------------+-------+-------------+----------------+----------------------------------+-------+
| Function name | Arity | Input types | Output type    | Options class                    | Notes |
+===============+=======+=============+================+==================================+=======+
| all           | Unary | Boolean     | Scalar Boolean | :struct:`ScalarAggregateOptions` | \(1)  |
+---------------+-------+-------------+----------------+----------------------------------+-------+
| any           | Unary | Boolean     | Scalar Boolean | :struct:`ScalarAggregateOptions` | \(1)  |
+---------------+-------+-------------+----------------+----------------------------------+-------+
| count         | Unary | Any         | Scalar Int64   | :struct:`ScalarAggregateOptions` |       |
+---------------+-------+-------------+----------------+----------------------------------+-------+
| index         | Unary | Any         | Scalar Int64   | :struct:`IndexOptions`           |       |
+---------------+-------+-------------+----------------+----------------------------------+-------+
| mean          | Unary | Numeric     | Scalar Float64 | :struct:`ScalarAggregateOptions` |       |
+---------------+-------+-------------+----------------+----------------------------------+-------+
| min_max       | Unary | Numeric     | Scalar Struct  | :struct:`ScalarAggregateOptions` | \(2)  |
+---------------+-------+-------------+----------------+----------------------------------+-------+
| mode          | Unary | Numeric     | Struct         | :struct:`ModeOptions`            | \(3)  |
+---------------+-------+-------------+----------------+----------------------------------+-------+
| quantile      | Unary | Numeric     | Scalar Numeric | :struct:`QuantileOptions`        | \(4)  |
+---------------+-------+-------------+----------------+----------------------------------+-------+
| stddev        | Unary | Numeric     | Scalar Float64 | :struct:`VarianceOptions`        |       |
+---------------+-------+-------------+----------------+----------------------------------+-------+
| sum           | Unary | Numeric     | Scalar Numeric | :struct:`ScalarAggregateOptions` | \(5)  |
+---------------+-------+-------------+----------------+----------------------------------+-------+
| tdigest       | Unary | Numeric     | Scalar Float64 | :struct:`TDigestOptions`         |       |
+---------------+-------+-------------+----------------+----------------------------------+-------+
| variance      | Unary | Numeric     | Scalar Float64 | :struct:`VarianceOptions`        |       |
+---------------+-------+-------------+----------------+----------------------------------+-------+

Notes:

* \(1) If null values are taken into account by setting ScalarAggregateOptions
  parameter skip_nulls = false then `Kleene logic`_ logic is applied.

* \(2) Output is a ``{"min": input type, "max": input type}`` Struct.

* \(3) Output is an array of ``{"mode": input type, "count": Int64}`` Struct.
  It contains the *N* most common elements in the input, in descending
  order, where *N* is given in :member:`ModeOptions::n`.
  If two values have the same count, the smallest one comes first.
  Note that the output can have less than *N* elements if the input has
  less than *N* distinct values.

* \(4) Output is Float64 or input type, depending on QuantileOptions.

* \(5) Output is Int64, UInt64 or Float64, depending on the input type.

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
Input(s) will be cast to the :ref:`common numeric type <common-numeric-type>`
(and dictionary decoded, if applicable) before the operation is applied.

The default variant of these functions does not detect overflow (the result
then typically wraps around).  Most functions are also available in an
overflow-checking variant, suffixed ``_checked``, which returns
an ``Invalid`` :class:`Status` when overflow is detected.

+------------------+--------+----------------+----------------------+-------+
| Function name    | Arity  | Input types    | Output type          | Notes |
+==================+========+================+======================+=======+
| abs              | Unary  | Numeric        | Numeric              |       |
+------------------+--------+----------------+----------------------+-------+
| abs_checked      | Unary  | Numeric        | Numeric              |       |
+------------------+--------+----------------+----------------------+-------+
| add              | Binary | Numeric        | Numeric              | \(1)  |
+------------------+--------+----------------+----------------------+-------+
| add_checked      | Binary | Numeric        | Numeric              | \(1)  |
+------------------+--------+----------------+----------------------+-------+
| divide           | Binary | Numeric        | Numeric              | \(1)  |
+------------------+--------+----------------+----------------------+-------+
| divide_checked   | Binary | Numeric        | Numeric              | \(1)  |
+------------------+--------+----------------+----------------------+-------+
| multiply         | Binary | Numeric        | Numeric              | \(1)  |
+------------------+--------+----------------+----------------------+-------+
| multiply_checked | Binary | Numeric        | Numeric              | \(1)  |
+------------------+--------+----------------+----------------------+-------+
| negate           | Unary  | Numeric        | Numeric              |       |
+------------------+--------+----------------+----------------------+-------+
| negate_checked   | Unary  | Signed Numeric | Signed Numeric       |       |
+------------------+--------+----------------+----------------------+-------+
| power            | Binary | Numeric        | Numeric              |       |
+------------------+--------+----------------+----------------------+-------+
| power_checked    | Binary | Numeric        | Numeric              |       |
+------------------+--------+----------------+----------------------+-------+
| sign             | Unary  | Numeric        | Int8/Float32/Float64 | \(2)  |
+------------------+--------+----------------+----------------------+-------+
| subtract         | Binary | Numeric        | Numeric              | \(1)  |
+------------------+--------+----------------+----------------------+-------+
| subtract_checked | Binary | Numeric        | Numeric              | \(1)  |
+------------------+--------+----------------+----------------------+-------+

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

* \(2) Output is any of (-1,1) for nonzero inputs and 0 for zero input.
  NaN values return NaN.  Integral values return signedness as Int8 and
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

Rounding functions convert a numeric input into an approximate value with a
simpler representation based on the rounding strategy.

+------------------+--------+----------------+-----------------+-------+
| Function name    | Arity  | Input types    | Output type     | Notes |
+==================+========+================+=================+=======+
| floor            | Unary  | Numeric        | Float32/Float64 |       |
+------------------+--------+----------------+-----------------+-------+
| ceil             | Unary  | Numeric        | Float32/Float64 |       |
+------------------+--------+----------------+-----------------+-------+
| trunc            | Unary  | Numeric        | Float32/Float64 |       |
+------------------+--------+----------------+-----------------+-------+

Logarithmic functions
~~~~~~~~~~~~~~~~~~~~~

Logarithmic functions are also supported, and also offer ``_checked``
variants that check for domain errors if needed.

+--------------------------+------------+--------------------+---------------------+
| Function name            | Arity      | Input types        | Output type         |
+==========================+============+====================+=====================+
| ln                       | Unary      | Float32/Float64    | Float32/Float64     |
+--------------------------+------------+--------------------+---------------------+
| ln_checked               | Unary      | Float32/Float64    | Float32/Float64     |
+--------------------------+------------+--------------------+---------------------+
| log10                    | Unary      | Float32/Float64    | Float32/Float64     |
+--------------------------+------------+--------------------+---------------------+
| log10_checked            | Unary      | Float32/Float64    | Float32/Float64     |
+--------------------------+------------+--------------------+---------------------+
| log1p                    | Unary      | Float32/Float64    | Float32/Float64     |
+--------------------------+------------+--------------------+---------------------+
| log1p_checked            | Unary      | Float32/Float64    | Float32/Float64     |
+--------------------------+------------+--------------------+---------------------+
| log2                     | Unary      | Float32/Float64    | Float32/Float64     |
+--------------------------+------------+--------------------+---------------------+
| log2_checked             | Unary      | Float32/Float64    | Float32/Float64     |
+--------------------------+------------+--------------------+---------------------+

Trigonometric functions
~~~~~~~~~~~~~~~~~~~~~~~

Trigonometric functions are also supported, and also offer ``_checked``
variants that check for domain errors if needed.

+--------------------------+------------+--------------------+---------------------+
| Function name            | Arity      | Input types        | Output type         |
+==========================+============+====================+=====================+
| acos                     | Unary      | Float32/Float64    | Float32/Float64     |
+--------------------------+------------+--------------------+---------------------+
| acos_checked             | Unary      | Float32/Float64    | Float32/Float64     |
+--------------------------+------------+--------------------+---------------------+
| asin                     | Unary      | Float32/Float64    | Float32/Float64     |
+--------------------------+------------+--------------------+---------------------+
| asin_checked             | Unary      | Float32/Float64    | Float32/Float64     |
+--------------------------+------------+--------------------+---------------------+
| atan                     | Unary      | Float32/Float64    | Float32/Float64     |
+--------------------------+------------+--------------------+---------------------+
| atan2                    | Binary     | Float32/Float64    | Float32/Float64     |
+--------------------------+------------+--------------------+---------------------+
| cos                      | Unary      | Float32/Float64    | Float32/Float64     |
+--------------------------+------------+--------------------+---------------------+
| cos_checked              | Unary      | Float32/Float64    | Float32/Float64     |
+--------------------------+------------+--------------------+---------------------+
| sin                      | Unary      | Float32/Float64    | Float32/Float64     |
+--------------------------+------------+--------------------+---------------------+
| sin_checked              | Unary      | Float32/Float64    | Float32/Float64     |
+--------------------------+------------+--------------------+---------------------+
| tan                      | Unary      | Float32/Float64    | Float32/Float64     |
+--------------------------+------------+--------------------+---------------------+
| tan_checked              | Unary      | Float32/Float64    | Float32/Float64     |
+--------------------------+------------+--------------------+---------------------+

Comparisons
~~~~~~~~~~~

These functions expect two inputs of numeric type (in which case they will be
cast to the :ref:`common numeric type <common-numeric-type>` before comparison),
or two inputs of Binary- or String-like types, or two inputs of Temporal types.
If any input is dictionary encoded it will be expanded for the purposes of
comparison. If any of the input elements in a pair is null, the corresponding
output element is null.

+--------------------------+------------+---------------------------------------------+---------------------+
| Function names           | Arity      | Input types                                 | Output type         |
+==========================+============+=============================================+=====================+
| equal, not_equal         | Binary     | Numeric, Temporal, Binary- and String-like  | Boolean             |
+--------------------------+------------+---------------------------------------------+---------------------+
| greater, greater_equal,  | Binary     | Numeric, Temporal, Binary- and String-like  | Boolean             |
| less, less_equal         |            |                                             |                     |
+--------------------------+------------+---------------------------------------------+---------------------+

These functions take any number of inputs of numeric type (in which case they
will be cast to the :ref:`common numeric type <common-numeric-type>` before
comparison) or of temporal types. If any input is dictionary encoded it will be
expanded for the purposes of comparison.

+--------------------------+------------+---------------------------------------------+---------------------+---------------------------------------+-------+
| Function names           | Arity      | Input types                                 | Output type         | Options class                         | Notes |
+==========================+============+=============================================+=====================+=======================================+=======+
| max_element_wise,        | Varargs    | Numeric and Temporal                        | Numeric or Temporal | :struct:`ElementWiseAggregateOptions` | \(1)  |
| min_element_wise         |            |                                             |                     |                                       |       |
+--------------------------+------------+---------------------------------------------+---------------------+---------------------------------------+-------+

* \(1) By default, nulls are skipped (but the kernel can be configured to propagate nulls).
  For floating point values, NaN will be taken over null but not over any other value.

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
| and_not                  | Binary     | Boolean            | Boolean             |
+--------------------------+------------+--------------------+---------------------+
| and_kleene               | Binary     | Boolean            | Boolean             |
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

+-------------------------+-------+------------------------+------------------------+-----------------------------------+-------+
| Function name           | Arity | Input types            | Output type            | Options class                     | Notes |
+=========================+=======+========================+========================+===================================+=======+
| ascii_lower             | Unary | String-like            | String-like            |                                   | \(1)  |
+-------------------------+-------+------------------------+------------------------+-----------------------------------+-------+
| ascii_reverse           | Unary | String-like            | String-like            |                                   | \(2)  |
+-------------------------+-------+------------------------+------------------------+-----------------------------------+-------+
| ascii_upper             | Unary | String-like            | String-like            |                                   | \(1)  |
+-------------------------+-------+------------------------+------------------------+-----------------------------------+-------+
| binary_length           | Unary | Binary- or String-like | Int32 or Int64         |                                   | \(3)  |
+-------------------------+-------+------------------------+------------------------+-----------------------------------+-------+
| binary_replace_slice    | Unary | String-like            | Binary- or String-like | :struct:`ReplaceSliceOptions`     | \(4)  |
+-------------------------+-------+------------------------+------------------------+-----------------------------------+-------+
| replace_substring       | Unary | String-like            | String-like            | :struct:`ReplaceSubstringOptions` | \(5)  |
+-------------------------+-------+------------------------+------------------------+-----------------------------------+-------+
| replace_substring_regex | Unary | String-like            | String-like            | :struct:`ReplaceSubstringOptions` | \(6)  |
+-------------------------+-------+------------------------+------------------------+-----------------------------------+-------+
| utf8_length             | Unary | String-like            | Int32 or Int64         |                                   | \(7)  |
+-------------------------+-------+------------------------+------------------------+-----------------------------------+-------+
| utf8_lower              | Unary | String-like            | String-like            |                                   | \(8)  |
+-------------------------+-------+------------------------+------------------------+-----------------------------------+-------+
| utf8_replace_slice      | Unary | String-like            | String-like            | :struct:`ReplaceSliceOptions`     | \(4)  |
+-------------------------+-------+------------------------+------------------------+-----------------------------------+-------+
| utf8_reverse            | Unary | String-like            | String-like            |                                   | \(9)  |
+-------------------------+-------+------------------------+------------------------+-----------------------------------+-------+
| utf8_upper              | Unary | String-like            | String-like            |                                   | \(8)  |
+-------------------------+-------+------------------------+------------------------+-----------------------------------+-------+


* \(1) Each ASCII character in the input is converted to lowercase or
  uppercase.  Non-ASCII characters are left untouched.

* \(2) ASCII input is reversed to the output. If non-ASCII characters
  are present, ``Invalid`` :class:`Status` will be returned.

* \(3) Output is the physical length in bytes of each input element.  Output
  type is Int32 for Binary / String, Int64 for LargeBinary / LargeString.

* \(4) Replace the slice of the substring from :member:`ReplaceSliceOptions::start`
  (inclusive) to :member:`ReplaceSliceOptions::stop` (exclusive) by
  :member:`ReplaceSubstringOptions::replacement`. The binary kernel measures the
  slice in bytes, while the UTF8 kernel measures the slice in codeunits.

* \(5) Replace non-overlapping substrings that match to
  :member:`ReplaceSubstringOptions::pattern` by
  :member:`ReplaceSubstringOptions::replacement`. If
  :member:`ReplaceSubstringOptions::max_replacements` != -1, it determines the
  maximum number of replacements made, counting from the left.

* \(6) Replace non-overlapping substrings that match to the regular expression
  :member:`ReplaceSubstringOptions::pattern` by
  :member:`ReplaceSubstringOptions::replacement`, using the Google RE2 library. If
  :member:`ReplaceSubstringOptions::max_replacements` != -1, it determines the
  maximum number of replacements made, counting from the left. Note that if the
  pattern contains groups, backreferencing can be used.

* \(7) Output is the number of characters (not bytes) of each input element.
  Output type is Int32 for String, Int64 for LargeString.

* \(8) Each UTF8-encoded character in the input is converted to lowercase or
  uppercase.

* \(9) Each UTF8-encoded code unit is written in reverse order to the output.
  If the input is not valid UTF8, then the output is undefined (but the size of output
  buffers will be preserved).

String padding
~~~~~~~~~~~~~~

These functions append/prepend a given padding byte (ASCII) or codepoint (UTF8) in
order to center (center), right-align (lpad), or left-align (rpad) a string.

+--------------------------+------------+-------------------------+---------------------+----------------------------------------+
| Function name            | Arity      | Input types             | Output type         | Options class                          |
+==========================+============+=========================+=====================+========================================+
| ascii_lpad               | Unary      | String-like             | String-like         | :struct:`PadOptions`                   |
+--------------------------+------------+-------------------------+---------------------+----------------------------------------+
| ascii_rpad               | Unary      | String-like             | String-like         | :struct:`PadOptions`                   |
+--------------------------+------------+-------------------------+---------------------+----------------------------------------+
| ascii_center             | Unary      | String-like             | String-like         | :struct:`PadOptions`                   |
+--------------------------+------------+-------------------------+---------------------+----------------------------------------+
| utf8_lpad                | Unary      | String-like             | String-like         | :struct:`PadOptions`                   |
+--------------------------+------------+-------------------------+---------------------+----------------------------------------+
| utf8_rpad                | Unary      | String-like             | String-like         | :struct:`PadOptions`                   |
+--------------------------+------------+-------------------------+---------------------+----------------------------------------+
| utf8_center              | Unary      | String-like             | String-like         | :struct:`PadOptions`                   |
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


Containment tests
~~~~~~~~~~~~~~~~~

+-----------------------+-------+-----------------------------------+----------------+---------------------------------+-------+
| Function name         | Arity | Input types                       | Output type    | Options class                   | Notes |
+=======================+=======+===================================+================+=================================+=======+
| count_substring       | Unary | String-like                       | Int32 or Int64 | :struct:`MatchSubstringOptions` | \(1)  |
+-----------------------+-------+-----------------------------------+----------------+---------------------------------+-------+
| count_substring_regex | Unary | String-like                       | Int32 or Int64 | :struct:`MatchSubstringOptions` | \(1)  |
+-----------------------+-------+-----------------------------------+----------------+---------------------------------+-------+
| ends_with             | Unary | String-like                       | Boolean        | :struct:`MatchSubstringOptions` | \(2)  |
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
| match_like            | Unary | String-like                       | Boolean        | :struct:`MatchSubstringOptions` | \(6)  |
+-----------------------+-------+-----------------------------------+----------------+---------------------------------+-------+
| match_substring       | Unary | String-like                       | Boolean        | :struct:`MatchSubstringOptions` | \(7)  |
+-----------------------+-------+-----------------------------------+----------------+---------------------------------+-------+
| match_substring_regex | Unary | String-like                       | Boolean        | :struct:`MatchSubstringOptions` | \(8)  |
+-----------------------+-------+-----------------------------------+----------------+---------------------------------+-------+
| starts_with           | Unary | String-like                       | Boolean        | :struct:`MatchSubstringOptions` | \(2)  |
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
| split_pattern            | Unary      | String-like             | List-like         | :struct:`SplitPatternOptions`    | \(1)    |
+--------------------------+------------+-------------------------+-------------------+----------------------------------+---------+
| split_pattern_regex      | Unary      | String-like             | List-like         | :struct:`SplitPatternOptions`    | \(2)    |
+--------------------------+------------+-------------------------+-------------------+----------------------------------+---------+
| utf8_split_whitespace    | Unary      | String-like             | List-like         | :struct:`SplitOptions`           | \(3)    |
+--------------------------+------------+-------------------------+-------------------+----------------------------------+---------+
| ascii_split_whitespace   | Unary      | String-like             | List-like         | :struct:`SplitOptions`           | \(4)    |
+--------------------------+------------+-------------------------+-------------------+----------------------------------+---------+

* \(1) The string is split when an exact pattern is found (the pattern itself
  is not included in the output).

* \(2) The string is split when a regex match is found (the matched
  substring itself is not included in the output).

* \(3) A non-zero length sequence of Unicode defined whitespace codepoints
  is seen as separator.

* \(4) A non-zero length sequence of ASCII defined whitespace bytes
  (``'\t'``, ``'\n'``, ``'\v'``, ``'\f'``, ``'\r'``  and ``' '``) is seen
  as separator.


String component extraction
~~~~~~~~~~~~~~~~~~~~~~~~~~~

+---------------+-------+-------------+-------------+-------------------------------+-------+
| Function name | Arity | Input types | Output type | Options class                 | Notes |
+===============+=======+=============+=============+===============================+=======+
| extract_regex | Unary | String-like | Struct      | :struct:`ExtractRegexOptions` | \(1)  |
+---------------+-------+-------------+-------------+-------------------------------+-------+

* \(1) Extract substrings defined by a regular expression using the Google RE2
  library.  The output struct field names refer to the named capture groups,
  e.g. 'letter' and 'digit' for the regular expression
  ``(?P<letter>[ab])(?P<digit>\\d)``.


String joining
~~~~~~~~~~~~~~

These functions do the inverse of string splitting.

+--------------------------+-----------+-----------------------+----------------+-------------------+-----------------------+---------+
| Function name            | Arity     | Input type 1          | Input type 2   | Output type       | Options class         | Notes   |
+==========================+===========+=======================+================+===================+=======================+=========+
| binary_join              | Binary    | List of string-like   | String-like    | String-like       |                       | \(1)    |
+--------------------------+-----------+-----------------------+----------------+-------------------+-----------------------+---------+
| binary_join_element_wise | Varargs   | String-like (varargs) | String-like    | String-like       | :struct:`JoinOptions` | \(2)    |
+--------------------------+-----------+-----------------------+----------------+-------------------+-----------------------+---------+

* \(1) The first input must be an array, while the second can be a scalar or array.
  Each list of values in the first input is joined using each second input
  as separator.  If any input list is null or contains a null, the corresponding
  output will be null.

* \(2) All arguments are concatenated element-wise, with the last argument treated
  as the separator (scalars are recycled in either case). Null separators emit
  null. If any other argument is null, by default the corresponding output will be
  null, but it can instead either be skipped or replaced with a given string.

Slicing
~~~~~~~

This function transforms each sequence of the array to a subsequence, according
to start and stop indices, and a non-zero step (defaulting to 1).  Slicing
semantics follow Python slicing semantics: the start index is inclusive,
the stop index exclusive; if the step is negative, the sequence is followed
in reverse order.

+--------------------------+------------+----------------+-----------------+--------------------------+---------+
| Function name            | Arity      | Input types    | Output type     | Options class            | Notes   |
+==========================+============+================+=================+==========================+=========+
| utf8_slice_codepoints    | Unary      | String-like    | String-like     | :struct:`SliceOptions`   | \(1)    |
+--------------------------+------------+----------------+-----------------+--------------------------+---------+

* \(1) Slice string into a substring defined by (``start``, ``stop``, ``step``)
  as given by :struct:`SliceOptions` where ``start`` and ``stop`` are measured
  in codeunits. Null inputs emit null.

.. _cpp-compute-scalar-structural-transforms:

Structural transforms
~~~~~~~~~~~~~~~~~~~~~

.. XXX (this category is a bit of a hodgepodge)

+--------------------------+------------+---------------------------------------------------+---------------------+---------+
| Function name            | Arity      | Input types                                       | Output type         | Notes   |
+==========================+============+===================================================+=====================+=========+
| case_when                | Varargs    | Struct of Boolean (Arg 0), Any fixed-width (rest) | Input type          | \(1)    |
+--------------------------+------------+---------------------------------------------------+---------------------+---------+
| coalesce                 | Varargs    | Any                                               | Input type          | \(2)    |
+--------------------------+------------+---------------------------------------------------+---------------------+---------+
| fill_null                | Binary     | Boolean, Null, Numeric, Temporal, String-like     | Input type          | \(3)    |
+--------------------------+------------+---------------------------------------------------+---------------------+---------+
| if_else                  | Ternary    | Boolean, Null, Numeric, Temporal                  | Input type          | \(4)    |
+--------------------------+------------+---------------------------------------------------+---------------------+---------+
| is_finite                | Unary      | Float, Double                                     | Boolean             | \(5)    |
+--------------------------+------------+---------------------------------------------------+---------------------+---------+
| is_inf                   | Unary      | Float, Double                                     | Boolean             | \(6)    |
+--------------------------+------------+---------------------------------------------------+---------------------+---------+
| is_nan                   | Unary      | Float, Double                                     | Boolean             | \(7)    |
+--------------------------+------------+---------------------------------------------------+---------------------+---------+
| is_null                  | Unary      | Any                                               | Boolean             | \(8)    |
+--------------------------+------------+---------------------------------------------------+---------------------+---------+
| is_valid                 | Unary      | Any                                               | Boolean             | \(9)    |
+--------------------------+------------+---------------------------------------------------+---------------------+---------+
| list_value_length        | Unary      | List-like                                         | Int32 or Int64      | \(10)   |
+--------------------------+------------+---------------------------------------------------+---------------------+---------+
| make_struct              | Varargs    | Any                                               | Struct              | \(11)   |
+--------------------------+------------+---------------------------------------------------+---------------------+---------+

* \(1) This function acts like a SQL 'case when' statement or switch-case. The
  input is a "condition" value, which is a struct of Booleans, followed by the
  values for each "branch". There must be either exactly one value argument for
  each child of the condition struct, or one more value argument than children
  (in which case we have an 'else' or 'default' value). The output is of the
  same type as the value inputs; each row will be the corresponding value from
  the first value datum for which the corresponding Boolean is true, or the
  corresponding value from the 'default' input, or null otherwise.

* \(2) Each row of the output will be the corresponding value of the first
  input which is non-null for that row, otherwise null.

* \(3) First input must be an array, second input a scalar of the same type.
  Output is an array of the same type as the inputs, and with the same values
  as the first input, except for nulls replaced with the second input value.

* \(4) First input must be a Boolean scalar or array. Second and third inputs
  could be scalars or arrays and must be of the same type. Output is an array
  (or scalar if all inputs are scalar) of the same type as the second/ third
  input. If the nulls present on the first input, they will be promoted to the
  output, otherwise nulls will be chosen based on the first input values.

  Also see: :ref:`replace_with_mask <cpp-compute-vector-structural-transforms>`.

* \(5) Output is true iff the corresponding input element is finite (not Infinity,
  -Infinity, or NaN).

* \(6) Output is true iff the corresponding input element is Infinity/-Infinity.

* \(7) Output is true iff the corresponding input element is NaN.

* \(8) Output is true iff the corresponding input element is null.

* \(9) Output is true iff the corresponding input element is non-null.

* \(10) Each output element is the length of the corresponding input element
  (null if input is null).  Output type is Int32 for List, Int64 for LargeList.

* \(11) The output struct's field types are the types of its arguments. The
  field names are specified using an instance of :struct:`MakeStructOptions`.
  The output shape will be scalar if all inputs are scalar, otherwise any
  scalars will be broadcast to arrays.

Conversions
~~~~~~~~~~~

A general conversion function named ``cast`` is provided which accepts a large
number of input and output types.  The type to cast to can be passed in a
:struct:`CastOptions` instance.  As an alternative, the same service is
provided by a concrete function :func:`~arrow::compute::Cast`.

+--------------------------+------------+--------------------+-----------------------+--------------------------------------------+
| Function name            | Arity      | Input types        | Output type           | Options class                              |
+==========================+============+====================+=======================+============================================+
| cast                     | Unary      | Many               | Variable              | :struct:`CastOptions`                      |
+--------------------------+------------+--------------------+-----------------------+--------------------------------------------+
| strptime                 | Unary      | String-like        | Timestamp             | :struct:`StrptimeOptions`                  |
+--------------------------+------------+--------------------+-----------------------+--------------------------------------------+

The conversions available with ``cast`` are listed below.  In all cases, a
null input value is converted into a null output value.

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
| List-like                   | List-like                          | \(2)    |
+-----------------------------+------------------------------------+---------+
| Null                        | Any                                |         |
+-----------------------------+------------------------------------+---------+

* \(1) The dictionary indices are unchanged, the dictionary values are
  cast from the input value type to the output value type (if a conversion
  is available).

* \(2) The list offsets are unchanged, the list values are cast from the
  input value type to the output value type (if a conversion is
  available).


Temporal component extraction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These functions extract datetime components (year, month, day, etc) from timestamp type.
Note: this is currently not supported for timestamps with timezone information.

+--------------------+------------+-------------------+---------------+----------------------------+-------+
| Function name      | Arity      | Input types       | Output type   | Options class              | Notes |
+====================+============+===================+===============+============================+=======+
| year               | Unary      | Temporal          | Int64         |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| month              | Unary      | Temporal          | Int64         |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| day                | Unary      | Temporal          | Int64         |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| day_of_week        | Unary      | Temporal          | Int64         | :struct:`DayOfWeekOptions` | \(1)  |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| day_of_year        | Unary      | Temporal          | Int64         |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| iso_year           | Unary      | Temporal          | Int64         |                            | \(2)  |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| iso_week           | Unary      | Temporal          | Int64         |                            | \(2)  |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| iso_calendar       | Unary      | Temporal          | Struct        |                            | \(3)  |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| quarter            | Unary      | Temporal          | Int64         |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| hour               | Unary      | Temporal          | Int64         |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| minute             | Unary      | Temporal          | Int64         |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| second             | Unary      | Temporal          | Int64         |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| millisecond        | Unary      | Temporal          | Int64         |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| microsecond        | Unary      | Temporal          | Int64         |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| nanosecond         | Unary      | Temporal          | Int64         |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+
| subsecond          | Unary      | Temporal          | Double        |                            |       |
+--------------------+------------+-------------------+---------------+----------------------------+-------+

* \(1) Outputs the number of the day of the week. By default week begins on Monday
  represented by 0 and ends on Sunday represented by 6. :member:`DayOfWeekOptions::week_start` can be used to set
  the starting day of the week using ISO convention (Monday=1, Sunday=7). Day numbering can start with 0 or 1
  using :member:`DayOfWeekOptions::one_based_numbering` parameter.
* \(2) First ISO week has the majority (4 or more) of it's days in January. ISO year
  starts with the first ISO week.
  See `ISO 8601 week date definition`_ for more details.
* \(3) Output is a ``{"iso_year": output type, "iso_week": output type, "iso_day_of_week":  output type}`` Struct.

.. _ISO 8601 week date definition: https://en.wikipedia.org/wiki/ISO_week_date#First_week


Array-wise ("vector") functions
-------------------------------

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

These functions select a subset of the first input defined by the second input.

+---------------+--------+--------------+--------------+--------------+-------------------------+-----------+
| Function name | Arity  | Input type 1 | Input type 2 | Output type  | Options class           | Notes     |
+===============+========+==============+==============+==============+=========================+===========+
| filter        | Binary | Any          | Boolean      | Input type 1 | :struct:`FilterOptions` | \(1) \(2) |
+---------------+--------+--------------+--------------+--------------+-------------------------+-----------+
| take          | Binary | Any          | Integer      | Input type 1 | :struct:`TakeOptions`   | \(1) \(3) |
+---------------+--------+--------------+--------------+--------------+-------------------------+-----------+

* \(1) Unions are unsupported.

* \(2) Each element in input 1 is appended to the output iff the corresponding
  element in input 2 is true.

* \(3) For each element *i* in input 2, the *i*'th element in input 1 is
  appended to the output.

Sorts and partitions
~~~~~~~~~~~~~~~~~~~~

In these functions, nulls are considered greater than any other value
(they will be sorted or partitioned at the end of the array).
Floating-point NaN values are considered greater than any other non-null
value, but smaller than nulls.

+-----------------------+------------+-----------------------------+-------------------+--------------------------------+----------------+
| Function name         | Arity      | Input types                 | Output type       | Options class                  | Notes          |
+=======================+============+=============================+===================+================================+================+
| partition_nth_indices | Unary      | Binary- and String-like     | UInt64            | :struct:`PartitionNthOptions`  | \(1) \(3)      |
+-----------------------+------------+-----------------------------+-------------------+--------------------------------+----------------+
| partition_nth_indices | Unary      | Boolean, Numeric, Temporal  | UInt64            | :struct:`PartitionNthOptions`  | \(1)           |
+-----------------------+------------+-----------------------------+-------------------+--------------------------------+----------------+
| array_sort_indices    | Unary      | Binary- and String-like     | UInt64            | :struct:`ArraySortOptions`     | \(2) \(3) \(4) |
+-----------------------+------------+-----------------------------+-------------------+--------------------------------+----------------+
| array_sort_indices    | Unary      | Boolean, Numeric, Temporal  | UInt64            | :struct:`ArraySortOptions`     | \(2) \(4)      |
+-----------------------+------------+-----------------------------+-------------------+--------------------------------+----------------+
| sort_indices          | Unary      | Binary- and String-like     | UInt64            | :struct:`SortOptions`          | \(2) \(3) \(5) |
+-----------------------+------------+-----------------------------+-------------------+--------------------------------+----------------+
| sort_indices          | Unary      | Boolean, Numeric, Temporal  | UInt64            | :struct:`SortOptions`          | \(2) \(5)      |
+-----------------------+------------+-----------------------------+-------------------+--------------------------------+----------------+

* \(1) The output is an array of indices into the input array, that define
  a partial non-stable sort such that the *N*'th index points to the *N*'th
  element in sorted order, and all indices before the *N*'th point to
  elements less or equal to elements at or after the *N*'th (similar to
  :func:`std::nth_element`).  *N* is given in
  :member:`PartitionNthOptions::pivot`.

* \(2) The output is an array of indices into the input, that define a
  stable sort of the input.

* \(3) Input values are ordered lexicographically as bytestrings (even
  for String arrays).

* \(4) The input must be an array. The default order is ascending.

* \(5) The input can be an array, chunked array, record batch or
  table. If the input is a record batch or table, one or more sort
  keys must be specified.

.. _cpp-compute-vector-structural-transforms:

Structural transforms
~~~~~~~~~~~~~~~~~~~~~

+--------------------------+------------+--------------------+---------------------+---------+
| Function name            | Arity      | Input types        | Output type         | Notes   |
+==========================+============+====================+=====================+=========+
| list_flatten             | Unary      | List-like          | List value type     | \(1)    |
+--------------------------+------------+--------------------+---------------------+---------+
| list_parent_indices      | Unary      | List-like          | Int32 or Int64      | \(2)    |
+--------------------------+------------+--------------------+---------------------+---------+

* \(1) The top level of nesting is removed: all values in the list child array,
  including nulls, are appended to the output.  However, nulls in the parent
  list array are discarded.

* \(2) For each value in the list child array, the index at which it is found
  in the list array is appended to the output.  Nulls in the parent list array
  are discarded.

These functions create a copy of the first input with some elements
replaced, based on the remaining inputs.

+--------------------------+------------+-----------------------+--------------+--------------+--------------+-------+
| Function name            | Arity      | Input type 1          | Input type 2 | Input type 3 | Output type  | Notes |
+==========================+============+=======================+==============+==============+==============+=======+
| replace_with_mask        | Ternary    | Fixed-width or binary | Boolean      | Input type 1 | Input type 1 | \(1)  |
+--------------------------+------------+-----------------------+--------------+--------------+--------------+-------+

* \(1) Each element in input 1 for which the corresponding Boolean in input 2
  is true is replaced with the next value from input 3. A null in input 2
  results in a corresponding null in the output.

  Also see: :ref:`if_else <cpp-compute-scalar-structural-transforms>`.
