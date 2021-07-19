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

.. _api.compute:
.. currentmodule:: pyarrow.compute

Compute Functions
=================

Aggregations
------------

.. autosummary::
   :toctree: ../generated/

   count
   index
   mean
   min_max
   mode
   stddev
   sum
   variance

Arithmetic Functions
--------------------

By default these functions do not detect overflow. Most functions are also
available in an overflow-checking variant, suffixed ``_checked``, which
throws an ``ArrowInvalid`` exception when overflow is detected.

.. autosummary::
   :toctree: ../generated/

   abs
   abs_checked
   add
   add_checked
   divide
   divide_checked
   multiply
   multiply_checked
   power
   power_checked
   shift_left
   shift_left_checked
   shift_right
   shift_right_checked
   sign
   subtract
   subtract_checked

Bit-wise operations do not offer (or need) a checked variant.

.. autosummary::
   :toctree: ../generated/

   bit_wise_and
   bit_wise_not
   bit_wise_or
   bit_wise_xor

Rounding Functions
------------------

Rounding functions convert a numeric input into an approximate value with a
simpler representation based on the rounding strategy.

.. autosummary::
   :toctree: ../generated/

   ceil
   floor
   trunc

Logarithmic Functions
---------------------

Logarithmic functions are also supported, and also offer ``_checked``
variants which detect domain errors.

.. autosummary::
   :toctree: ../generated/

   ln
   ln_checked
   log10
   log10_checked
   log1p
   log1p_checked
   log2
   log2_checked

Trigonometric Functions
-----------------------

Trigonometric functions are also supported, and also offer ``_checked``
variants which detect domain errors where appropriate.

.. autosummary::
   :toctree: ../generated/

   acos
   acos_checked
   asin
   asin_checked
   atan
   atan2
   cos
   cos_checked
   sin
   sin_checked
   tan
   tan_checked

Comparisons
-----------

These functions expect two inputs of the same type. If one of the inputs is `null`
they return ``null``.

.. autosummary::
   :toctree: ../generated/

   equal
   greater
   greater_equal
   less
   less_equal
   not_equal

These functions take any number of arguments of a numeric or temporal type.

.. autosummary::
   :toctree: ../generated/

   max_element_wise
   min_element_wise

Logical Functions
-----------------

These functions normally emit a null when one of the inputs is null. However, Kleene
logic variants are provided (suffixed ``_kleene``). See User Guide for details.

.. autosummary::
   :toctree: ../generated/

   and_
   and_kleene
   all
   any
   invert
   or_
   or_kleene
   xor

String Predicates
-----------------

In these functions an empty string emits false in the output. For ASCII
variants (prefixed ``ascii_``) a string element with non-ASCII characters
emits false in the output.

The first set of functions emit true if the input contains only
characters of a given class.

.. autosummary::
   :toctree: ../generated/

   ascii_is_alnum
   ascii_is_alpha
   ascii_is_decimal
   ascii_is_lower
   ascii_is_printable
   ascii_is_space
   ascii_is_upper
   utf8_is_alnum
   utf8_is_alpha
   utf8_is_decimal
   utf8_is_digit
   utf8_is_lower
   utf8_is_numeric
   utf8_is_printable
   utf8_is_space
   utf8_is_upper

The second set of functions also consider the order of characters
in the string element.

.. autosummary::
   :toctree: ../generated/

   ascii_is_title
   utf8_is_title

The third set of functions examines string elements on
a byte-by-byte basis.

.. autosummary::
   :toctree: ../generated/

   string_is_ascii

String Splitting
----------------

.. autosummary::
   :toctree: ../generated/

   split_pattern
   split_pattern_regex
   ascii_split_whitespace
   utf8_split_whitespace

String Component Extraction
---------------------------

.. autosummary::
   :toctree: ../generated/

   extract_regex

String Joining
--------------

.. autosummary::
   :toctree: ../generated/

   binary_join
   binary_join_element_wise

String Transforms
-----------------

.. autosummary::
   :toctree: ../generated/

   ascii_center
   ascii_lpad
   ascii_ltrim
   ascii_ltrim_whitespace
   ascii_lower
   ascii_reverse
   ascii_rpad
   ascii_rtrim
   ascii_rtrim_whitespace
   ascii_trim
   ascii_upper
   binary_length
   binary_replace_slice
   replace_substring
   replace_substring_regex
   utf8_center
   utf8_length
   utf8_lower
   utf8_lpad
   utf8_ltrim
   utf8_ltrim_whitespace
   utf8_replace_slice
   utf8_reverse
   utf8_rpad
   utf8_rtrim
   utf8_rtrim_whitespace
   utf8_trim
   utf8_upper

Containment tests
-----------------

.. autosummary::
   :toctree: ../generated/

   count_substring
   count_substring_regex
   ends_with
   find_substring
   find_substring_regex
   index_in
   is_in
   match_like
   match_substring
   match_substring_regex
   starts_with

Conversions
-----------

.. autosummary::
   :toctree: ../generated/

   cast
   strptime

Replacements
------------

.. autosummary::
   :toctree: ../generated/

   replace_with_mask

Selections
----------

.. autosummary::
   :toctree: ../generated/

   filter
   take

Associative transforms
----------------------

.. autosummary::
   :toctree: ../generated/

   dictionary_encode
   unique
   value_counts

Sorts and partitions
--------------------

.. autosummary::
   :toctree: ../generated/

   partition_nth_indices
   sort_indices

Structural Transforms
---------------------

.. autosummary::
   :toctree: ../generated/

   binary_length
   case_when
   coalesce
   fill_null
   if_else
   is_finite
   is_inf
   is_nan
   is_null
   is_valid
   list_value_length
   list_flatten
   list_parent_indices
