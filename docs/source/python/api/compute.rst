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

   all
   any
   approximate_median
   count
   count_distinct
   index
   max
   mean
   min
   min_max
   mode
   product
   quantile
   stddev
   sum
   tdigest
   variance

..
  Nullary aggregate functions (count_all) aren't exposed in pyarrow.compute,
  so they aren't listed here.

Cumulative Functions
--------------------

Cumulative functions are vector functions that perform a running total on their
input and output an array containing the corresponding intermediate running values.
By default these functions do not detect overflow. They are also
available in an overflow-checking variant, suffixed ``_checked``, which
throws an ``ArrowInvalid`` exception when overflow is detected.

.. autosummary::
   :toctree: ../generated/

   cumulative_sum
   cumulative_sum_checked

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
   negate
   negate_checked
   power
   power_checked
   sign
   sqrt
   sqrt_checked
   subtract
   subtract_checked

Bit-wise Functions
------------------

.. autosummary::
   :toctree: ../generated/

   bit_wise_and
   bit_wise_not
   bit_wise_or
   bit_wise_xor
   shift_left
   shift_left_checked
   shift_right
   shift_right_checked

Rounding Functions
------------------

Rounding functions displace numeric inputs to an approximate value with a simpler
representation based on the rounding criterion.

.. autosummary::
   :toctree: ../generated/

   ceil
   floor
   round
   round_to_multiple
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
   logb
   logb_checked

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
   and_not
   and_not_kleene
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

String Transforms
-----------------

.. autosummary::
   :toctree: ../generated/

   ascii_capitalize
   ascii_lower
   ascii_reverse
   ascii_swapcase
   ascii_title
   ascii_upper
   binary_length
   binary_repeat
   binary_replace_slice
   binary_reverse
   replace_substring
   replace_substring_regex
   utf8_capitalize
   utf8_length
   utf8_lower
   utf8_replace_slice
   utf8_reverse
   utf8_swapcase
   utf8_title
   utf8_upper

String Padding
--------------

.. autosummary::
   :toctree: ../generated/

   ascii_center
   ascii_lpad
   ascii_rpad
   utf8_center
   utf8_lpad
   utf8_rpad

String Trimming
---------------

.. autosummary::
   :toctree: ../generated/

   ascii_ltrim
   ascii_ltrim_whitespace
   ascii_rtrim
   ascii_rtrim_whitespace
   ascii_trim
   ascii_trim_whitespace
   utf8_ltrim
   utf8_ltrim_whitespace
   utf8_rtrim
   utf8_rtrim_whitespace
   utf8_trim
   utf8_trim_whitespace

String Splitting
----------------

.. autosummary::
   :toctree: ../generated/

   ascii_split_whitespace
   split_pattern
   split_pattern_regex
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

String Slicing
--------------

.. autosummary::
   :toctree: ../generated/

   binary_slice
   utf8_slice_codeunits

Containment Tests
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
   indices_nonzero

Categorizations
---------------

.. autosummary::
   :toctree: ../generated/

   is_finite
   is_inf
   is_nan
   is_null
   is_valid
   true_unless_null

Selecting / Multiplexing
------------------------

.. autosummary::
   :toctree: ../generated/

   case_when
   choose
   coalesce
   if_else

Conversions
-----------

.. autosummary::
   :toctree: ../generated/

   cast
   ceil_temporal
   floor_temporal
   round_temporal
   strftime
   strptime

Temporal Component Extraction
-----------------------------

.. autosummary::
   :toctree: ../generated/

   day
   day_of_week
   day_of_year
   hour
   iso_week
   iso_year
   iso_calendar
   is_leap_year
   microsecond
   millisecond
   minute
   month
   nanosecond
   quarter
   second
   subsecond
   us_week
   us_year
   week
   year
   year_month_day

Temporal Difference
-------------------

.. autosummary::
   :toctree: ../generated/

   day_time_interval_between
   days_between
   hours_between
   microseconds_between
   milliseconds_between
   minutes_between
   month_day_nano_interval_between
   month_interval_between
   nanoseconds_between
   quarters_between
   seconds_between
   weeks_between
   years_between

Timezone Handling
-----------------

.. autosummary::
   :toctree: ../generated/

   assume_timezone

Associative Transforms
----------------------

.. autosummary::
   :toctree: ../generated/

   dictionary_encode
   unique
   value_counts

Selections
----------

.. autosummary::
   :toctree: ../generated/

   array_filter
   array_take
   drop_null
   filter
   take

Sorts and Partitions
--------------------

.. autosummary::
   :toctree: ../generated/

   array_sort_indices
   partition_nth_indices
   select_k_unstable
   sort_indices

Structural Transforms
---------------------

.. autosummary::
   :toctree: ../generated/

   fill_null
   fill_null_backward
   fill_null_forward
   list_element
   list_flatten
   list_parent_indices
   list_slice
   list_value_length
   make_struct
   map_lookup
   replace_with_mask
   struct_field

Compute Options
---------------

.. autosummary::
   :toctree: ../generated/

   ArraySortOptions
   AssumeTimezoneOptions
   CastOptions
   CountOptions
   CountOptions
   CumulativeSumOptions
   DayOfWeekOptions
   DictionaryEncodeOptions
   ElementWiseAggregateOptions
   ExtractRegexOptions
   FilterOptions
   IndexOptions
   JoinOptions
   ListSliceOptions
   MakeStructOptions
   MapLookupOptions
   MatchSubstringOptions
   ModeOptions
   NullOptions
   PadOptions
   PartitionNthOptions
   QuantileOptions
   ReplaceSliceOptions
   ReplaceSubstringOptions
   RoundOptions
   RoundTemporalOptions
   RoundToMultipleOptions
   ScalarAggregateOptions
   ScalarAggregateOptions
   SelectKOptions
   SetLookupOptions
   SliceOptions
   SortOptions
   SplitOptions
   SplitPatternOptions
   StrftimeOptions
   StrptimeOptions
   StructFieldOptions
   TakeOptions
   TDigestOptions
   TDigestOptions
   TrimOptions
   VarianceOptions
   WeekOptions

User-Defined Functions
----------------------

.. autosummary::
   :toctree: ../generated/

   register_scalar_function
   ScalarUdfContext
