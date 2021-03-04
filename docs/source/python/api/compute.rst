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
   mean
   min_max
   mode
   stddev
   sum
   variance

Arithmetic Functions
--------------------

By default these functions do not detect overflow. Each function is also
available in an overflow-checking variant, suffixed ``_checked``, which 
throws an ``ArrowInvalid`` exception when overflow is detected.

.. autosummary::
   :toctree: ../generated/

   add
   add_checked
   divide
   divide_checked
   multiply
   multiply_checked
   subtract
   subtract_checked

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

String Transforms
-----------------

.. autosummary::
   :toctree: ../generated/

   ascii_lower
   ascii_upper
   utf8_lower
   utf8_upper

Containment tests
-----------------

.. autosummary::
   :toctree: ../generated/

   index_in
   is_in
   match_substring

Conversions
-----------

.. autosummary::
   :toctree: ../generated/

   cast
   strptime

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
   fill_null
   is_null
   is_valid
   list_value_length
   list_flatten
   list_parent_indices
