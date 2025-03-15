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

Compute Functions
=================

The Arrow C++ library provides many compute functions that can operate on arrays or scalars.
They are organized into several categories such as arithmetic, logical, comparison, conditional,
and string operations. Below is a representative overview of common function categories.

Arithmetic Functions
--------------------

.. doxygengroup:: compute-arithmetic
   :content-only:
   :members:

Boolean and Logical Functions
-----------------------------

.. doxygengroup:: compute-boolean
   :content-only:
   :members:

Comparison (Relational) Functions
---------------------------------

.. doxygengroup:: compute-comparison
   :content-only:
   :members:

Conditional Functions
---------------------

.. doxygengroup:: compute-conditional
   :content-only:
   :members:

Null and Mask-Related Functions
-------------------------------

.. doxygengroup:: compute-null-mask
   :content-only:
   :members:

Temporal Functions
------------------

.. doxygengroup:: compute-temporal
   :content-only:
   :members:

String and Binary Functions
---------------------------

.. doxygengroup:: compute-string-binary
   :content-only:
   :members:

Casting Functions
-----------------

.. doxygengroup:: compute-casting
   :content-only:
   :members:

Reshaping, Filtering, and Indexing
----------------------------------

.. doxygengroup:: compute-reshaping
   :content-only:
   :members:

Aggregation Functions
---------------------

.. doxygengroup:: compute-aggregation
   :content-only:
   :members:

Rounding and Mathematical Functions
-----------------------------------

.. doxygengroup:: compute-rounding
   :content-only:
   :members:

Random / Utility Functions
--------------------------

.. doxygengroup:: compute-random
   :content-only:
   :members:

Datum class
-----------

.. doxygenclass:: arrow::Datum
   :members:

Abstract Function classes
-------------------------

.. doxygengroup:: compute-functions
   :content-only:
   :members:

Function execution
------------------

.. doxygengroup:: compute-function-executor
   :content-only:
   :members:

Function registry
-----------------

.. doxygenclass:: arrow::compute::FunctionRegistry
   :members:

.. doxygenfunction:: arrow::compute::GetFunctionRegistry

Convenience functions
---------------------

.. doxygengroup:: compute-call-function
   :content-only:

Concrete options classes
------------------------

.. doxygengroup:: compute-concrete-options
   :content-only:
   :members:
   :undoc-members:

.. TODO: List concrete function invocation shortcuts?

Compute Expressions
-------------------

.. doxygengroup:: expression-core
   :content-only:
   :members:

.. doxygengroup:: expression-convenience
   :content-only:
   :members:
   :undoc-members:

.. doxygengroup:: expression-passes
   :content-only:
   :members: