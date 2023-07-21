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
