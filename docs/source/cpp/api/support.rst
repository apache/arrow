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

===================
Programming Support
===================

General information
===================

.. doxygenfunction:: arrow::GetBuildInfo

.. doxygenstruct:: arrow::BuildInfo
   :members:

.. doxygenfunction:: arrow::GetRuntimeInfo

.. doxygenstruct:: arrow::RuntimeInfo
   :members:

Macro definitions
-----------------

These can be useful if you need to decide between different blocks of code
*at compile time* (for example to conditionally take advantage of a recently
introduced API).

.. c:macro:: ARROW_VERSION_MAJOR

   The Arrow major version number, for example ``7`` for Arrow 7.0.1.

.. c:macro:: ARROW_VERSION_MINOR

   The Arrow minor version number, for example ``0`` for Arrow 7.0.1.

.. c:macro:: ARROW_VERSION_PATCH

   The Arrow patch version number, for example ``1`` for Arrow 7.0.1.

.. c:macro:: ARROW_VERSION

   A consolidated integer representing the full Arrow version in an easily
   comparable form, computed with the formula:
   ``((ARROW_VERSION_MAJOR * 1000) + ARROW_VERSION_MINOR) * 1000 + ARROW_VERSION_PATCH``.

   For example, this would choose a different block of code if the code is
   being compiled against a Arrow version equal to or greater than 7.0.1::

      #if ARROW_VERSION >= 7000001
      // Arrow 7.0.1 or later...
      #endif

.. c:macro:: ARROW_VERSION_STRING

   A human-readable string representation of the Arrow version, such as
   ``"7.0.1"``.


Runtime Configuration
=====================

.. doxygenstruct:: arrow::ArrowGlobalOptions
   :members:


.. doxygenfunction:: arrow::Initialize


Error return and reporting
==========================

.. doxygenclass:: arrow::Status
   :members:

.. doxygenclass:: arrow::StatusDetail
   :members:

.. doxygenclass:: arrow::Result
   :members:

.. doxygenclass:: parquet::ParquetException
   :members:

Functional macros for error-based control flow
----------------------------------------------

.. doxygendefine:: ARROW_RETURN_NOT_OK

.. doxygendefine:: ARROW_ASSIGN_OR_RAISE

.. doxygendefine:: PARQUET_THROW_NOT_OK

.. doxygendefine:: PARQUET_ASSIGN_OR_THROW
