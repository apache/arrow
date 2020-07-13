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

===================
Programming Support
===================

General information
-------------------

.. doxygenfunction:: arrow::GetBuildInfo
   :project: arrow_cpp

.. doxygenstruct:: arrow::BuildInfo
   :project: arrow_cpp
   :members:

Error return and reporting
--------------------------

.. doxygenclass:: arrow::Status
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::StatusDetail
   :project: arrow_cpp
   :members:

.. doxygenclass:: arrow::Result
   :project: arrow_cpp
   :members:

.. doxygenclass:: parquet::ParquetException
   :project: arrow_cpp
   :members:

.. doxygendefine:: ARROW_RETURN_NOT_OK

.. doxygendefine:: ARROW_ASSIGN_OR_RAISE

.. doxygendefine:: PARQUET_THROW_NOT_OK

.. doxygendefine:: PARQUET_ASSIGN_OR_THROW
