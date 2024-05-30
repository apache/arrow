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

.. _api.substrait:
.. currentmodule:: pyarrow.substrait

Substrait
=========

These functions allow you to execute Substrait plans against Arrow data

Query Execution
---------------

.. autosummary::
   :toctree: ../generated/

   run_query

Expression Serialization
------------------------

These functions allow for serialization and deserialization of pyarrow
compute expressions.

.. autosummary::
   :toctree: ../generated/

   BoundExpressions
   deserialize_expressions
   serialize_expressions

Utility
-------

.. autosummary::
   :toctree: ../generated/

   get_supported_functions
