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

.. _stream_execution_project_docs:

=======
Project
=======

``project`` operation rearranges, deletes, transforms, and creates columns.
Each output column is computed by evaluating an expression
against the source record batch. This is exposed via 
:class:`arrow::compute::ProjectNodeOptions` which requires,
an :class:`arrow::compute::Expression` and name for each of the output columns (if names are not
provided, the string representations of exprs will be used).  

Example:

.. literalinclude:: ../../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Project Example)
  :end-before: (Doc section: Project Example)
  :linenos:
  :lineno-match:
