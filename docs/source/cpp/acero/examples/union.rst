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

.. _stream_execution_union_docs:

=====
Union
=====

``union`` merges multiple data streams with the same schema into one, similar to 
a SQL ``UNION ALL`` clause.

The following example demonstrates how this can be achieved using 
two data sources.

Example:

.. literalinclude:: ../../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Union Example)
  :end-before: (Doc section: Union Example)
  :linenos:
  :lineno-match:
