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

.. _stream_execution_sink_docs:

====
Sink
====

``sink`` operation provides output and is the final node of a streaming 
execution definition. :class:`arrow::compute::SinkNodeOptions` interface is used to pass 
the required options. Similar to the source operator the sink operator exposes the output
with a function that returns a record batch future each time it is called.  It is expected the
caller will repeatedly call this function until the generator function is exhausted (returns
``arrow::util::optional::nullopt``).  If this function is not called often enough then record batches
will accumulate in memory.  An execution plan should only have one
"terminal" node (one sink node).  An :class:`ExecPlan` can terminate early due to cancellation or 
an error, before the output is fully consumed. However, the plan can be safely destroyed independently
of the sink, which will hold the unconsumed batches by `exec_plan->finished()`.

Example:

.. literalinclude:: ../../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Source Example)
  :end-before: (Doc section: Source Example)
  :linenos:
  :lineno-match:
