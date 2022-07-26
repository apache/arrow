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

.. _stream_execution_source_docs:

======
Source
======

A ``source`` operation can be considered as an entry point to create a streaming execution plan. 
:class:`arrow::compute::SourceNodeOptions` are used to create the ``source`` operation.  The
``source`` operation is the most generic and flexible type of source currently available but it can
be quite tricky to configure.  To process data from files the scan operation is likely a simpler choice.

The source node requires some kind of function that can be called to poll for more data.  This
function should take no arguments and should return an
``arrow::Future<std::shared_ptr<arrow::util::optional<arrow::RecordBatch>>>``.
This function might be reading a file, iterating through an in memory structure, or receiving data
from a network connection.  The arrow library refers to these functions as ``arrow::AsyncGenerator``
and there are a number of utilities for working with these functions.  For this example we use 
a vector of record batches that we've already stored in memory.
In addition, the schema of the data must be known up front.  Acero must know the schema of the data
at each stage of the execution graph before any processing has begun.  This means we must supply the
schema for a source node separately from the data itself.

Here we define a struct to hold the data generator definition. This includes in-memory batches, schema
and a function that serves as a data generator :

.. literalinclude:: ../../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: BatchesWithSchema Definition)
  :end-before: (Doc section: BatchesWithSchema Definition)
  :linenos:
  :lineno-match:

Generating sample batches for computation:

.. literalinclude:: ../../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: MakeBasicBatches Definition)
  :end-before: (Doc section: MakeBasicBatches Definition)
  :linenos:
  :lineno-match:

Example of using ``source`` (usage of sink is explained in detail in :ref:`sink<stream_execution_sink_docs>`):

.. literalinclude:: ../../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Source Example)
  :end-before: (Doc section: Source Example)
  :linenos:
  :lineno-match:
  