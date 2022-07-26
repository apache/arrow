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

.. _stream_execution_consuming_sink_docs:

==============
Consuming Sink
==============

``consuming_sink`` operator is a sink operation containing consuming operation within the
execution plan (i.e. the exec plan should not complete until the consumption has completed).
Unlike the ``sink`` node this node takes in a callback function that is expected to consume the
batch.  Once this callback has finished the execution plan will no longer hold any reference to
the batch.
The consuming function may be called before a previous invocation has completed.  If the consuming
function does not run quickly enough then many concurrent executions could pile up, blocking the
CPU thread pool.  The execution plan will not be marked finished until all consuming function callbacks
have been completed.
Once all batches have been delivered the execution plan will wait for the `finish` future to complete
before marking the execution plan finished.  This allows for workflows where the consumption function
converts batches into async tasks (this is currently done internally for the dataset write node).

Example::

  // define a Custom SinkNodeConsumer
  std::atomic<uint32_t> batches_seen{0};
  arrow::Future<> finish = arrow::Future<>::Make();
  struct CustomSinkNodeConsumer : public cp::SinkNodeConsumer {

      CustomSinkNodeConsumer(std::atomic<uint32_t> *batches_seen, arrow::Future<>finish): 
      batches_seen(batches_seen), finish(std::move(finish)) {}
      // Consumption logic can be written here
      arrow::Status Consume(cp::ExecBatch batch) override {
      // data can be consumed in the expected way
      // transfer to another system or just do some work 
      // and write to disk
      (*batches_seen)++;
      return arrow::Status::OK();
      }

      arrow::Future<> Finish() override { return finish; }

      std::atomic<uint32_t> *batches_seen;
      arrow::Future<> finish;
      
  };
  
  std::shared_ptr<CustomSinkNodeConsumer> consumer =
          std::make_shared<CustomSinkNodeConsumer>(&batches_seen, finish);

  arrow::compute::ExecNode *consuming_sink;

  ARROW_ASSIGN_OR_RAISE(consuming_sink, MakeExecNode("consuming_sink", plan.get(),
      {source}, cp::ConsumingSinkNodeOptions(consumer)));


Example:

.. literalinclude:: ../../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: ConsumingSink Example)
  :end-before: (Doc section: ConsumingSink Example)
  :linenos:
  :lineno-match:
