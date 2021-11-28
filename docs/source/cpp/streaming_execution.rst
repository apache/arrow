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

==========================
Streaming execution engine
==========================

.. warning::

    The streaming execution engine is experimental, and a stable API
    is not yet guaranteed.

Motivation
----------

For many complex computations, successive direct :ref:`invocation of
compute functions <invoking-compute-functions>` is not feasible
in either memory or computation time. Doing so causes all intermediate
data to be fully materialized. To facilitate arbitrarily large inputs
and more efficient resource usage, Arrow also provides a streaming query
engine with which computations can be formulated and executed.

.. image:: simple_graph.svg
   :alt: An example graph of a streaming execution workflow.

:class:`ExecNode` is provided to reify the graph of operations in a query.
Batches of data (:struct:`ExecBatch`) flow along edges of the graph from
node to node. Structuring the API around streams of batches allows the
working set for each node to be tuned for optimal performance independent
of any other nodes in the graph. Each :class:`ExecNode` processes batches
as they are pushed to it along an edge of the graph by upstream nodes
(its inputs), and pushes batches along an edge of the graph to downstream
nodes (its outputs) as they are finalized.

.. seealso::

   `SHAIKHHA, A., DASHTI, M., & KOCH, C.
   (2018). Push versus pull-based loop fusion in query engines.
   Journal of Functional Programming, 28.
   <https://doi.org/10.1017/s0956796818000102>`_

Overview
--------

:class:`ExecNode`
  Each node in the graph is an implementation of the :class:`ExecNode` interface.

:class:`ExecPlan`
  A set of :class:`ExecNode` is contained and (to an extent) coordinated by an
  :class:`ExecPlan`.

:class:`ExecFactoryRegistry`
  Instances of :class:`ExecNode` are constructed by factory functions held
  in a :class:`ExecFactoryRegistry`.

:class:`ExecNodeOptions`
  Heterogenous parameters for factories of :class:`ExecNode` are bundled in an
  :class:`ExecNodeOptions`.

:struct:`Declaration`
  ``dplyr``-inspired helper for efficient construction of an :class:`ExecPlan`.

:struct:`ExecBatch`
  A lightweight container for a single chunk of data in the Arrow format. In
  contrast to :class:`RecordBatch`, :struct:`ExecBatch` is intended for use
  exclusively in a streaming execution context (for example, it doesn't have a
  corresponding Python binding). Furthermore columns which happen to have a
  constant value may be represented by a :class:`Scalar` instead of an
  :class:`Array`. In addition, :struct:`ExecBatch` may carry
  execution-relevant properties including a guaranteed-true-filter
  for :class:`Expression` simplification.


An example :class:`ExecNode` implementation which simply passes all input batches
through unchanged::

    class PassthruNode : public ExecNode {
     public:
      // InputReceived is the main entry point for ExecNodes. It is invoked
      // by an input of this node to push a batch here for processing.
      void InputReceived(ExecNode* input, ExecBatch batch) override {
        // Since this is a passthru node we simply push the batch to our
        // only output here.
        outputs_[0]->InputReceived(this, batch);
      }

      // ErrorReceived is called by an input of this node to report an error.
      // ExecNodes should always forward errors to their outputs unless they
      // are able to fully handle the error (this is rare).
      void ErrorReceived(ExecNode* input, Status error) override {
        outputs_[0]->ErrorReceived(this, error);
      }

      // InputFinished is used to signal how many batches will ultimately arrive.
      // It may be called with any ordering relative to InputReceived/ErrorReceived.
      void InputFinished(ExecNode* input, int total_batches) override {
        outputs_[0]->InputFinished(this, total_batches);
      }

      // ExecNodes may request that their inputs throttle production of batches
      // until they are ready for more, or stop production if no further batches
      // are required.  These signals should typically be forwarded to the inputs
      // of the ExecNode.
      void ResumeProducing(ExecNode* output) override { inputs_[0]->ResumeProducing(this); }
      void PauseProducing(ExecNode* output) override { inputs_[0]->PauseProducing(this); }
      void StopProducing(ExecNode* output) override { inputs_[0]->StopProducing(this); }

      // An ExecNode has a single output schema to which all its batches conform.
      using ExecNode::output_schema;

      // ExecNodes carry basic introspection for debugging purposes
      const char* kind_name() const override { return "PassthruNode"; }
      using ExecNode::label;
      using ExecNode::SetLabel;
      using ExecNode::ToString;

      // An ExecNode holds references to its inputs and outputs, so it is possible
      // to walk the graph of execution if necessary.
      using ExecNode::inputs;
      using ExecNode::outputs;

      // StartProducing() and StopProducing() are invoked by an ExecPlan to
      // coordinate the graph-wide execution state.  These do not need to be
      // forwarded to inputs or outputs.
      Status StartProducing() override { return Status::OK(); }
      void StopProducing() override {}
      Future<> finished() override { return inputs_[0]->finished(); }
    };

Note that each method which is associated with an edge of the graph must be invoked
with an ``ExecNode*`` to identify the node which invoked it. For example, in an
:class:`ExecNode` which implements ``JOIN`` this tagging might be used to differentiate
between batches from the left or right inputs.
``InputReceived``, ``ErrorReceived``, ``InputFinished`` may only be invoked by
the inputs of a node, while ``ResumeProducing``, ``PauseProducing``, ``StopProducing``
may only be invoked by outputs of a node.

:class:`ExecPlan` contains the associated instances of :class:`ExecNode`
and is used to start and stop execution of all nodes and for querying/awaiting
their completion::

    // construct an ExecPlan first to hold your nodes
    ARROW_ASSIGN_OR_RAISE(auto plan, ExecPlan::Make(default_exec_context()));

    // ... add nodes to your ExecPlan

    // start all nodes in the graph
    ARROW_RETURN_NOT_OK(plan->StartProducing());

    SetUserCancellationCallback([plan] {
      // stop all nodes in the graph
      plan->StopProducing();
    });

    // Complete will be marked finished when all nodes have run to completion
    // or acknowledged a StopProducing() signal. The ExecPlan should be kept
    // alive until this future is marked finished.
    Future<> complete = plan->finished();


Constructing ``ExecPlan`` objects
---------------------------------

.. warning::

    The following will be superceded by construction from Compute IR, see ARROW-14074.

None of the concrete implementations of :class:`ExecNode` are exposed
in headers, so they can't be constructed directly outside the
translation unit where they are defined. Instead, factories to
create them are provided in an extensible registry. This structure
provides a number of benefits:

- This enforces consistent construction.
- It decouples implementations from consumers of the interface
  (for example: we have two classes for scalar and grouped aggregate,
  we can choose which to construct within the single factory by
  checking whether grouping keys are provided)
- This expedites integration with out-of-library extensions. For example
  "scan" nodes are implemented in the separate ``libarrow_dataset.so`` library.
- Since the class is not referencable outside the translation unit in which it
  is defined, compilers can optimize more aggressively.

Factories of :class:`ExecNode` can be retrieved by name from the registry.
The default registry is available through
:func:`arrow::compute::default_exec_factory_registry()`
and can be queried for the built-in factories::

    // get the factory for "filter" nodes:
    ARROW_ASSIGN_OR_RAISE(auto make_filter,
                          default_exec_factory_registry()->GetFactory("filter"));

    // factories take three arguments:
    ARROW_ASSIGN_OR_RAISE(ExecNode* filter_node, *make_filter(
        // the ExecPlan which should own this node
        plan.get(),

        // nodes which will send batches to this node (inputs)
        {scan_node},

        // parameters unique to "filter" nodes
        FilterNodeOptions{filter_expression}));

    // alternative shorthand:
    ARROW_ASSIGN_OR_RAISE(filter_node, MakeExecNode("filter",
        plan.get(), {scan_node}, FilterNodeOptions{filter_expression});

Factories can also be added to the default registry as long as they are
convertible to ``std::function<Result<ExecNode*>(
ExecPlan*, std::vector<ExecNode*>, const ExecNodeOptions&)>``.

To build an :class:`ExecPlan` representing a simple pipeline which
reads from a :class:`RecordBatchReader` then filters, projects, and
writes to disk::

    std::shared_ptr<RecordBatchReader> reader = GetStreamOfBatches();
    ExecNode* source_node = *MakeExecNode("source", plan.get(), {},
                                          SourceNodeOptions::FromReader(
                                              reader,
                                              GetCpuThreadPool()));

    ExecNode* filter_node = *MakeExecNode("filter", plan.get(), {source_node},
                                          FilterNodeOptions{
                                            greater(field_ref("score"), literal(3))
                                          });

    ExecNode* project_node = *MakeExecNode("project", plan.get(), {filter_node},
                                           ProjectNodeOptions{
                                             {add(field_ref("score"), literal(1))},
                                             {"score + 1"}
                                           });

    arrow::dataset::internal::Initialize();
    MakeExecNode("write", plan.get(), {project_node},
                 WriteNodeOptions{/*base_dir=*/"/dat", /*...*/});

:struct:`Declaration` is a `dplyr <https://dplyr.tidyverse.org>`_-inspired
helper which further decreases the boilerplate associated with populating
an :class:`ExecPlan` from C++::

    arrow::dataset::internal::Initialize();

    std::shared_ptr<RecordBatchReader> reader = GetStreamOfBatches();
    ASSERT_OK(Declaration::Sequence(
                  {
                      {"source", SourceNodeOptions::FromReader(
                           reader,
                           GetCpuThreadPool())},
                      {"filter", FilterNodeOptions{
                           greater(field_ref("score"), literal(3))}},
                      {"project", ProjectNodeOptions{
                           {add(field_ref("score"), literal(1))},
                           {"score + 1"}}},
                      {"write", WriteNodeOptions{/*base_dir=*/"/dat", /*...*/}},
                  })
                  .AddToPlan(plan.get()));

Note that a source node can wrap anything which resembles a stream of batches.
For example, `PR#11032 <https://github.com/apache/arrow/pull/11032>`_ adds
support for use of a `DuckDB <https://duckdb.org>`_ query as a source node.
Similarly, a sink node can wrap anything which absorbs a stream of batches.
In the example above we're writing completed
batches to disk. However we can also collect these in memory into a :class:`Table`
or forward them to a :class:`RecordBatchReader` as an out-of-graph stream.
This flexibility allows an :class:`ExecPlan` to be used as streaming middleware
between any endpoints which support Arrow formatted batches.

An :class:`arrow::dataset::Dataset` can also be wrapped as a source node which
pushes all the dataset's batches into an :class:`ExecPlan`. This factory is added
to the default registry with the name ``"scan"`` by calling
``arrow::dataset::internal::Initialize()``::

    arrow::dataset::internal::Initialize();

    std::shared_ptr<Dataset> dataset = GetDataset();

    ASSERT_OK(Declaration::Sequence(
                  {
                      {"scan", ScanNodeOptions{dataset,
                         /* push down predicate, projection, ... */}},
                      {"filter", FilterNodeOptions{/* ... */}},
                      // ...
                  })
                  .AddToPlan(plan.get()));

Datasets may be scanned multiple times; just make multiple scan
nodes from that dataset. (Useful for a self-join, for example.)
Note that producing two scan nodes like this will perform all
reads and decodes twice.
