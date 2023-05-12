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
.. cpp:namespace:: arrow::acero

==================
Acero User's Guide
==================

This page describes how to use Acero.  It's recommended that you read the
overview first and familiarize yourself with the basic concepts.

Using Acero
===========

The basic workflow for Acero is this:

#. First, create a graph of :class:`Declaration` objects describing the plan
 
#. Call one of the DeclarationToXyz methods to execute the Declaration.

   a. A new ExecPlan is created from the graph of Declarations.  Each Declaration will correspond to one
      ExecNode in the plan.  In addition, a sink node will be added, depending on which DeclarationToXyz method
      was used.

   b. The ExecPlan is executed.  Typically this happens as part of the DeclarationToXyz call but in 
      DeclarationToReader the reader is returned before the plan is finished executing.

   c. Once the plan is finished it is destroyed

Creating a Plan
===============

Using Substrait
---------------

Substrait is the preferred mechanism for creating a plan (graph of :class:`Declaration`).  There are a few
reasons for this:

* Substrait producers spend a lot of time and energy in creating user-friendly APIs for producing complex
  execution plans in a simple way.  For example, the ``pivot_wider`` operation can be achieved using a complex
  series of ``aggregate`` nodes.  Rather than create all of those ``aggregate`` nodes by hand a producer will
  give you a much simpler API.

* If you are using Substrait then you can easily switch out to any other Substrait-consuming engine should you
  at some point find that it serves your needs better than Acero.

* We hope that tools will eventually emerge for Substrait-based optimizers and planners.  By using Substrait
  you will be making it much easier to use these tools in the future.

You could create the Substrait plan yourself but you'll probably have a much easier time finding an existing
Substrait producer.  For example, you could use `ibis-substrait <https://github.com/ibis-project/ibis-substrait>`_
to easily create Substrait plans from python expressions.  There are a few different tools that are able to create
Substrait plans from SQL.  Eventually, we hope that C++ based Substrait producers will emerge.  However, we
are not aware of any at this time.

Detailed instructions on creating an execution plan from Substrait can be found in
:ref:`the Substrait page<acero-substrait>`

Programmatic Plan Creation
--------------------------

Creating an execution plan programmatically is simpler than creating a plan from Substrait, though loses some of
the flexibility and future-proofing guarantees.  The simplest way to create a Declaration is to simply instantiate
one.  You will need the name of the declaration, a vector of inputs, and an options object.  For example:

.. literalinclude:: ../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Project Example)
  :end-before: (Doc section: Project Example)
  :linenos:
  :lineno-match:

The above code creates a scan declaration (which has no inputs) and a project declaration (using the scan as
input).  This is simple enough but we can make it slightly easier.  If you are creating a linear sequence of
declarations (like in the above example) then you can also use the :func:`Declaration::Sequence` function.

.. literalinclude:: ../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Project Sequence Example)
  :end-before: (Doc section: Project Sequence Example)
  :linenos:
  :lineno-match:

There are many more examples of programmatic plan creation later in this document.

Executing a Plan
================

There are a number of different methods that can be used to execute a declaration.  Each one provides the
data in a slightly different form.  Since all of these methods start with ``DeclarationTo...`` this guide
will often refer to these methods as the ``DeclarationToXyz`` methods.

DeclarationToTable
------------------

The :func:`DeclarationToTable` method will accumulate all of the results into a single :class:`arrow::Table`.
This is perhaps the simplest way to collect results from Acero.  The main disadvantage to this approach is
that it requires accumulating all results into memory.

.. note::

   Acero processes large datasets in small chunks.  This is described in more detail in the developer's guide.
   As a result, you may be surprised to find that a table collected with DeclarationToTable is chunked
   differently than your input.  For example, your input might be a large table with a single chunk with 2
   million rows.  Your output table might then have 64 chunks with 32Ki rows each.  There is a current request
   to specify the chunk size for the output in `GH-15155 <https://github.com/apache/arrow/issues/15155>`_.

DeclarationToReader
-------------------

The :func:`DeclarationToReader` method allows you to iteratively consume the results.  It will create an
:class:`arrow::RecordBatchReader` which you can read from at your leisure.  If you do not read from the
reader quickly enough then backpressure will be applied and the execution plan will pause.  Closing the
reader will cancel the running execution plan and the reader's destructor will wait for the execution plan
to finish whatever it is doing and so it may block.

DeclarationToStatus
-------------------

The :func:`DeclarationToStatus` method is useful if you want to run the plan but do not actually want to
consume the results.  For example, this is useful when benchmarking or when the plan has side effects such
as a dataset write node.  If the plan generates any results then they will be immediately discarded.

Running a Plan Directly
-----------------------

If one of the ``DeclarationToXyz`` methods is not sufficient for some reason then it is possible to run a plan
directly.  This should only be needed if you are doing something unique.  For example, if you have created a
custom sink node or if you need a plan that has multiple outputs.

.. note::
   In academic literature and many existing systems there is a general assumption that an execution plan has
   at most one output.  There are some things in Acero, such as the DeclarationToXyz methods, which will expect
   this.  However, there is nothing in the design that strictly prevents having multiple sink nodes.

Detailed instructions on how to do this are out of scope for this guide but the rough steps are:

1. Create a new :class:`ExecPlan` object.
2. Add sink nodes to your graph of :class:`Declaration` objects (this is the only type you will need
   to create declarations for sink nodes)
3. Use :func:`Declaration::AddToPlan` to add your declaration to your plan (if you have more than one output
   then you will not be able to use this method and will need to add your nodes one at a time)
4. Validate the plan with :func:`ExecPlan::Validate`
5. Start the plan with :func:`ExecPlan::StartProducing`
6. Wait for the future returned by :func:`ExecPlan::finished` to complete.

Providing Input
===============

Input data for an exec plan can come from a variety of sources.  It is often read from files stored on some
kind of filesystem.  It is also common for input to come from in-memory data.  In-memory data is typical, for
example, in a pandas-like frontend.  Input could also come from network streams like a Flight request.  Acero
can support all of these cases and can even support unique and custom situations not mentioned here.

There are pre-defined source nodes that cover the most common input scenarios.  These are listed below.  However,
if your source data is unique then you will need to use the generic ``source`` node.  This node expects you to
provide an asycnhronous stream of batches and is covered in more detail :ref:`here <stream_execution_source_docs>`.

.. _ExecNode List:

Available ``ExecNode`` Implementations
======================================

The following tables quickly summarize the available operators.

Sources
-------

These nodes can be used as sources of data

.. list-table:: Source Nodes
   :widths: 25 25 50
   :header-rows: 1

   * - Factory Name
     - Options
     - Brief Description
   * - ``source``
     - :class:`SourceNodeOptions`
     - A generic source node that wraps an asynchronous stream of data (:ref:`example <stream_execution_source_docs>`)
   * - ``table_source``
     - :class:`TableSourceNodeOptions`
     - Generates data from an :class:`arrow::Table` (:ref:`example <stream_execution_table_source_docs>`)
   * - ``record_batch_source``
     - :class:`RecordBatchSourceNodeOptions`
     - Generates data from an iterator of :class:`arrow::RecordBatch`
   * - ``record_batch_reader_source``
     - :class:`RecordBatchReaderSourceNodeOptions`
     - Generates data from an :class:`arrow::RecordBatchReader`
   * - ``exec_batch_source``
     - :class:`ExecBatchSourceNodeOptions`
     - Generates data from an iterator of :class:`arrow::compute::ExecBatch`
   * - ``array_vector_source``
     - :class:`ArrayVectorSourceNodeOptions`
     - Generates data from an iterator of vectors of :class:`arrow::Array`
   * - ``scan``
     - :class:`arrow::dataset::ScanNodeOptions`
     - Generates data from an :class:`arrow::dataset::Dataset` (requires the datasets module)
       (:ref:`example <stream_execution_scan_docs>`)

Compute Nodes
-------------

These nodes perform computations on data and may transform or reshape the data

.. list-table:: Compute Nodes
   :widths: 25 25 50
   :header-rows: 1

   * - Factory Name
     - Options
     - Brief Description
   * - ``filter``
     - :class:`FilterNodeOptions`
     - Removes rows that do not match a given filter expression
       (:ref:`example <stream_execution_filter_docs>`)
   * - ``project``
     - :class:`ProjectNodeOptions`
     - Creates new columns by evaluating compute expressions.  Can also drop and reorder columns
       (:ref:`example <stream_execution_project_docs>`)
   * - ``aggregate``
     - :class:`AggregateNodeOptions`
     - Calculates summary statistics across the entire input stream or on groups of data
       (:ref:`example <stream_execution_aggregate_docs>`)
   * - ``pivot_longer``
     - :class:`PivotLongerNodeOptions`
     - Reshapes data by converting some columns into additional rows

Arrangement Nodes
-----------------

These nodes reorder, combine, or slice streams of data

.. list-table:: Arrangement Nodes
   :widths: 25 25 50
   :header-rows: 1

   * - Factory Name
     - Options
     - Brief Description
   * - ``hash_join``
     - :class:`HashJoinNodeOptions`
     - Joins two inputs based on common columns (:ref:`example <stream_execution_hashjoin_docs>`)
   * - ``asofjoin``
     - :class:`AsofJoinNodeOptions`
     - Joins multiple inputs to the first input based on a common ordered column (often time)
   * - ``union``
     - N/A
     - Merges two inputs with identical schemas (:ref:`example <stream_execution_union_docs>`)
   * - ``order_by``
     - :class:`OrderByNodeOptions`
     - Reorders a stream
   * - ``fetch``
     - :class:`FetchNodeOptions`
     - Slices a range of rows from a stream

Sink Nodes
----------

These nodes terminate a plan.  Users do not typically create sink nodes as they are
selected based on the DeclarationToXyz method used to consume the plan.  However, this
list may be useful for those developing new sink nodes or using Acero in advanced ways.

.. list-table:: Sink Nodes
   :widths: 25 25 50
   :header-rows: 1

   * - Factory Name
     - Options
     - Brief Description
   * - ``sink``
     - :class:`SinkNodeOptions`
     - Collects batches into a FIFO queue with optional backpressure
   * - ``write``
     - :class:`arrow::dataset::WriteNodeOptions`
     - Writes batches to a filesystem (:ref:`example <stream_execution_write_docs>`)
   * - ``consuming_sink``
     - :class:`ConsumingSinkNodeOptions`
     - Consumes batches using a user provided callback function
   * - ``table_sink``
     - :class:`TableSinkNodeOptions`
     - Collects batches into an :class:`arrow::Table`
   * - ``order_by_sink``
     - :class:`OrderBySinkNodeOptions`
     - Deprecated
   * - ``select_k_sink``
     - :class:`SelectKSinkNodeOptions`
     - Deprecated

Examples
========

The rest of this document contains example execution plans.  Each example highlights the behavior
of a specific execution node.

.. _stream_execution_source_docs:

``source``
----------

A ``source`` operation can be considered as an entry point to create a streaming execution plan. 
:class:`SourceNodeOptions` are used to create the ``source`` operation.  The
``source`` operation is the most generic and flexible type of source currently available but it can
be quite tricky to configure.  First you should review the other source node types to ensure there
isn't a simpler choice.

The source node requires some kind of function that can be called to poll for more data.  This
function should take no arguments and should return an
``arrow::Future<std::optional<arrow::ExecBatch>>``.
This function might be reading a file, iterating through an in memory structure, or receiving data
from a network connection.  The arrow library refers to these functions as ``arrow::AsyncGenerator``
and there are a number of utilities for working with these functions.  For this example we use 
a vector of record batches that we've already stored in memory.
In addition, the schema of the data must be known up front.  Acero must know the schema of the data
at each stage of the execution graph before any processing has begun.  This means we must supply the
schema for a source node separately from the data itself.

Here we define a struct to hold the data generator definition. This includes in-memory batches, schema
and a function that serves as a data generator :

.. literalinclude:: ../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: BatchesWithSchema Definition)
  :end-before: (Doc section: BatchesWithSchema Definition)
  :linenos:
  :lineno-match:

Generating sample batches for computation:

.. literalinclude:: ../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: MakeBasicBatches Definition)
  :end-before: (Doc section: MakeBasicBatches Definition)
  :linenos:
  :lineno-match:

Example of using ``source`` (usage of sink is explained in detail in :ref:`sink<stream_execution_sink_docs>`):

.. literalinclude:: ../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Source Example)
  :end-before: (Doc section: Source Example)
  :linenos:
  :lineno-match:

``table_source``
----------------

.. _stream_execution_table_source_docs:

In the previous example, :ref:`source node <stream_execution_source_docs>`, a source node
was used to input the data.  But when developing an application, if the data is already in memory
as a table, it is much easier, and more performant to use :class:`TableSourceNodeOptions`.
Here the input data can be passed as a ``std::shared_ptr<arrow::Table>`` along with a ``max_batch_size``. 
The ``max_batch_size`` is to break up large record batches so that they can be processed in parallel.
It is important to note that the table batches will not get merged to form larger batches when the source
table has a smaller batch size. 

Example of using ``table_source``

.. literalinclude:: ../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Table Source Example)
  :end-before: (Doc section: Table Source Example)
  :linenos:
  :lineno-match:

.. _stream_execution_filter_docs:

``filter``
----------

``filter`` operation, as the name suggests, provides an option to define data filtering 
criteria. It selects rows where the given expression evaluates to true. Filters can be written using
:class:`arrow::compute::Expression`, and the expression should have a return type of boolean.
For example, if we wish to keep rows where the value
of column ``b`` is greater than 3,  then we can use the following expression.

Filter example:

.. literalinclude:: ../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Filter Example)
  :end-before: (Doc section: Filter Example)
  :linenos:
  :lineno-match:

.. _stream_execution_project_docs:

``project``
-----------

``project`` operation rearranges, deletes, transforms, and creates columns.
Each output column is computed by evaluating an expression
against the source record batch. These must be scalar expressions
(expressions consisting of scalar literals, field references and scalar
functions, i.e. elementwise functions that return one value for each input
row independent of the value of all other rows).
This is exposed via :class:`ProjectNodeOptions` which requires,
an :class:`arrow::compute::Expression` and name for each of the output columns (if names are not
provided, the string representations of exprs will be used).  

Project example:

.. literalinclude:: ../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Project Example)
  :end-before: (Doc section: Project Example)
  :linenos:
  :lineno-match:

.. _stream_execution_aggregate_docs:

``aggregate``
-------------

The ``aggregate`` node computes various types of aggregates over data.

Arrow supports two types of aggregates: "scalar" aggregates, and
"hash" aggregates. Scalar aggregates reduce an array or scalar input
to a single scalar output (e.g. computing the mean of a column). Hash
aggregates act like ``GROUP BY`` in SQL and first partition data based
on one or more key columns, then reduce the data in each
partition. The ``aggregate`` node supports both types of computation,
and can compute any number of aggregations at once.

:class:`AggregateNodeOptions` is used to define the
aggregation criteria.  It takes a list of aggregation functions and
their options; a list of target fields to aggregate, one per function;
and a list of names for the output fields, one per function.
Optionally, it takes a list of columns that are used to partition the
data, in the case of a hash aggregation.  The aggregation functions
can be selected from :ref:`this list of aggregation functions
<aggregation-option-list>`.

.. note:: This node is a "pipeline breaker" and will fully materialize
          the dataset in memory.  In the future, spillover mechanisms
          will be added which should alleviate this constraint.

The aggregation can provide results as a group or scalar. For instances,
an operation like `hash_count` provides the counts per each unique record
as a grouped result while an operation like `sum` provides a single record. 

Scalar Aggregation example:

.. literalinclude:: ../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Scalar Aggregate Example)
  :end-before: (Doc section: Scalar Aggregate Example)
  :linenos:
  :lineno-match:

Group Aggregation example:

.. literalinclude:: ../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Group Aggregate Example)
  :end-before: (Doc section: Group Aggregate Example)
  :linenos:
  :lineno-match:

.. _stream_execution_sink_docs:

``sink``
--------

``sink`` operation provides output and is the final node of a streaming 
execution definition. :class:`SinkNodeOptions` interface is used to pass 
the required options. Similar to the source operator the sink operator exposes the output
with a function that returns a record batch future each time it is called.  It is expected the
caller will repeatedly call this function until the generator function is exhausted (returns
``std::optional::nullopt``).  If this function is not called often enough then record batches
will accumulate in memory.  An execution plan should only have one
"terminal" node (one sink node).  An :class:`ExecPlan` can terminate early due to cancellation or 
an error, before the output is fully consumed. However, the plan can be safely destroyed independently
of the sink, which will hold the unconsumed batches by `exec_plan->finished()`.

As a part of the Source Example, the Sink operation is also included;

.. literalinclude:: ../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Source Example)
  :end-before: (Doc section: Source Example)
  :linenos:
  :lineno-match:

.. _stream_execution_consuming_sink_docs:

``consuming_sink``
------------------

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

  arrow::acero::ExecNode *consuming_sink;

  ARROW_ASSIGN_OR_RAISE(consuming_sink, MakeExecNode("consuming_sink", plan.get(),
      {source}, cp::ConsumingSinkNodeOptions(consumer)));


Consuming-Sink example:

.. literalinclude:: ../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: ConsumingSink Example)
  :end-before: (Doc section: ConsumingSink Example)
  :linenos:
  :lineno-match:

.. _stream_execution_order_by_sink_docs:

``order_by_sink``
-----------------

``order_by_sink`` operation is an extension to the ``sink`` operation. 
This operation provides the ability to guarantee the ordering of the 
stream by providing the :class:`OrderBySinkNodeOptions`. 
Here the :class:`arrow::compute::SortOptions` are provided to define which columns 
are used for sorting and whether to sort by ascending or descending values.

.. note:: This node is a "pipeline breaker" and will fully materialize the dataset in memory.
          In the future, spillover mechanisms will be added which should alleviate this 
          constraint.


Order-By-Sink example:

.. literalinclude:: ../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: OrderBySink Example)
  :end-before: (Doc section: OrderBySink Example)
  :linenos:
  :lineno-match:


.. _stream_execution_select_k_docs:

``select_k_sink``
-----------------

``select_k_sink`` option enables selecting the top/bottom K elements, 
similar to a SQL ``ORDER BY ... LIMIT K`` clause.  
:class:`SelectKOptions` which is a defined by 
using :struct:`OrderBySinkNode` definition. This option returns a sink node that receives 
inputs and then compute top_k/bottom_k.

.. note:: This node is a "pipeline breaker" and will fully materialize the input in memory.
          In the future, spillover mechanisms will be added which should alleviate this 
          constraint.

SelectK example:

.. literalinclude:: ../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: KSelect Example)
  :end-before: (Doc section: KSelect Example)
  :linenos:
  :lineno-match:

``table_sink``
----------------

.. _stream_execution_table_sink_docs:

The ``table_sink`` node provides the ability to receive the output as an in-memory table. 
This is simpler to use than the other sink nodes provided by the streaming execution engine
but it only makes sense when the output fits comfortably in memory.
The node is created using :class:`TableSinkNodeOptions`.

Example of using ``table_sink``

.. literalinclude:: ../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Table Sink Example)
  :end-before: (Doc section: Table Sink Example)
  :linenos:
  :lineno-match:

.. _stream_execution_scan_docs:

``scan``
---------

``scan`` is an operation used to load and process datasets.  It should be preferred over the
more generic ``source`` node when your input is a dataset.  The behavior is defined using 
:class:`arrow::dataset::ScanNodeOptions`.  More information on datasets and the various
scan options can be found in :doc:`../dataset`.

This node is capable of applying pushdown filters to the file readers which reduce
the amount of data that needs to be read.  This means you may supply the same
filter expression to the scan node that you also supply to the FilterNode because
the filtering is done in two different places.

Scan example:

.. literalinclude:: ../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Scan Example)
  :end-before: (Doc section: Scan Example)
  :linenos:
  :lineno-match:

.. _stream_execution_write_docs:

``write``
---------

The ``write`` node saves query results as a dataset of files in a
format like Parquet, Feather, CSV, etc. using the :doc:`../dataset`
functionality in Arrow. The write options are provided via the
:class:`arrow::dataset::WriteNodeOptions` which in turn contains
:class:`arrow::dataset::FileSystemDatasetWriteOptions`.
:class:`arrow::dataset::FileSystemDatasetWriteOptions` provides
control over the written dataset, including options like the output
directory, file naming scheme, and so on.

Write example:

.. literalinclude:: ../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Write Example)
  :end-before: (Doc section: Write Example)
  :linenos:
  :lineno-match:

.. _stream_execution_union_docs:

``union``
-------------

``union`` merges multiple data streams with the same schema into one, similar to 
a SQL ``UNION ALL`` clause.

The following example demonstrates how this can be achieved using 
two data sources.

Union example:

.. literalinclude:: ../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Union Example)
  :end-before: (Doc section: Union Example)
  :linenos:
  :lineno-match:

.. _stream_execution_hashjoin_docs:

``hash_join``
-------------

``hash_join`` operation provides the relational algebra operation, join using hash-based
algorithm. :class:`HashJoinNodeOptions` contains the options required in 
defining a join. The hash_join supports 
`left/right/full semi/anti/outerjoins
<https://en.wikipedia.org/wiki/Join_(SQL)>`_. 
Also the join-key (i.e. the column(s) to join on), and suffixes (i.e a suffix term like "_x"
which can be appended as a suffix for column names duplicated in both left and right 
relations.) can be set via the the join options. 
`Read more on hash-joins
<https://en.wikipedia.org/wiki/Hash_join>`_. 

Hash-Join example:

.. literalinclude:: ../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: HashJoin Example)
  :end-before: (Doc section: HashJoin Example)
  :linenos:
  :lineno-match:

Summary
=======

There are examples of these nodes which can be found in 
``cpp/examples/arrow/execution_plan_documentation_examples.cc`` in the Arrow source.

Complete Example:

.. literalinclude:: ../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Execution Plan Documentation Example)
  :end-before: (Doc section: Execution Plan Documentation Example)
  :linenos:
  :lineno-match:
