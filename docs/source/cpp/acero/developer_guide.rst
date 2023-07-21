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

=================
Developer's Guide
=================

This page goes into more detail into the design of Acero.  It discusses how
to create custom exec nodes and describes some of the philosophies behind Acero's
design and implementation.  Finally, it gives an overview of how to extend Acero
with new behaviors and how this new behavior can be upstreamed into the core Arrow
repository.

Understanding ExecNode
======================

ExecNode is an abstract class with several pure virtual methods that control how the node operates:

:func:`ExecNode::StartProducing`
--------------------------------

This method is called once at the start of the plan.  Most nodes ignore this method (any
neccesary initialization should happen in the construtor or Init).  However, source nodes
will typically provide a custom implementation.  Source nodes should schedule whatever tasks
are needed to start reading and providing the data.  Source nodes are usually the primary
creator of tasks in a plan.

.. note::
   The ExecPlan operates on a push-based model.  Sources are often pull-based.  For example,
   your source may be an iterator.  The source node will typically then schedule tasks to pull one
   item from the source and push that item into the source's output node (via ``InputReceived``).

Examples
^^^^^^^^

* In the ``table_source`` node the input table is divided into batches.  A task is created for
  each batch and that task calls ``InputRecieved`` on the node's output.
* In the ``scan`` node a task is created to start listing fragments from the dataset.  Each listing
  task then creates tasks to read batches from the fragment, asynchronously.  When the batch is
  full read in then a continuation schedules a new task with the exec plan.  This task calls
  ``InputReceived`` on the scan node's output.

:func:`ExecNode::InputReceived`
-------------------------------

This method is called many times during the execution of a plan.  It is how nodes pass data
to each other.  An input node will call InputReceived on its output.  Acero's execution model
is push-based.  Each node pushes data into its output by calling InputReceived and passing in
a batch of data.

The InputReceived method is often where the actual work happens for the node.  For example,
a project node will execute its expressions and create a new expanded output batch.  It will then
call InputReceived on its output.  InputReceived will never be called on a source node.  Sink
nodes will never call InputReceived.  All other nodes will experience both.

Some nodes (often called "pipeline breakers") must accumulate input before they can generate
any output.  For example, a sort node must accumulate all input before it can sort the data and
generate output.  In these nodes the InputReceived method will typically place the data into
some kind of accumulation queue.  If the node doesn't have enough data to operate then it will
not call InputReceived.  This will then be the end of the current task.

Examples
^^^^^^^^

* The ``project`` node runs its expressions, using the received batch as input for the expression.
  A new batch is created from the input batch and the result of the expressions.  The new batch is
  given the same order index as the input batch and the node then calls ``InputReceived`` on its
  output.
* The ``order_by`` node inserts the batch into an accumulation queue.  If this was the last batch
  then the node will sort everything in the accumulation queue.  The node will then call
  ``InputReceived`` on the output for each batch in the sorted result.  A new batch index will be
  assigned to each batch.  Note that this final output step might also occur as a result of a call
  to ``InputFinished`` (described below).

:func:`ExecNode::InputFinished`
-------------------------------

This method will be called once per input.  A node will call InputFinished on its output once it
knows how many batches it will be sending to that output.  Normally this happens when the node is
finished working.  For example, a scan node will call InputFinished once it has finsihed reading
its files.  However, it could call it earlier if it knows (maybe from file metadata) how many
batches will be created.

Some nodes will use this signal to trigger some processing.  For example, a sort node need to
wait until it has received all of its input before it can sort the data.  It relies on the InputFinished
call to know this has happened.

Even if a node is not doing any special processing when finished (e.g. a project node or filter node
doesn't need to do any end-of-stream processing) that node will still call InputFinished on its
output.

.. warning::
   The InputFinished call might arrive before the final call to InputReceived.  In fact, it could
   even be sent out before any calls to InputReceived begin.  For example, the table source node
   always knows exactly how many batches it will be producing.  It could choose to call InputFinished
   before it ever calls InputReceived.  If a node needs to do "end-of-stream" processing then it typically
   uses an AtomicCounter which is a helper class to figure out when all of the data has arrived.

Examples
^^^^^^^^

* The ``order_by`` checks to see if it has already received all its batches.  If it has then it performs
  the sorting step described in the ``InputReceived`` example.  Before it starts sending output data it
  checks to see how many output batches it has (it's possible the batch size changed as part of the
  accumulating or sorting) and calls ``InputFinished`` on the node's output.
* The ``fetch`` node, during a call to ``InputReceived`` realizes it has received all the rows it was
  asked for.  It calls ``InputFinished`` on its output immediately (even though its own ``InputFinished``
  method has not yet been called)

:func:`ExecNode::PauseProducing` / :func:`ExecNode::ResumeProducing`
---------------------------------------------------------------------

These methods control backpressure.  Some nodes may need to pause their input to avoid accumulating
too much data.  For example, when the user is consuming the plan with a RecordBatchReader we use a
SinkNode.  The SinkNode places data in a queue that the RecordBatchReader pulls from (this is a
conversion from a push-model to a pull-model).  If the user is reading the RecordBatchReader slowly then
it is possible this queue will start to fill up.  For another example we can consider the write node.
This node writes data to a filesystem.  If the writes are slow then data might accumulate at the
write node.  As a result, the write node would need to apply backpressure.

When a node realizes that it needs to apply some backpressure it will call PauseProducing on its input.
Once the node has enough space to continue it will then call ResumeProducing on its input.  For example,
the SinkNode would pause when its queue gets too full.  As the user continues to read from the
RecordBatchReader we can expect the queue to slowly drain.  Once the queue has drained enough then the
SinkNode can call ResumeProducing.

Source nodes typically need to provide special behavior for PauseProducing and ResumeProducing.  For
example, a scan node that is reading from a file can pause reading the file.  However, some source nodes
may not be able to pause in any meaningful way.  There is not much point in a table source node pausing
because its data is already in memory.

Nodes that are neither source or sink should still forward backpressure signals.  For example, when
PauseProducing is called on a project node it should call PauseProducing on its input.  If a node has
multiple inputs then it should forward the signal to every input.

Examples
^^^^^^^^

* The ``write`` node, in its ``InputReceived`` method, adds a batch to a dataset writer's queue.  If the
  dataset writer is then full it will return an unfinished future that will complete when it has more room.
  The ``write`` node then calls ``PauseProducing`` on its input.  It then adds a continuation to the future
  that will call ``ResumeProducing`` on its input.
* The ``scan`` node uses an :class:`AsyncTaskScheduler` to keep track of all the tasks it schedules.  This
  scheduler is throttled to limit how much concurrent I/O the ``scan`` node is allowed to perform.  When
  ``PauseProducing`` is called then the node will pause the scheduler.  This means that any tasks queued
  behind the throttle will not be submitted.  However, any ongoing I/O will continue (backpressure can't
  take effect immediately).  When ``ResumeProducing`` is called the ``scan`` node will unpause the scheduler.

:func:`ExecNode::StopProducing`
-------------------------------

StopProducing is called when a plan needs to end early.  This can happen because the user cancelled
the plan and it can happen because an error occurred.  Most nodes do not need to do anything here.
There is no expectation or requirement that a node sends any remaining data it has.  Any node that
schedules tasks (e.g. a source node) should stop producing new data.

In addition to plan-wide cancellation, a node may call this method on its input if it has decided
that it has recevied all the data that it needs.  However, because of parallelism, a node may still
receive a few calls to ``InputReceived`` after it has stopped its input.

If any external reosurces are used then cleanup should happen as part of this call.

Examples
^^^^^^^^

* The ``asofjoin`` node has a dedicated processing thread the communicates with the main Acero threads
  using a queue.  When ``StopProducing`` is called the node inserts a poison pill into the queue.  This
  tells the processing thread to stop immediately.  Once the processing thread stops it marks its external
  task (described below) as completed which allows the plan to finish.
* The ``fetch`` node, in ``InputReceived``, may decide that it has all the data it needs.  It can then call
  ``StopProducing`` on its input.
  
Initialization / Construction / Destruction
-------------------------------------------

Simple initialization logic (that cannot error) can be done in the constructor.  If the initialization
logic may return an invalid status then it can either be done in the exec node's factory method or
the ``Init`` method.  The factory method is preferred for simple validation.  The ``Init`` method is
preferred if the intialization might do expensive allocation or other resource consumption.  ``Init`` will
always be called before ``StartProducing`` is called.  Initialization could also be done in
``StartProducing`` but keep in mind that other nodes may have started by that point.

In addition, there is a ``Validate`` method that can be overloaded to provide custom validation.  This
method is normally called before ``Init`` but after all inputs and outputs have been added.

Finalization happens today in the destructor.  There are a few examples today where that might be slow.
For example, in the write node, if there was an error during the plan, then we might close out some open
files here.  Should there be significant finalization that is either asynchronous or could potentially
trigger an error then we could introduce a Finalize method to the ExecNode lifecycle.  It hasn't been
done yet only because it hasn't been needed.

Summary
-------

.. list-table:: ExecNode Lifecycle
   :widths: 20 40 40
   :header-rows: 1

   * - Method Name
     - This is called when...
     - A node calls this when...
   * - StartProducing
     - The plan is starting
     - N/A
   * - InputReceived
     - Data is received from the input
     - To send data to the output
   * - InputFinished
     - The input knows how many batches there are
     - The node can tell its output how many batches there are
   * - StopProducing
     - A plan is aborted or an output has enough data
     - A node has all the data it needs

Extending Acero
===============

Acero instantiates a singleton :class:`ExecFactoryRegistry` which maps between names and exec node
factories (methods which create an ExecNode from options).  To create a new ExecNode you can register
the node with this registry and your node will now be usable by Acero.  If you would like to be able
to use this node with Substrait plans you will also need to configure the Substrait registry so that it
knows how to map Substrait to your custom node.

This means that you can create and add new nodes to Acero without recompiling Acero from source.

Scheduling and Parallelism
==========================

There are many ways in that data engines can utilize multiple compute resources (e.g. multiple cores).
Before we get into the details of Acero's scheduling we will cover a few high level topics.

Parallel Execution of Plans
---------------------------

Users may want to execute multiple plans concurrently and they are welcome to do so.  However, Acero has no
concept of inter-plan scheduling.  Each plan will attempt to maximize its usage of compute resources and
there will likely be contention of CPU and memory and disk resources.  If plans are using the default CPU &
I/O thread pools this will be mitigated somewhat since they will share the same thread pool.

Locally Distributed Plans
-------------------------

A common way to tackle multi-threading is to split the input into partitions and then create a plan for
each partition and then merge the results from these plans in some way.  For example, let's assume you
have 20 files and 10 cores and you want to read and sort all the data.  You could create a plan for every
2 files to read and sort those files.  Then you could create one extra plan that takes the input from these
10 child plans and merges the 10 input streams in a sorted fashion.

This approach is popular because it is how queries are distributed across mulitple servers and so it
is widely supported and well understood.  Acero does not do this today but there is no reason to prevent it.
Adding shuffle & partition nodes to Acero should be a high priority and would enable Acero to be used by
distributed systems.  Once that has been done then it should be possible to do a local shuffle (local
meaning exchanging between multiple exec plan instances on a single system) if desired.

.. figure:: dist_plan.svg
   
   A distributed plan can provide parallelism even if the plans themselves run serially

Pipeline Parallelism
--------------------

Acero attempts to maximize parallelism using pipeline parallelism.  As each batch of data arrives from the
source we immediately create a task and start processing it.  This means we will likely start processing
batch X before the processing of batch X-1 has completed.  This is very flexible and powerful.  However, it also
means that properly implementing an ExecNode is difficult.

For example, an ExecNode's InputReceived method should be reentrant.  In other words, it should be expected
that InputReceived will be called before the previous call to InputReceived has completed.  This means that
nodes with any kind of mutable state will need mutexes or similar mechanisms to protect that state from race
conditions.  It also means that tasks can easily get out of order and nodes should not expect any particular ordering
of their input (more on this later).

.. figure:: pipeline.svg

   An example of pipeline parallelism on a system with 3 CPU threads and 2 I/O threads

Asynchronicity
--------------

Some operations take a long time and may not require the CPU.  Reading data from the filesystem is one example.  If we
only have one thread per core then time will be wasted while we wait for these operations to complete.  There
are two common solutions to this problem.  A synchronous solution is often to create more threads than there are
cores with the expectation that some of them will be blocked and that is ok.  This approach tends to be simpler
but it can lead to excess thread contention and requires fine-tuning.

Another solution is to make the slow operations asynchronous.  When the slow operation starts the caller gives up
the thread and allows other tasks to run in the meantime.  Once the slow operation finishes then a new task is
created to take the result and continue processing.  This helps to minimize thread contention but tends to be
more complex to implement.

Due to a lack of standard C++ async APIs, Acero uses a combination of the two approaches.  Acero has two thread pools.
The first is the CPU thread pool.  This thread pool has one thread per core.  Tasks in this thread pool should never
block (beyond minor delays for synchornization) and should generally be actively using CPU as much as possible.  Threads
on the I/O thread pool are expected to spend most of the time idle.  They should avoid doing any CPU-intensive work.
Their job is basically to wait for data to be available and schedule follow-up tasks on the CPU thread pool.

.. figure:: async.svg

   Arrow achieves asynchronous execution by combining CPU & I/O thread pools

.. note::

   Most nodes in Acero do not need to worry about asynchronicity.  They are fully synchronous and do not spawn tasks.

Task per Pipeline (and sometimes beyond)
----------------------------------------

An engine could choose to create a thread task for every execution of a node.  However, without careful scheduling,
this leads to problems with cache locality.  For example, let's assume we have a basic plan consisting of three
exec nodes, scan, project, and then filter (this is a very common use case).  Now let's assume there are 100 batches.
In a task-per-operator model we would have tasks like "Scan Batch 5", "Project Batch 5", and "Filter Batch 5".  Each
of those tasks is potentially going to access the same data.  For example, maybe the `project` and `filter` nodes need
to read the same column.  A column which is intially created in a decode phase of the `scan` node.  To maximize cache
utiliziation we would need to carefully schedule our tasks to ensure that all three of those tasks are run consecutively
and assigned to the same CPU core.

To avoid this problem we design tasks that run through as many nodes as possible before the task ends.  This sequence
of nodes is often referred to as a "pipeline" and the nodes that end the pipeline (and thus end the task) are often
called "pipeline breakers".  Some nodes might even fall somewhere in between.  For example, in a hash join node, when
we receive a batch on the probe side, and the hash table has been built, we do not need to end the task and instead keep
on running.  This means that tasks might sometimes end at the join node and might sometimes continue past the join node.

.. figure:: pipeline_task.svg

   A logical view of pipelines in a plan and two tasks, showing that pipeline boundaries may vary during a plan


Thread Pools and Schedulers
---------------------------

The CPU and I/O thread pools are a part of the core Arrow-C++ library.  They contain a FIFO queue of tasks and will
execute them as a thread is available.  For Acero we need additional capabilities.  For this we use the
AsyncTaskScheduler.  In the simplest mode of operation the scheduler simply submits tasks to an underlying thread pool.
However, it is also capable of creating sub-schedulers which can apply throttling, prioritization, and task tracking:

 * A throttled scheduler associates a cost with each task.  Tasks are only submitted to the underlying scheduler
   if there is room.  If there is not then the tasks are placed in a queue.  The write node uses a throttle of size
   1 to avoid reentrantly calling the dataset writer (the dataset writer does its own internal scheduling).  A throttled
   scheduler can be manually paused and unpaused.  When paused all tasks are queued and queued tasks will not be submitted
   even if there is room.  This can be useful in source nodes to implement PauseProducing and ResumeProducing.
 * Priority can be applied to throttled schedulers to control the order in which queued tasks are submitted.  If
   there is room a task is submitted immediately (regardless of priority).  However, if the throttle is full then
   the task is queued and subject to prioritization.  The scan node throttles how many read requests it generates
   and prioritizes reading a dataset in order, if possible.
 * A task group can be used to keep track of a collection of tasks and run a finalization task when all of the
   tasks have completed.  This is useful for fork-join style problems.  The write node uses a task group to close
   a file once all outstanding write tasks for the file have completed.

There is research and examples out there for different ways to prioritize tasks in an execution engine.  Acero has not
yet had to address this problem.  Let's go through some common situations:

 * Engines will often prioritize reading from the build side of a join node before reading from the probe side.  This
   would be more easily handled in Acero by applying backpressure.
 * Another common use case is to control memory accumulation.  Engines will prioritize tasks which are closer to the
   sink node in an effort to relieve memory pressure.  However, Acero currently assumes that spilling will be added
   at pipeline breakers and that memory usage in a plan will be more or less static (per core) and well below the
   limits of the hardware.  This might change if Acero needs to be used in an environment where there are many compute
   resources and limited memory (e.g. a GPU)
 * Engines will often use work stealing algorithms to prioritize running tasks on the same core to improve cache
   locality.  However, since Acero uses a task-per-pipeline model there isn't much lost opportunity for cache
   parallelism that a scheduler could reclaim.  Tasks only end when there is no more work that can be done with the data.

While there is not much prioritzation in place in Acero today we do have the tools to apply it should we need to.

.. note::
   In addition to the AsyncTaskScheduler there is another class called the TaskScheduler.  This class predates the
   AsyncTaskScheduler and was designed to offer task tracking for highly efficient synchronous fork-join workloads.
   If this specialized purpose meets your needs then you may consider using it.  It would be interesting to profile
   this against the AsyncTaskScheduler and see how closely the two compare.

Intra-node Parallelism
----------------------

Some nodes can potentially exploit parallelism within a task.  For example, in the scan node we can decode
columns in parallel.  In the hash join node, parallelism is sometimes exploited for complex tasks such as
building the hash table.  This sort of parallelism is less common but not neccesarily discouraged.  Profiling should
be done first though to ensure that this extra parallelism will be helpful in your workload.

All Work Happens in Tasks
-------------------------

All work in Acero happens as part of a task.  When a plan is started the AsyncTaskScheduler is created and given an
initial task.  This initial task calls StartProducing on the nodes.  Tasks may schedule additional tasks.  For example,
source nodes will usually schedule tasks during the call to StartProducing.  Pipeline breakers will often schedule tasks
when they have accumulated all the data they need.  Once all tasks in a plan are finished then the plan is considered
done.

Some nodes use external threads.  These threads must be registered as external tasks using the BeginExternalTask method.
For example, the asof join node uses a dedicated processing thread to achieve serial execution.  This dedicated thread
is registered as an external task.  External tasks should be avoided where possible because they require careful
handling to avoid deadlock in error situations.

Ordered Execution
=================

Some nodes either establish an ordering to their outgoing batches or they need to be able to process batches in order.
Acero handles ordering using the `batch_index` property on an ExecBatch.  If a node has a determinstic output order
then it should apply a batch index on batches that it emits.  For example, the OrderByNode applies a new ordering to
batches (regardless of the incoming ordering).  The scan node is able to attach an implicit ordering to batches which
reflects the order of the rows in the files being scanned.

If a node needs to process data in order then it is a bit more complicated.  Because of the parallel nature of execution
we cannot guarantee that batches will arrive at a node in order.  However, they can generally be expected to be "mostly
ordered".  As a result, we can insert the batches into a sequencing queue.  The sequencing queue is given a callback which
is guaranteed to run on the batches, serially, in order.  For example, the fetch node uses a sequencing queue.  The callback
checks to see if we need to include part or all of the batch, and then slices the batch if needed.

Even if a node does not care about order it should try and maintain the batch index if it can.  The project and filter
nodes do not care about order but they ensure that output batches keep the same index as their input batches.  The filter
node will even emit empty batches if it needs to so that it can maintain the batch order without gaps.

.. figure:: ordered.svg

   An example of ordered execution


Partitioned Execution
=====================

A stream is partitioned (or sometimes called segmented) if rows are grouped together in some way.  Currently there is not
a formal notion of partitioning.  However, one is starting to develop (e.g. segmented aggregation) and we may end up
introducing a more formal notion of partitions to Acero at some point as well.

Spillover
=========

Spillover has not yet been implemented in Acero.

Distributed Execution
=====================

There are certain exec nodes which are useful when an engine is used in a distributed environment.  The terminology
can vary so we will use the Substrait terminology.  An exchange node sends data to different workers.  Often this is
a partitioned exchange so that Acero is expected to partition each batch and distribute partitions across N different
workers.  On the other end we have the capture node.  This node receives data from different workers.

These nodes do not exist in Acero today.  However, they would be in scope and we hope to have such nodes someday.

Profiling & Tracing
===================

Acero's tracing is currently half-implemented and there are major gaps in profiling tools.  However, there has been some
effort at tracing with open telemetry and most of the neccesary pieces are in place.  The main thing currently lacking is
some kind of effective visualization of the tracing results.

In order to use the tracing that is present today you will need to build with Arrow with `ARROW_WITH_OPENTELEMETRY=ON`.
Then you will need to set the environment variable `ARROW_TRACING_BACKEND=otlp_http`.  This will configure open telemetry
to export trace results (as OTLP) to the HTTP endpoint http://localhost:4318/v1/traces.  You will need to configure an
open telemetry collector to collect results on that endpoint and you will need to configure a trace viewer of some kind
such as Jaeger: https://www.jaegertracing.io/docs/1.21/opentelemetry/

Benchmarking
============

The most complete macro benchmarking for Acero is provided by https://github.com/voltrondata-labs/arrowbench
These include a set of TPC-H benchmarks, executed from the R-dplyr integration, which are run on every Arrow commit and
reported to Conbench at https://conbench.ursa.dev/ 

In addition to these TPC-H benchmarks there are a number of micro-benchmarks for various nodes (hash-join, asof-join,
etc.)  Finally, the compute functions themselves should mostly have micro-benchmarks.  For more on micro benchmarks you
can refer to https://arrow.apache.org/docs/developers/benchmarks.html

Any new functionality should include micro benchmarks to avoid regressions.

Bindings
========

Public API
----------

The public API for Acero consists of Declaration and the various DeclarationToXyz methods.  In addition the
options classes for each node are part of the public API.  However, nodes are extensible and so this API is
extensible.

R (dplyr)
---------

Dplyr is an R library for programmatically building queries.  The arrow-r package has dplyr bindings which
adapt the dplyr API to create Acero execution plans.  In addition, there is a dplyr-substrait backend that
is in development which could eventually replace the Acero-aware binding.

Python
------

The pyarrow library binds to Acero in two different ways.  First, there is a direct binding in pyarrow.acero
which directly binds to the public API.  Second, there are a number of compute utilities like
pyarrow.Table.group_by which uses Acero, though this is invisible to the user.

Java
----

The Java implementation exposes some capabilities from Arrow datasets.  These use Acero implicitly.  There
are no direct bindings to Acero or Substrait in the Java implementation today.

Design Philosophies
===================

Engine Independent Compute
--------------------------

If a node requires complex computation then it should encapsulate that work in abstractions that don't depend on
any particular engine design.  For example, the hash join node uses utilities such as a row encoder, a hash table,
and an exec batch builder.  Other places share implementations of sequencing queues and row segmenters.  The node
itself should be kept minimal and simply maps from Acero to the abstraction.

This helps to decouple designs from Acero's design details and allows them to be more resilant to changes in the
engine.  It also helps to promote these abstractions as capabilities on their own.  Either for use in other engines
or for potential new additions to pyarrow as compute utilities.

Make Tasks not Threads
----------------------

If you need to run something in parallel then you should use thread tasks and not dedicated threads.

 * This keeps the thread count down (reduces thread contention and context switches)
 * This prevents deadlock (tasks get cancelled automatically in the event of a failure)
 * This simplifies profiling (Tasks can be easily measured, easier to know where all the work is)
 * This makes it possible to run without threads (sometimes users are doing their own threading and
   sometimes we need to run in thread-restricted environments like emscripten)

Note: we do not always follow this advice currently.  There is a dedicated process thread in the asof join
node.  Dedicated threads are "ok" for experimental use but we'd like to migrate away from them.

Don't Block on CPU Threads
--------------------------

If you need to run a potentially long running activity that is not actively using CPU resources (e.g. reading from
disk, network I/O, waiting on an external library using its own threads) then you should use asynchronous utilities
to ensure that you do not block CPU threads.

Don't Reinvent the Wheel
------------------------

Each node should not be a standalone island of utilities.  Where possible, computation should be pushed
either into compute functions or into common shared utilities.  This is the only way a project as large as
this can hope to be maintained.

Avoid Query Optimization
------------------------

Writing an efficient Acero plan can be challenging.  For example, filter expressions and column selection
should be pushed down into the scan node so that the data isn't read from disk.  Expressions should be
simplified and common sub-expressions factored out.  The build side of a hash join node should be the
smaller of the two inputs.

However, figuring these problems out is a challenge reserved for a query planner or a query optimizer.
Creating a query optimizer is a challenging task beyond the scope of Acero.  With adoption of Substrait
we hope utilities will eventually emerge that solve these problems.  As a result, we generally avoid doing
any kind of query optimization within Acero.  Acero should interpret declarations as literally as possible.
This helps reduce maintenance and avoids surprises.

We also realize that this is not always possible.  For example, the hash join node currently detects if there
is a chain of hash join operators and, if there is, it configure bloom filters between the operators.  This is
technically a task that could be left to a query optimizer.  However, this behavior is rather specific to Acero
and fairly niche and so it is unlikely it will be introduced to an optimizer anytime soon.

Performance Guidelines
======================

Batch Size
----------

Perhaps the most discussed performance criteria is batch size.  Acero was originally
designed based on research to follow a morsel-batch model.  Tasks are created based on
a large batch of rows (a morsel).  The goal is for the morsel to be large enough to justify
the overhead of a task.  Within a task the data is further subdivided into batches.
Each batch should be small enough to fit comfortable into CPU cache (often the L2 cache).

This sets up two loops.  The outer loop is parallel and the inner loop is not:

.. code:: python

  for morsel in dataset: # parallel
    for batch in morsel:
      run_pipeline(batch)

The advantage of this style of execution is that successive nodes (or successive operations
within an exec node) that access the same column are likely to benefit from cache.  It also
is essential for functions that require random access to data.  It maximizes parallelism while
minimizing the data transfer from main memory to CPU cache.

.. figure:: microbatch.svg

   If multiple passes through the data are needed (or random access) and the batch is much bigger
   then the cache then performance suffers.  Breaking the task into smaller batches helps improve
   task locality.

The morsel/batch model is reflected in a few places in Acero:

 * In most source nodes we will try and grab batches of 1Mi rows.  This is often configurable.
 * In the source node we then iterate and slice off batches of 32Ki rows.  This is not currently
   configurable.
 * The hash join node currently requires that a batches contain at 32Ki rows or less as it uses
   16-bit signed integers as row indices in some places.

However, this guidance is debateable.  Profiling has shown that we do not get any real benefit
from moving to a smaller batch size.  It seems any advantage we do get is lost in per-batch
overhead.  Most of this overhead appears to be due to various per-batch allocations.  In addition,
depending on your hardware, it's not clear that CPU Cache<->RAM will always be the bottleneck.  A
combination of linear access, pre-fetch, and high CPU<->RAM bandwidth can alleviate the penalty
of cache misses.

As a result, this section is included in the guide to provide historical context, but should not
be considered binding.

Ongoing & Deprecated Work
=========================

The following efforts are ongoing.  They are described here to explain certain duplication in the
code base as well as explain types that are going away.

Scanner v2
----------

The scanner is currently a node in the datasets module registered with the factory registry as "scan".
This node was written prior to Acero and made extensive use of AsyncGenerator to scan multiple files
in parallel.  Unfortunately, the use of AsyncGenerator made the scan difficult to profile, difficult
to debug, and impossible to cancel.  A new scan node is in progress.  It is currently registered with
the name "scan2".  The new scan node uses the AsyncTaskScheduler instead of AsyncGenerator and should
provide additional features such as the ability to skip rows and handle nested column projection (for
formats that support it)

OrderBySink and SelectKSink
---------------------------

These two exec nodes provided custom sink implementations.  They were written before ordered execution
was added to Acero and were the only way to generate ordered ouptut.  However, they had to be placed
at the end of a plan and the fact that they were custom sink nodes made them difficult to describe with
Declaration.  The OrderByNode and FetchNode replace these.  These are kept at the moment until existing
bindings move away from them.

Upstreaming Changes
===================

Acero is designed so that it can be extended without recompilation.  You can easily add new compute
functions and exec nodes without creating a fork or compiling Acero.  However, as you develop new
features that are generally useful, we hope you will make time to upstream your changes.

Even though we welcome these changes we have to admit that there is a cost to this process.  Upstreaming
code requires that the new module behave correctly, but that is typically the easier part to review.
More importantly, upstreaming code is a process of transferring the maintenance burden from yourself to
the wider Arrow C++ project maintainers.  This requires a deep understanding of the code by maintainers,
it requires the code be consistent with the style of the project, and it requires that the code be well
tested with unit tests to aid in regression.

Because of this, we highly recommend taking the following steps:

* As you are starting out you should send a message to the mailing list announcing your intentions and
  design.  This will help you determine if there is wider interest in the feature and others may have
  ideas or suggestions to contribute early on in the process.

  * If there is not much interest in the feature then keep in mind that it may be difficult to eventually
    upstream the change.  The maintenance capacity of the team is limited and we try and prioritize
    features that are in high demand.

* We recommend developing and testing the change on your own fork until you get it to a point where you
  are fairly confident things are working correctly.  If the change is large then you might also think
  about how you can break up the change into smaller pieces.  As you do this you can share both the larger
  PR (as a draft PR or a branch on your local fork) and the smaller PRs.  This way we can see the context
  of the smaller PRs.  However, if you do break things up, smaller PRs should still ideally stand on their
  own.

* Any PR will need to have the following:

  * Unit tests convering the new functionality

  * Microbenchmarks if there is any significant compute work going on

  * Examples demonstrating how to use the new feature

  * Updates to the API reference and this guide

  * Passing CI (you can enable Github Actions on your fork and that will allow most CI jobs to run before
    you create your PR)
