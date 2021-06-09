<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# ExecNodes and logical operators

`ExecNode`s are intended to implement individual logical operators
in a streaming execution graph. Each node receives batches from
upstream nodes (inputs), processes them in some way, then pushes
results to downstream nodes (outputs). `ExecNode`s are owned and
(to an extent) coordinated by an `ExecPlan`.

> Terminology: "operator" and "node" are mostly interchangable, like
> "Interface" and "Abstract Base Class" in c++ space. The latter is
> a formal and specific bit of code which implements the abstract
> concept.

## Types of logical operators

Each of these will have at least one corresponding concrete
`ExecNode`. Where possible, compatible implementations of a
logical operator will *not* be exposed as independent subclasses
of `ExecNode`. Instead we prefer that they be
be encapsulated internally by a single subclass of `ExecNode`
to permit switching between them during a query.

- Scan: materializes in-memory batches from storage (e.g. Parquet
  files, flight stream, ...)
- Filter: evaluates an `Expression` on each input batch and outputs
  a copy with any rows excluded for which the filter did not return
  `true`.
- Project: evaluates `Expression`s on each input batch to produce
  the columns of an output batch.
- Grouped Aggregate: identify groups based on one or more key columns
  in each input batch, then update aggregates corresponding to those
  groups. Node that this is a pipeline breaker; it will wait for its
  inputs to complete before outputting any batches.
- Union: merge two or more streams of batches into a single stream
  of batches.
- Write: write each batch to storage
- ToTable: Collect batches into a `Table` with stable row ordering where
  possible.

#### Not in scope for Arrow 5.0:

- Join: perform an inner, left, outer, semi, or anti join given some
  join predicates.
- Sort: accumulate all input batches into a single table, reorder its
  rows by some sorting condition, then stream the sorted table out as
  batches
- Top-K: retrieve a limited subset of rows from a table as though it
  were in sorted order.

For example: a dataset scan with only a filter and a
projection will correspond to a fairly trivial graph:

```
ScanNode -> FilterNode -> ProjectNode -> ToTableNode
```

A scan node loads batches from disk and pushes to a filter node.
The filter node excludes some rows based on an `Expression` then
pushes filtered batches to a project node. The project node
materializes new columns based on `Expression`s then pushes those
batches to a table collection node. The table collection node
assembles these batches into a `Table` which is handed off as the
result of the `ExecPlan`.

## Parallelism, pipelines

The execution graph is orthogonal to parallelism; any
node may push to any other node from any thread. A scan node causes
each batch to arrive on a thread after which it will pass through
each node in the example graph above, never leaving that thread
(memory/other resource pressure permitting).

The example graph above happens to be simple enough that processing
of any batch by any node is independent of other nodes and other
batches; it is a pipeline. Note that there is no explicit `Pipeline`
class- pipelined execution is an emergent property of some sub
graphs.

Nodes which do not share this property (pipeline breakers) are
responsible for deciding when they have received sufficient input,
when they can start emitting output, etc. For example a `GroupByNode`
will wait for its input to be exhausted before it begins pushing
batches to its own outputs.

Parallelism is "seeded" by `ScanNode` (or other source nodes)- it
owns a reference to the thread pool on which the graph is executing
and fans out pushing to its outputs across that pool. A subsequent
`ProjectNode` will process the batch immediately after it is handed
off by the `ScanNode`- no explicit scheduling required.
Eventually, individual nodes may internally
parallelize processing of individual batches (for example, if a
`FilterNode`'s filter expression is slow). This decision is also left
up to each `ExecNode` implementation.

# ExecNode interface and usage

`ExecNode`s are constructed using one of the available factory
functions, such as `arrow::compute::MakeFilterNode`
or `arrow::dataset::MakeScanNode`. Any inputs to an `ExecNode`
must be provided when the node is constructed, so the first
nodes to be constructed are source nodes with no inputs
such as `ScanNode`.

The batches yielded by an `ExecNode` always conform precisely
to its output schema. NB: no by-name field lookups or type
checks are performed during execution. The output schema
is usually derived from the output schemas of inputs. For
example a `FilterNode`'s output schema is always identical to
that of its input since batches are only modified by exclusion
of some rows.

An `ExecNode` will begin producing batches when
`node->StartProducing()` is invoked and will proceed until stopped
with `node->StopProducing()`. Started nodes may not be destroyed
until stopped. `ExecNode`s are not currently restartable.
An `ExecNode` pushes batches to its outputs by passing each batch
to `output->InputReceived()`. It signals exhaustion by invoking
`output->InputFinished()`.

Error recovery is permitted within a node. For example, if evaluation
of an `Expression` runs out of memory the governing node may
try that evaluation again after some memory has been freed up.
If a node experiences an error from which it cannot recover (for
example an IO error while parsing a CSV file) then it reports this
with `output->ErrorReceived()`. An error which escapes the scope of
a single node should not be considered recoverable (no `FilterNode`
should `try/catch` the IO error above).

