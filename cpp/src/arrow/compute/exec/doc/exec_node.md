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

# ExecNodes

`ExecNode`s are intended to implement individual logical operators
in a streaming execution graph. Each node receives batches from
upstream nodes (inputs), processes them in some way, then pushes
results to downstream nodes (outputs). `ExecNode`s are owned and
(to an extent) coordinated by an `ExecPlan`.

For example: for a simple dataset scan with only a filter and a
projection, we'll have a pretty trivial graph with a scan node
which loads batches from disk and pushes to a filter node. The
filter node excludes some rows based on an `Expression` then
pushes filtered batches to a project node. The project node
materializes new columns based on `Expressions` then pushes those
batches to a table collection node. The table collection node
assembles these batches into a `Table` which is handed off as the
result of the `ExecPlan`.

Note that the execution graph is orthogonal to parallelism; any
node may push to any other node from any thread. In most cases,
a batch will arrive on a thread from a scan node and will
pass through each node in the graph on that same thread.

