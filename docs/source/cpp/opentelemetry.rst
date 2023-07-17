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

.. _cpp_opentelemetry:

=============
OpenTelemetry
=============

Portions of Arrow C++ are instrumented with the `OpenTelemetry
<https://opentelemetry.io/>`_ C++ SDK which makes it possible to generate
detailed tracing information which can be analyzed in other tools.

Creating a Build with OpenTelemetry Tracing Enabled
---------------------------------------------------

OpenTelemetry tracing is not turned on by default so you must first create a
custom build of Arrow C++ with tracing turned on. See :ref:`Building Arrow C++
<building-arrow-cpp>` for general instructions on creating a custom build.

To enable tracing, specify ``ARROW_WITH_OPENTELEMETRY=ON`` when generating your
build. You may also want to specify ``CMAKE_BUILD_TYPE=RelWithDebInfo`` in order
to get representative timings while retaining debug information.

Exporting Tracing Information
-----------------------------

By default, no tracing information is exported until a tracing backend has been
specified. The choice of tracing backend is controlled with the
:envvar:`ARROW_TRACING_BACKEND` environment variable. Possible values are:

   - ``ostream``: emit textual log messages to stdout
   - ``otlp_http``: emit OTLP JSON encoded traces to a HTTP server (by default,
     the endpoint URL is "http://localhost:4318/v1/traces")
   - ``arrow_otlp_stdout``: emit JSON traces to stdout
   - ``arrow_otlp_stderr``: emit JSON traces to stderr

For example, to enable exporting JSON traces to stdout, set::

   export ARROW_TRACING_BACKEND=arrow_otlp_stdout

At this point, running the program you've linked to your custom build of
Arrow C++ should produce JSON traces on stdout.

Visualizing Traces with Jaeger UI
---------------------------------

Analyzing trace information exported to stdout/stderr may involve writing custom
processing scripts. As an alternative -- or even a complement -- to this
process, the "all-in-one" `Jaeger <https://jaegertracing.io>`_ `Docker
<https://www.docker.com/>`_ image is a relatively straightforward way of
visualizing trace data and is suitable for local development and testing.

Note: This assumes you have `Docker <https://www.docker.com/>`_ installed.

First, change your tracing backend to ``otlp_http``::

   export ARROW_TRACING_BACKEND=otlp_http

Then start the Jaeger all-in-one container::

   docker run \
     -e COLLECTOR_OTLP_ENABLED=true \
     -p 16686:16686 \
     -p 4317:4317 \
     -p 4318:4318 \
     jaegertracing/all-in-one:1.35

Now you should be able to run your program and view any traces in a web browser
at http://localhost:16686. Note that unlike with other methods of exporting
traces, no output will be made to stdout/stderr. However, if you tail your
Docker container logs, you should see output when traces are received by the
all-in-one container.

Note that the volume of spans produced by Acero can quickly become overwhelming
for many tracing frameworks. Several spans are produced per input 
file, input batch, internal chunk of data (called Morsel, consisting of 128k 
rows by default) and per output file (possibly also divided by columns).
In practice, this means that for each MB of data processed by Acero, it will
produce 10 - 20 spans. Choose a suitably sized dataset that strikes a balance
between being representative for the real-world workload, but not too large to 
be inspected with (or even ingested by!) a span visualizer such as Jaeger.

Additional background on tracing
--------------------------------
Traces produced by Acero are conceptually similar to information produced by
using profiling tools, but they are not the same.
For example, the spans by Acero do not necessarily follow the structure of the 
code, like in case of the call-stacks and flame-graphs produced by profiling.
The spans aim to highlight:
- code sections that are performing significant work on the CPU
- code sections that perform I/O operations (reading/writing to disk)
- The way blocks of data flow through the execution graph
- The way data is being reorganized (e.g. a file being split into blocks)
Each span instance can have various attributes added to it when it is created.
This allows us to capture the exact size of each block of data and the amount
of time each node in the execution graph has spent on it.

Span hierarchy
----------------------
Traces are organized in a hierarchical fashion, where each span except the root
span has parents and can have any number of children.
If a span has a child span active during its lifetime, this usually means that
this parent span is not actually in control of the CPU. Thus, calculating the 
total CPU time is not as easy as adding up all of the span durations; only the
time that a span does not have any active children (this is often referred to 
as the "self-time") should count.
However, Acero is a multi-threaded engine, so it is likely that there are
in fact multiple spans performing work on a CPU at any given time!

To achieve this multi-threaded behavior, many sections of code are
executed through a task scheduling mechanism. When these tasks are scheduled,
they can start execution immediately or some time in the future.
Often, a certain span is active that represents the lifetime of some resource
(like a scanner, but also a certain batch of data) that functions as the parent
of a set of spans where actual compute happens.
Care must be taken when aggregating the durations of these spans.

Structure of Acero traces
-------------------------
Acero traces are structured to allow following pieces of data as they flow
through the graph. Each node's function (a kernel) is represented as a child
span of the preceding node.
Acero uses "Morsel-driven parallelism" where batches of data called "morsels" 
flow through the graph. 
The morsels are produced by e.g. a DatasetScanner.
First, the DatasetScanner reads files (called Fragments) into Batches. 
Depending on the size of the fragments it will produce several Batches per 
Fragment.
Then, it may slice the Batches so they do conform to the maximum size of a 
morsel.
Each morsel has a toplevel span called ProcessMorsel.
Currently, the DatasetScanner cannot connect its output to the ProcessMorsel 
spans due to the asynchronous structure of the code.
The dataset writer will gather batches of data in its staging area, and will 
issue a write operation once it has enough rows.
This is represented by the DatasetWriter::Push and DatasetWriter::Pop spans.
These also carry the current fill level of the staging area.
This means that some Morsels will not trigger a write.
Only if a morsel causes the staging area to overflow its threshold,
a DatasetWriter::Pop is triggered that will perform a write operation.


Backpressure
------------
When a node in the execution graph is receiving more data than it can process,
it can ask its preceding nodes to slow down. This process is called 
"backpressure". Reasons for this can include for example:
- the buffer capacity for the node is almost full
- the maximum number of concurrently open files is reached
Relevant events such as a node applying/releasing backpressure, or an async task
group/scheduler throttling task submission, are posted as events to the toplevel
span that belongs to the asynchronous task scheduler,
 and can also be posted to the "local" span (that belongs to the block of data 
 that caused the backpressure).



