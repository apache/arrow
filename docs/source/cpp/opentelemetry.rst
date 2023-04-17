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
