---
layout: post
title: "Gandiva: A LLVM-based Analytical Expression Compiler for Apache Arrow"
date: "2018-12-05 00:00:00 -0500"
author: jacques
categories: [application]
---
<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

Today we're happy to announce that the Gandiva Initiative for Apache Arrow, an
LLVM-based execution kernel, is now part of the Apache Arrow project. Gandiva
was kindly donated by [Dremio](https://www.dremio.com/), where it was
originally developed and open-sourced. Gandiva extends Arrow's capabilities to
provide high performance analytical execution and is composed of two main
components:

* A runtime expression compiler leveraging LLVM

* A high performance execution environment

Gandiva works as follows: applications submit an expression tree to the
compiler, built in a language agnostic protobuf-based expression
representation. From there, Gandiva then compiles the expression tree to native
code for the current runtime environment and hardware. Once compiled, the
Gandiva execution kernel then consumes and produces Arrow columnar batches. The
generated code is highly optimized for parallel processing on modern CPUs. For
example, on AVX-128 processors Gandiva can process 8 pairs of 2 byte values in
a single vectorized operation, and on AVX-512 processors Gandiva can process 4x
as many values in a single operation. Gandiva is built from the ground up to
understand Arrow's in-memory representation and optimize processing against it.

While Gandiva is just starting within the Arrow community, it already supports
hundreds of [expressions][1], ranging from math functions to case
statements. Gandiva was built as a standalone C++ library built on top of the
core Apache Arrow codebase and was donated with C++ and Java APIs construction
and execution APIs for projection and filtering operations. The Arrow community
is already looking to expand Gandiva's capabilities. This will include
incorporating more operations and supporting many new language bindings. As an
example, multiple community members are already actively building new language
bindings that allow use of Gandiva within Python and Ruby.

While young within the Arrow community, Gandiva is already shipped and used in
production by many Dremio customers as part of Dremio's execution
engine. Experiments have demonstrated [70x performance improvement][2] on many
SQL queries. We expect to see similar performance gains for many other projects
that leverage Arrow.

The Arrow community is working to ship the first formal Apache Arrow release
that includes Gandiva, and we hope this will be available within the next
couple months. This should make it much easier for the broader analytics and
data science development communities to leverage runtime code generation for
high-performance data processing in a variety of contexts and projects.

We started the Arrow project a couple of years ago with the objective of
creating an industry-standard columnar in-memory data representation for
analytics. Within this short period of time, Apache Arrow has been adopted by
dozens of both open source and commercial software products. Some key examples
include technologies such as Apache Spark, Pandas, Nvidia RAPIDS, Dremio, and
InfluxDB. This success has driven Arrow to now be downloaded more than 1
million times per month. Over 200 developers have already contributed to Apache
Arrow. If you're interested in contributing to Gandiva or any other part of the
Apache Arrow project, feel free to reach out on the mailing list and join us!

For additional technical details on Gandiva, you can check out some of the
following resources:

* [https://www.dremio.com/announcing-gandiva-initiative-for-apache-arrow/](https://www.dremio.com/announcing-gandiva-initiative-for-apache-arrow/)

* [https://www.dremio.com/gandiva-performance-improvements-production-query/](https://www.dremio.com/gandiva-performance-improvements-production-query/)

* [https://www.dremio.com/webinars/vectorized-query-processing-apache-arrow/](https://www.dremio.com/webinars/vectorized-query-processing-apache-arrow/)

* [https://www.dremio.com/adding-a-user-define-function-to-gandiva/](https://www.dremio.com/adding-a-user-define-function-to-gandiva/)

[1]: https://github.com/apache/arrow/blob/master/cpp/src/gandiva/function_registry.cc
[2]: https://www.dremio.com/gandiva-performance-improvements-production-query/