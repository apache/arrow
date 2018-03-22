---
layout: post
title: "A Native Go Library for Apache Arrow"
date: "2018-03-22 00:00:00 -0400"
author: pmc
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

Since launching in early 2016, Apache Arrow has been growing fast. We have made
nine major releases through the efforts of over 120 distinct contributors. The
project’s scope has also expanded. We began by focusing on the development of
the standardized in-memory columnar data format, which now serves as a pillar
of the project. Since then, we have been growing into a more general
cross-language platform for in-memory data analysis through new additions to
the project like the [Plasma shared memory object store][1]. A primary goal of
the project is to enable data system developers to process and move data fast.

So far, we officially have developed native Arrow implementations in C++, Java,
and JavaScript. We have created binding layers for the C++ libraries in C
(using the GLib libraries) and Python. We have also seen efforts to develop
interfaces to the Arrow C++ libraries in Go, Lua, Ruby, and Rust. While binding
layers serve many purposes, there can be benefits to native implementations,
and so we’ve been keen to see future work on native implementations in growing
systems languages like Go and Rust.

This past October, engineers [Stuart Carnie][2], [Nathaniel Cook][3], and
[Chris Goller][4], employees of [InfluxData][5], began developing a native [Go
language implementation of the [Apache Arrow][6] in-memory columnar format for
use in Go-based database systems like InfluxDB. We are excited to announce that
InfluxData has donated this native Go implementation to the Apache Arrow
project, where it will continue to be developed. This work features low-level
integration with the Go runtime and native support for SIMD instruction
sets. We are looking forward to working more closely with the Go community on
solving in-memory analytics and data interoperability problems.

<div align="center">
<img src="{{ site.base-url }}/img/native_go_implementation.png"
     alt="Apache Arrow implementations and bindings"
     width="60%" class="img-responsive">
</div>

One of the mantras in [The Apache Software Foundation][7] is "Community over
Code". By building an open and collaborative development community across many
programming language ecosystems, we will be able to development better and
longer-lived solutions to the systems problems faced by data developers.

We are excited for what the future holds for the Apache Arrow project. Adding
first-class support for a popular systems programming language like Go is an
important step along the way. We welcome others from the Go community to get
involved in the project. We also welcome others who wish to explore building
Arrow support for other programming languages not yet represented. Learn more
at [https://arrow.apache.org][9] and join the mailing list
[dev@arrow.apache.org][8].

[1]: http://arrow.apache.org/blog/2017/08/16/0.6.0-release/
[2]: https://github.com/stuartcarnie
[3]: https://github.com/nathanielc
[4]: https://github.com/goller
[5]: https://influxdata.com
[6]: https://github.com/influxdata/arrow
[7]: https://www.apache.org
[8]: https://lists.apache.org/list.html?dev@arrow.apache.org
[9]: https://arrow.apache.org