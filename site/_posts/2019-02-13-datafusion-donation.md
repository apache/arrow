---
layout: post
title: "DataFusion: A Rust-native Query Engine for Apache Arrow"
date: "2019-02-06 00:00:00 -0600"
author: agrove
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

We are excited to announce that [DataFusion](https://github.com/apache/arrow/tree/master/rust/datafusion) has been donated to the Apache Arrow project. DataFusion is an in-memory query engine for the Rust implementation of Apache Arrow.

Although DataFusion was started two years ago, it was recently re-implemented to be Arrow-native and currently has limited capabilities but does support SQL queries against iterators of RecordBatch and has support for CSV files. Support for Parquet files will be available soon.

Currently, only a subset of SQL is supported. Query support is limited to projection (SELECT), selection (WHERE), and simple aggregates (MIN, MAX, SUM) with support for GROUP BY.

Supported expressions are identifiers, literals, simple math operations, binary expressions (AND, OR), equality and comparison operators, and CAST.

## Why another query engine?

Apache Arrow already has the [Gandiva](https://arrow.apache.org/blog/2018/12/05/gandiva-donation/) C++ query engine that was very recently donated. Gandiva is currently both more mature and more advanced than DataFusion but it is not yet possible to call Gandiva from Rust. It would certainly be possible to write a Rust FFI wrapper for Gandiva and it seems likely that could happen in the future but there are advantages to having a Rust-native query engine too, even if it is less capable than Gandiva.

## Example

The following example demonstrates running a simple aggregate SQL query against a CSV file.

```rust
```

## Roadmap

The roadmap for DataFusion will depend on interest from the Rust community, but some of the short term items that I am motivated to work on are:

- Extending test coverage of the existing functionality
- Adding support for Parquet data sources
- Implementing more SQL features such as `JOIN`, `ORDER BY` and `LIMIT`
- Implement a DataFrame API as an alternative to SQL
- Adding support for partitioning and parallel query execution using Rust's async and await functionality



