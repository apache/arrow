---
layout: post
title: "DataFusion: A Rust-native Query Engine for Apache Arrow"
date: "2019-02-04 00:00:00 -0600"
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

Although DataFusion was started two years ago, it was recently re-implemented to be Arrow-native and currently has limited capabilities but does support SQL queries against iterators of RecordBatch and has support for CSV files. There are plans to [add support for Parquet files](https://issues.apache.org/jira/browse/ARROW-4466).

SQL support is limited to projection (`SELECT`), selection (`WHERE`), and simple aggregates (`MIN`, `MAX`, `SUM`) with an optional `GROUP BY` clause.

Supported expressions are identifiers, literals, simple math operations (`+`, `-`, `*`, `/`), binary expressions (`AND`, `OR`), equality and comparison operators (`=`, `!=`, `<`, `<=`, `>=`, `>`), and `CAST(expr AS type)`.

## Example

The following example demonstrates running a simple aggregate SQL query against a CSV file.

```rust
// create execution context
let mut ctx = ExecutionContext::new();

// define schema for data source (csv file)
let schema = Arc::new(Schema::new(vec![
    Field::new("c1", DataType::Utf8, false),
    Field::new("c2", DataType::UInt32, false),
    Field::new("c3", DataType::Int8, false),
    Field::new("c4", DataType::Int16, false),
    Field::new("c5", DataType::Int32, false),
    Field::new("c6", DataType::Int64, false),
    Field::new("c7", DataType::UInt8, false),
    Field::new("c8", DataType::UInt16, false),
    Field::new("c9", DataType::UInt32, false),
    Field::new("c10", DataType::UInt64, false),
    Field::new("c11", DataType::Float32, false),
    Field::new("c12", DataType::Float64, false),
    Field::new("c13", DataType::Utf8, false),
]));

// register csv file with the execution context
let csv_datasource =
    CsvDataSource::new("test/data/aggregate_test_100.csv", schema.clone(), 1024);
ctx.register_datasource("aggregate_test_100", Rc::new(RefCell::new(csv_datasource)));

let sql = "SELECT c1, MIN(c12), MAX(c12) FROM aggregate_test_100 WHERE c11 > 0.1 AND c11 < 0.9 GROUP BY c1";

// execute the query
let relation = ctx.sql(&sql).unwrap();
let mut results = relation.borrow_mut();

// iterate over the results
while let Some(batch) = results.next().unwrap() {
    println!(
        "RecordBatch has {} rows and {} columns",
        batch.num_rows(),
        batch.num_columns()
    );

    let c1 = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();

    let min = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    let max = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    for i in 0..batch.num_rows() {
        let c1_value: String = String::from_utf8(c1.value(i).to_vec()).unwrap();
        println!("{}, Min: {}, Max: {}", c1_value, min.value(i), max.value(i),);
    }
}
```

## Roadmap

The roadmap for DataFusion will depend on interest from the Rust community, but here are some of the short term items that are planned:

- Extending test coverage of the existing functionality
- Adding support for Parquet data sources
- Implementing more SQL features such as `JOIN`, `ORDER BY` and `LIMIT`
- Implement a DataFrame API as an alternative to SQL
- Adding support for partitioning and parallel query execution using Rust's async and await functionality
- Creating a Docker image to make it easy to use DataFusion as a standalone query tool for interactive and batch queries

## Contributors Welcome!

If you are excited about being able to use Rust for data science and would like to contribute to this work then there are many ways to get involved. The simplest way to get started is to try out DataFusion against your own data sources and file bug reports for any issues that you find. You could also check out the current [list of issues](https://cwiki.apache.org/confluence/display/ARROW/Rust+JIRA+Dashboard) and have a go at fixing one. You can also join the [user mailing list](http://mail-archives.apache.org/mod_mbox/arrow-user/) to ask questions.


