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

# DataFusion

DataFusion is an in-memory query engine that uses Apache Arrow as the memory model. It supports executing SQL queries against CSV and Parquet files as well as querying directly against in-memory data.

## Using DataFusion as a library

DataFusion can be used as a library by adding the following to your `Cargo.toml` file.

```toml
[dependencies]
datafusion = "4.0.0-SNAPSHOT"
```

## Using DataFusion as a binary

DataFusion includes a simple command-line interactive SQL utility. See the [CLI reference](docs/cli.md) for more information.

# Status

## General

- [x] SQL Parser
- [x] SQL Query Planner
- [x] Query Optimizer
- [x] Projection push down
- [x] Predicate push down
- [x] Type coercion
- [x] Parallel query execution

## SQL Support

- [x] Projection
- [x] Filter (WHERE)
- [x] Limit
- [x] Aggregate
- [x] UDFs (user-defined functions)
- [x] UDAFs (user-defined aggregate functions)
- [x] Common math functions
- String functions
  - [x] Length
  - [x] Concatenate
- Miscellaneous/Boolean functions
  - [x] nullif
- Common date/time functions
  - [ ] Basic date functions
  - [ ] Basic time functions
  - [x] Basic timestamp functions
- nested functions
  - [x] Array of columns
- [x] Sorting
- [ ] Nested types
- [ ] Lists
- [x] Subqueries
- [ ] Joins

## Data Sources

- [x] CSV
- [x] Parquet primitive types
- [ ] Parquet nested types

# Supported SQL

This library currently supports the following SQL constructs:

* `CREATE EXTERNAL TABLE X STORED AS PARQUET LOCATION '...';` to register a table's locations
* `SELECT ... FROM ...` together with any expression
* `ALIAS` to name an expression
* `CAST` to change types, including e.g. `Timestamp(Nanosecond, None)`
* most mathematical unary and binary expressions such as `+`, `/`, `sqrt`, `tan`, `>=`.
* `WHERE` to filter
* `GROUP BY` together with one of the following aggregations: `MIN`, `MAX`, `COUNT`, `SUM`, `AVG`
* `ORDER BY` together with an expression and optional `ASC` or `DESC` and also optional `NULLS FIRST` or `NULLS LAST`

## Supported Functions

DataFusion strives to implement a subset of the [PostgreSQL SQL dialect](https://www.postgresql.org/docs/current/functions.html) where possible. We explicitly choose a single dialect to maximize interoperability with other tools and allow reuse of the PostgreSQL documents and tutorials as much as possible.

Currently, only a subset of the PosgreSQL dialect is implemented, and we will document any deviations.


## Supported Data Types

DataFusion uses Arrow, and thus the Arrow type system, for query
execution. The SQL types from
[sqlparser-rs](https://github.com/ballista-compute/sqlparser-rs/blob/main/src/ast/data_type.rs#L57)
are mapped to Arrow types according to the following table


| SQL Data Type   | Arrow DataType                   |
| --------------- | -------------------------------- |
| `CHAR`          | `Utf8`                           |
| `VARCHAR`       | `Utf8`                           |
| `UUID`          | *Not yet supported*              |
| `CLOB`          | *Not yet supported*              |
| `BINARY`        | *Not yet supported*              |
| `VARBINARY`     | *Not yet supported*              |
| `DECIMAL`       | `Float64`                        |
| `FLOAT`         | `Float32`                        |
| `SMALLINT`      | `Int16`                          |
| `INT`           | `Int32`                          |
| `BIGINT`        | `Int64`                          |
| `REAL`          | `Float64`                        |
| `DOUBLE`        | `Float64`                        |
| `BOOLEAN`       | `Boolean`                        |
| `DATE`          | `Date64(DateUnit::Day)`          |
| `TIME`          | `Time64(TimeUnit::Millisecond)`  |
| `TIMESTAMP`     | `Date64(DateUnit::Millisecond)`  |
| `INTERVAL`      | *Not yet supported*              |
| `REGCLASS`      | *Not yet supported*              |
| `TEXT`          | *Not yet supported*              |
| `BYTEA`         | *Not yet supported*              |
| `CUSTOM`        | *Not yet supported*              |
| `ARRAY`         | *Not yet supported*              |

# Developer's guide

This section describes how you can get started at developing DataFusion.

### Bootstrap environment

DataFusion is written in Rust and it uses a standard rust toolkit:

* `cargo build`
* `cargo fmt` to format the code
* `cargo test` to test
* etc.

## How to add a new scalar function

Below is a checklist of what you need to do to add a new scalar function to DataFusion:

* Add the actual implementation of the function:
  * [here](src/physical_plan/string_expressions.rs) for string functions
  * [here](src/physical_plan/math_expressions.rs) for math functions
  * [here](src/physical_plan/datetime_expressions.rs) for datetime functions
  * create a new module [here](src/physical_plan) for other functions
* In [src/physical_plan/functions](src/physical_plan/functions.rs), add:
  * a new variant to `BuiltinScalarFunction`
  * a new entry to `FromStr` with the name of the function as called by SQL
  * a new line in `return_type` with the expected return type of the function, given an incoming type
  * a new line in `signature` with the signature of the function (number and types of its arguments)
  * a new line in `create_physical_expr` mapping the built-in to the implementation
  * tests to the function.
* In [tests/sql.rs](tests/sql.rs), add a new test where the function is called through SQL against well known data and returns the expected result.
* In [src/logical_plan/expr](src/logical_plan/expr.rs), add:
  * a new entry of the `unary_scalar_expr!` macro for the new function.
* In [src/logical_plan/mod](src/logical_plan/mod.rs), add:
  * a new entry in the `pub use expr::{}` set.

## How to add a new aggregate function

Below is a checklist of what you need to do to add a new aggregate function to DataFusion:

* Add the actual implementation of an `Accumulator` and `AggregateExpr`:
  * [here](src/physical_plan/string_expressions.rs) for string functions
  * [here](src/physical_plan/math_expressions.rs) for math functions
  * [here](src/physical_plan/datetime_expressions.rs) for datetime functions
  * create a new module [here](src/physical_plan) for other functions
* In [src/physical_plan/aggregates](src/physical_plan/aggregates.rs), add:
  * a new variant to `BuiltinAggregateFunction`
  * a new entry to `FromStr` with the name of the function as called by SQL
  * a new line in `return_type` with the expected return type of the function, given an incoming type
  * a new line in `signature` with the signature of the function (number and types of its arguments)
  * a new line in `create_aggregate_expr` mapping the built-in to the implementation
  * tests to the function.
* In [tests/sql.rs](tests/sql.rs), add a new test where the function is called through SQL against well known data and returns the expected result.

## How to display plans graphically

The query plans represented by `LogicalPlan` nodes can be graphically
rendered using [Graphviz](http://www.graphviz.org/).

To do so, save the output of the `display_graphviz` function to a file.:

```rust
// Create plan somehow...
let mut output = File::create("/tmp/plan.dot")?;
write!(output, "{}", plan.display_graphviz());
```

Then, use the `dot` command line tool to render it into a file that
can be displayed. For example, the following command creates a
`/tmp/plan.pdf` file:

```bash
dot -Tpdf < /tmp/plan.dot > /tmp/plan.pdf
```
