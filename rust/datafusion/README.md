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
datafusion = "2.0.0-SNAPSHOT"
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
- Common date/time functions
  - [ ] Basic date functions
  - [ ] Basic time functions
  - [x] Basic timestamp functions
- nested functions
  - [x] Array of columns
- [x] Sorting
- [ ] Nested types
- [ ] Lists
- [ ] Subqueries
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

## Core concepts

### Dynamic typing

DataFusion's memory layout is the columnar format Arrow. Because a column type is only known 
at runtime, DataFusion, like Arrow's create, uses dynamic typing throughout most of its code. Thus, a central aspect of DataFusion's query engine is keeping track of an expression's `datatype`.

### Nullability

Arrow's columnar format natively supports the notion of null values, and DataFusion also. Like types,
DataFusion keeps track of an expression's `nullability` throughout planning and execution.

### Field and Schema

Arrow's implementation in rust has a `Field` that contains information about a column:

* name
* datatype
* nullability

A `Schema` is essentially a vector of fields.

### parse, plan, optimize, execute

When a query is sent to DataFusion, there are different steps that it passes through until a result is
obtained. Broadly, they are:

1. The string is parsed to an Abstract syntax tree (AST). We use [sqlparser](https://docs.rs/sqlparser/0.6.1/sqlparser/) for this.
2. The AST is converted to a logical plan ([src/sql](src/sql/planner.rs))
3. The logical plan is optimized to a new logical plan ([src/optimizer](src/optimizer))
4. The logical plan is converted to a physical plan ([src/physical_plan/planner](src/physical_plan/planner.rs))
5. The physical plan is executed ([src/execution/context.rs](src/execution/context.rs))

Phases 1-4 are typically cheap/fast when compared to phase 5, and thus DataFusion puts a lot of effort to ensure that phase 5 runs without errors.

#### Logical plan

A logical plan is a representation of the plan without details of how it is executed. In general, 

* given a data schema and a logical plan, the resulting schema is known.
* given data and a logical plan, we agree on the result, irrespectively of how it is computed.

A logical plan is composed by nodes (called `LogicalPlan`), and each node is composed by logical expressions (called `Expr`). All of these are located in [src/logical_plan/mod.rs](src/logical_plan/mod.rs).

#### Physical plan

A Physical plan is a plan that can be executed. Contrarily to a logical plan, the physical plan has specific
information about how the calculation should be performed (e.g. what actual rust functions are used).

A physical plan is composed by nodes (implement the trait `ExecutionPlan`), and each node is composed by physical expressions (implement the trait `PhysicalExpr`) or aggreagate expressions (implement the trait `AggregateExpr`). All of these are located in [src/physical_plan](src/physical_plan).

Physical expressions are evaluated against `RecordBatch` (a group of `Array`s and a `Schema`).

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
