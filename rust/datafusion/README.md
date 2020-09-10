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
- [x] UDFs
- [x] Common math functions
- String functions
  - [x] Length
  - [x] Concatenate
- Common date/time functions
  - [ ] Basic date functions
  - [ ] Basic time functions
  - [x] Basic timestamp functions
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
