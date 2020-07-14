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
datafusion = "1.0.0-SNAPSHOT"
```

## Using DataFusion as a binary

DataFusion includes a simple command-line interactive SQL utility. See the [CLI reference](docs/cli.md) for more information.

# Status

## General

- [x] SQL Parser
- [x] SQL Query Planner
- [x] Query Optimizer
- [x] Projection push down
- [x] Projection push down
- [ ] Predicate push down
- [x] Type coercion
- [x] Parallel query execution

## SQL Support

- [x] Projection
- [x] Selection
- [x] Limit
- [x] Aggregate
- [x] UDFs
- [x] Common math functions
- [ ] Common string functions
- [ ] Common date/time functions
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
* `ORDER BY` together with an expression and optional `DESC`
