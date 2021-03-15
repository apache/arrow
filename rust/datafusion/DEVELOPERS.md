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
