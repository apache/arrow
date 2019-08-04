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

## Usage


#### Use as a lib
Add this to your Cargo.toml:

```toml
[dependencies]
datafusion = "0.15.0-SNAPSHOT"
```

#### Use as a bin
##### Build your own bin(requires rust toolchains)
```sh
git clone https://github/apache/arrow
cd arrow/rust/datafusion
cargo run --bin datafusion-cli
```
##### Use Dockerfile
```sh
git clone https://github/apache/arrow
cd arrow
docker build -f rust/datafusion/Dockerfile . --tag datafusion-cli
docker run -it -v $(your_data_location):/data datafusion-cli
```

```
USAGE:
    datafusion-cli [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -c, --batch-size <batch-size>    The batch size of each query, default value is 1048576
    -p, --data-path <data-path>      Path to your data, default to current directory
```


# Status

## General

- [x] SQL Parser
- [x] SQL Query Planner
- [x] Query Optimizer
- [x] Projection push down
- [ ] Predicate push down
- [x] Type coercion
- [ ] Parallel query execution

## SQL Support

- [x] Projection
- [x] Selection
- [x] Aggregate
- [ ] Sorting
- [x] Limit
- [ ] Nested types and dot notation
- [ ] Lists
- [ ] UDFs
- [ ] Subqueries
- [ ] Joins

## Data Sources

- [x] CSV
- [x] Parquet primitive types
- [ ] Parquet nested types

# Example

Here is a brief example for running a SQL query against a CSV file. See the [examples](examples) directory for full examples.

```rust
fn main() {
    // create local execution context
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
    let csv_datasource = CsvDataSource::new(
        "../../testing/data/csv/aggregate_test_100.csv",
        schema.clone(),
        1024,
    );
    ctx.register_datasource("aggregate_test_100", Rc::new(RefCell::new(csv_datasource)));

    // execute the query
    let sql = "SELECT c1, MIN(c12), MAX(c12) FROM aggregate_test_100 WHERE c11 > 0.1 AND c11 < 0.9 GROUP BY c1";
    let relation = ctx.sql(&sql).unwrap();
    let mut results = relation.borrow_mut();

    // iterate over result batches
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
}
```
