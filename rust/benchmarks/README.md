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

# Apache Arrow Rust Benchmarks

This crate contains benchmarks based on popular public data sets and open source benchmark suites, making it easy to
run real-world benchmarks to help with performance and scalability testing and for comparing performance with other Arrow
implementations as well as other query engines.

Currently, only DataFusion benchmarks exist, but the plan is to add benchmarks for the arrow, flight, and parquet
crates as well.

## Benchmark derived from TPC-H

These benchmarks are derived from the [TPC-H](http://www.tpc.org/tpch/) benchmark.

Data for this benchmark can be generated using the
[tpch-dbgen](https://github.com/databricks/tpch-dbgen) tool with a
command such as the following (`-s 1` means use Scale Factor 1 or ~1 GB of
data, replace the 1 to change datasize).

```
cd /mnt/tpch-dbgen
dbgen -vf -s 1
```

The benchmark can then be run (assuming the data created from `dbgen` is in `/mnt/tpch-dbgen`) with a command such as:

```bash
cargo run --release --bin tpch -- --iterations 3 --path /mnt/tpch-dbgen --format tbl --query 1 --batch-size 4096
```

The benchmark program also supports csv and parquet file formats


## NYC Taxi Benchmark

These benchmarks are based on the [New York Taxi and Limousine Commission](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) data set.

```bash
cargo run --release --bin nyctaxi -- --iterations 3 --path /mnt/nyctaxi/csv --format csv --batch-size 4096
```

Example output:

```bash
Running benchmarks with the following options: Opt { debug: false, iterations: 3, batch_size: 4096, path: "/mnt/nyctaxi/csv", file_format: "csv" }
Executing 'fare_amt_by_passenger'
Query 'fare_amt_by_passenger' iteration 0 took 7138 ms
Query 'fare_amt_by_passenger' iteration 1 took 7599 ms
Query 'fare_amt_by_passenger' iteration 2 took 7969 ms
```
