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

These benchmarks are derived from the [TPC-H][1] benchmark.

Data for this benchmark can be generated using the [tpch-dbgen][2] command-line tool. Run the following commands to
clone the repository and build the source code.

```bash
git clone git@github.com:databricks/tpch-dbgen.git
cd tpch-dbgen
make
```

Data can now be generated with the following command. Note that `-s 1` means use Scale Factor 1 or ~1 GB of
data. This value can be increased to generate larger data sets.

```bash
./dbgen -vf -s 1
```

The benchmark can then be run (assuming the data created from `dbgen` is in `/mnt/tpch-dbgen`) with a command such as:

```bash
cargo run --release --bin tpch -- benchmark --iterations 3 --path /mnt/tpch-dbgen --format tbl --query 1 --batch-size 4096
```

The benchmark program also supports CSV and Parquet input file formats and a utility is provided to convert from `tbl`
to CSV and Parquet.

```bash
cargo run --release --bin tpch -- convert --input /mnt/tpch-dbgen --output /mnt/tpch-parquet --format parquet
```

## NYC Taxi Benchmark

These benchmarks are based on the [New York Taxi and Limousine Commission][3] data set.

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

[1]: http://www.tpc.org/tpch/
[2]: https://github.com/databricks/tpch-dbgen
[3]: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page