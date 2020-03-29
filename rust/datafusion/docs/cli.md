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

# DataFusion CLI

The DataFusion CLI is a command-line interactive SQL utility that allows queries to be executed against CSV and Parquet files. It is a convenient way to try DataFusion out with your own data sources.

## Run using Cargo

Use the following commands to clone this repository and run the CLI. This will require the Rust toolchain to be installed. Rust can be installed from [https://rustup.rs/](https://rustup.rs/).

```sh
git clone https://github.com/apache/arrow
cd arrow/rust/datafusion
cargo run --bin datafusion-cli --release
```

## Run using Docker

Use the following commands to clone this repository and build a Docker image containing the CLI tool. Note that there is `.dockerignore` file in the root of the repository that may need to be deleted in order for this to work.

```sh
git clone https://github.com/apache/arrow
cd arrow
docker build -f rust/datafusion/Dockerfile . --tag datafusion-cli
docker run -it -v $(your_data_location):/data datafusion-cli
```

## Usage

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

Type `exit` or `quit` to exit the CLI.

## Registering Parquet Data Sources

Parquet data sources can be registered by executing a `CREATE EXTERNAL TABLE` SQL statement. It is not necessary to provide schema information for Parquet files.

```sql
CREATE EXTERNAL TABLE taxi 
STORED AS PARQUET
LOCATION '/mnt/nyctaxi/tripdata.parquet';
```

## Registering CSV Data Sources

CSV data sources can be registered by executing a `CREATE EXTERNAL TABLE` SQL statement. It is necessary to provide schema information for CSV files since DataFusion does not automatically infer the schema when using SQL to query CSV files.

```sql
CREATE EXTERNAL TABLE test (
    c1  VARCHAR NOT NULL,
    c2  INT NOT NULL,
    c3  SMALLINT NOT NULL,
    c4  SMALLINT NOT NULL,
    c5  INT NOT NULL,
    c6  BIGINT NOT NULL,
    c7  SMALLINT NOT NULL,
    c8  INT NOT NULL,
    c9  BIGINT NOT NULL,
    c10 VARCHAR NOT NULL,
    c11 FLOAT NOT NULL,
    c12 DOUBLE NOT NULL,
    c13 VARCHAR NOT NULL
)
STORED AS CSV
WITH HEADER ROW
LOCATION '/path/to/aggregate_test_100.csv';
```
