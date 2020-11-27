// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::time::Instant;

use arrow::util::pretty;
use datafusion::error::Result;
use datafusion::prelude::*;

/// This example demonstrates executing a simple query against a SQL database and
/// fetching results.
/// The example assumes that one has a table called `nyc_yellow_taxi` in a postgresql
/// database.
#[tokio::main]
async fn main() -> Result<()> {
    // Please note, the example is to demonstrate functionality, and it reads in
    // a lot of data. The intention is to create a small table for CI, which
    // we can also use to run examples

    // create local execution context
    let config = ExecutionConfig::new().with_batch_size(1024 * 1024);
    let mut ctx = ExecutionContext::with_config(config);

    // register sql table with the execution context
    let start = Instant::now();
    ctx.register_sql_source(
        "nyctaxi",
        "postgres://postgres:password@localhost:5432/postgres",
        "select * from nyc_yellow_taxi limit 10000000",
    )?;
    println!("Registered source in {} ms", start.elapsed().as_millis());

    // execute the query
    let df = ctx.sql(
        "SELECT vendorId, RateCodeID, MIN(passenger_count) as min_passengers, \
        MAX(passenger_count) as max_passengers, \
        AVG(passenger_count) as avg_passengers, \
        COUNT(passenger_count) as count_passengers \
        FROM nyctaxi \
        WHERE trip_distance > 10.0 AND passenger_count > 0 \
        GROUP BY vendorId, RateCodeID \
        ORDER BY RateCodeID DESC",
    )?;
    let results = df.collect().await?;

    println!("Total query time was {} ms", start.elapsed().as_millis());

    // print the results
    pretty::print_batches(&results)?;

    // now try with pushing the query to PostgreSQL

    // register sql query with the execution context
    let query = "SELECT a.\"vendorId\", a.\"RateCodeID\", MIN(a.passenger_count) as min_passengers, \
    MAX(a.passenger_count) as max_passengers, \
    CAST(AVG(a.passenger_count) AS DOUBLE PRECISION) as avg_passengers, \
    COUNT(a.passenger_count) as count_passengers \
    FROM ( SELECT * FROM nyc_yellow_taxi limit 10000000) a \
    WHERE a.trip_distance > 10.0 AND a.passenger_count > 0 \
    GROUP BY a.\"vendorId\", a.\"RateCodeID\" \
    ORDER BY a.\"RateCodeID\" DESC";
    let start = Instant::now();
    ctx.register_sql_source(
        "nyctaxi2",
        "postgres://postgres:password@localhost:5432/postgres",
        query,
    )?;
    println!("Registered source in {} ms", start.elapsed().as_millis());

    // execute the query
    let df = ctx.sql("select * from nyctaxi2")?;
    let results = df.collect().await?;

    println!("Total query time was {} ms", start.elapsed().as_millis());

    // print the results
    pretty::print_batches(&results)?;

    Ok(())
}
