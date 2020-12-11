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

use datafusion::error::Result;
use datafusion::prelude::*;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<()> {
    //TODO add command-line args
    let ratings_csv = "/tmp/movies/ratings_small.csv";

    let start = Instant::now();
    let mut ctx = ExecutionContext::new();
    let df = ctx.read_csv(ratings_csv, CsvReadOptions::new()).unwrap();
    let _results = df
        .filter(col("userId").eq(lit(1)))?
        .collect()
        .await
        .unwrap();
    println!("Duration: {:?}", start.elapsed());
    Ok(())
}
