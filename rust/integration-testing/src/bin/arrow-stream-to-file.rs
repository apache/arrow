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

use std::env;
use std::fs::File;
use std::io::BufWriter;
use std::io::{self, BufReader};

use arrow::error::Result;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::FileWriter;

fn main() -> Result<()> {
    let filename = env::args().next().unwrap();
    eprintln!("Writing to Arrow file {}", filename);

    let reader = BufReader::new(io::stdin());
    let mut arrow_stream_reader = StreamReader::try_new(reader)?;
    let schema = arrow_stream_reader.schema();

    let file = File::create(filename)?;
    let writer = BufWriter::new(file);
    let mut writer = FileWriter::try_new(writer, &schema)?;

    while let Some(batch) = arrow_stream_reader.next()? {
        writer.write(&batch)?;
    }
    writer.finish()?;

    Ok(())
}
