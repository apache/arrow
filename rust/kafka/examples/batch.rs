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

use arrow_kafka::{ClientConfig, KafkaBatchReader, KafkaReaderConfig};
use uuid::Uuid;

fn main() -> arrow::error::Result<()> {
    let mut args = std::env::args();

    let _ = args.next().unwrap();
    let broker = args.next().expect("Please specify broker.");
    let topic = args.next().expect("Please specify topic.");

    let uuid = Uuid::new_v4();
    let group = format!("{}", uuid);

    let mut client_config = ClientConfig::new();
    client_config.set("group.id", &group);
    client_config.set("bootstrap.servers", &broker);
    client_config.set("auto.offset.reset", "earliest");

    let mut config = KafkaReaderConfig::new(client_config);
    config.max_batch_size(2);
    config.poll_timeout(Some(std::time::Duration::new(5, 0)));

    let reader = KafkaBatchReader::new(config);
    reader.subscribe(&[&topic])?;

    println!("Reading batches...");
    for batch in reader {
        println!("{:?}", batch);
    }
    println!("Done.");
    Ok(())
}
