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

#[test]
fn test_create_reader() {
    let client_config = ClientConfig::new();
    let config = KafkaReaderConfig::new(client_config);
    let _reader = KafkaBatchReader::new(config);
}

#[test]
fn test_iterate_reader() {
    let mut client_config = ClientConfig::new();
    client_config.set("group.id", "test");

    let mut config = KafkaReaderConfig::new(client_config);
    config.poll_timeout(Some(std::time::Duration::new(0, 0)));

    let mut reader = KafkaBatchReader::new(config);

    let batch = reader.next();
    assert!(batch.is_none());
}
