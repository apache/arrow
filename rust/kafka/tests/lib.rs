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
    config.poll_timeout(Some(std::time::Duration::new(0,0))); 

    let mut reader = KafkaBatchReader::new(config);

    let batch = reader.next();
    assert!(batch.is_none());
}
