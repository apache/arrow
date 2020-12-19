use kafka::{ClientConfig, KafkaBatchReader, KafkaReaderConfig};
use uuid::Uuid;

fn main() {
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

    let reader = KafkaBatchReader::new(config, &[&topic]);

    println!("Reading batches...");
    for batch in reader {
        println!("{:?}", batch);
    }
    println!("Done.");
}
