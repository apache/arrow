use kafka::{ClientConfig, KafkaBatchReader};
use uuid::Uuid;

fn main() {
    env_logger::init();
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
    client_config.set_log_level(rdkafka::config::RDKafkaLogLevel::Debug);
    let reader = KafkaBatchReader::new(client_config, &[&topic], 2);

    println!("Reading batches...");
    for batch in reader {
        println!("{:?}", batch);
    }
    println!("Done.");
}
