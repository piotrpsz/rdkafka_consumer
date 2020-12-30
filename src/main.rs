use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::message::{Headers};
use rdkafka::{ClientContext, TopicPartitionList, Message};
use rdkafka::consumer::{ConsumerContext, Rebalance, StreamConsumer, Consumer, CommitMode};
use rdkafka::error::KafkaResult;
use tokio::stream::StreamExt;

struct CustomContext;
impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        eprintln!("pre-rebalance {:?}", rebalance);
    }
    fn post_rebalance(&self, rebalance: &Rebalance) {
        eprintln!("post-rebalance {:?}", rebalance);
    }
    fn commit_callback(&self, result: KafkaResult<()>, _offset: &TopicPartitionList) {
        eprintln!("commiting offset: {:?}", result);
    }
}

type LoggingConsumer = StreamConsumer<CustomContext>;

async fn fetch_messages(brokers: &str, group_id: &str, topics: &[&str]) {
    let context = CustomContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.auto.commit", "true")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("customer creation failed");

    consumer
        .subscribe(&topics.to_vec())
        .expect("can't subscribe to specific topic");

    let mut message_stream = consumer.start();

    while let Some(message) = message_stream.next().await {
        match message {
            Err(e) => {
                eprintln!("kafka error: {}", e);
            },
            Ok(msg) => {
                let payload: &str = match msg.payload_view() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        eprintln!("{}", e);
                        ""
                    },
                };

                eprintln!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                          msg.key(),
                          payload,
                          msg.topic(),
                          msg.partition(),
                          msg.offset(),
                          msg.timestamp());

                if let Some(headers) = msg.headers() {
                    for i in 0..headers.count() {
                        let hdr = headers.get(i).unwrap();
                        eprintln!("\tHeader: {:#?}: {:?}", hdr.0, hdr.1);
                    }
                }

                consumer.commit_message(&msg, CommitMode::Async).unwrap();
            }
        }

    }
}

#[tokio::main]
async fn main() {
    let brokers = "localhost:9092";
    let group_id = "example_consumer_group_id";
    let topics = vec!["testTopic"];

    fetch_messages(brokers, group_id, &topics).await
}
