package guru.learningjournal.kafka.examples;

class AppConfigs {
    final static String applicationID = "HelloStreams";
    final static String producerApplicationID = "HelloProducer";
    final static String bootstrapServers = "localhost:9092,localhost:9093,localhost:9094";
    final static String topicName = "hello-producer-topic";
    final static int numEvents = 10;
}
