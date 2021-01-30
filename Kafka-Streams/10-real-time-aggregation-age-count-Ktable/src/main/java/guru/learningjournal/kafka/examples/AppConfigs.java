
package guru.learningjournal.kafka.examples;

class AppConfigs {

    final static String applicationID = "AgeCountDemo";
    // This setting works with 1,2 or all 3 bootstrapServers- No Issues
    final static String bootstrapServers = "localhost:9092";
    final static String topicName = "person-age";
    final static String stateStoreLocation = "tmp/state-store";
}
