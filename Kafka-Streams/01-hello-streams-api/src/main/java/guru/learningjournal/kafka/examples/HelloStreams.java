package guru.learningjournal.kafka.examples;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

/*
*  1) Read a stream from Kafka Cluser
*  2) Print Key/Value pair on the console
*/

public class HelloStreams {
    private static final Logger logger = LogManager.getLogger(HelloProducer.class);

    public static void main(String[] args) {
        // Step 1 - Setup mandatory Configuration for Kafka Streams API
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        // SERDE is short form for SERIALIZER/DE-SERIALIZER
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Below steps are using Kafka DSL to define computational logic
        // Step 2 - Create a Topology (Java builder design pattern)
        // Step 2a - Open a stream to a source topic
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<Integer, String> kStream = streamsBuilder.stream(AppConfigs.topicName); // This is main source processor of a Topology which gives a KStream object
        // Step 2b - Define computational logic to process the stream
        kStream.foreach((k, v) -> System.out.println("Key= " + k + " Value= " + v)); // Once we get KStream object, we can add other processors (foreach processor here)
        // kStream transformation methods (like foreach/peek) can be used to add other processors to a Topology
        // Instead of foreach, peek method can also be used
        // kStream.peek((k,v)-> System.out.println("Key= " + k + " Value= " + v));
        // Step 2c - Create a Topology
        Topology topology = streamsBuilder.build();

        // Step 3 - Instantiate KafkaStreams with Topology and Properties and start the Stream
        KafkaStreams streams = new KafkaStreams(topology, props);
        logger.info("Starting stream.");
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down stream");

        // Step 4 - Close the Stream
            streams.close();
        }));
    }
}
