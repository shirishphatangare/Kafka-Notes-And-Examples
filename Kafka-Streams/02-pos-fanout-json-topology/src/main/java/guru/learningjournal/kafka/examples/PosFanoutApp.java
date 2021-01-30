package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

// Use Kafka-Basics/11-pos-simulator-json-serialized-invoices as a main Producer for this example

public class PosFanoutApp {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);

        // Creating Threads of Kafka Streams application is very easy. You just need to add below configuration for a Kafka Stream.
        // props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3); // This will start 3 threads of an application and distribute
        // tasks among these threads. This will ensure faster execution of large real-time data processing application.

        // Creating Topology
        StreamsBuilder builder = new StreamsBuilder();

        // KStream is a main source processor. A source processor internally creates a consumer and reads data from given topic.
        KStream<String, PosInvoice> KS0 = builder.stream(AppConfigs.posTopicName,
            Consumed.with(AppSerdes.String(), AppSerdes.PosInvoice())); // Consumed object provides Key-Value Serdes for internal Consumer

        // Next processor in the chain
        KS0.filter((k, v) ->
            v.getDeliveryType().equalsIgnoreCase(AppConfigs.DELIVERY_TYPE_HOME_DELIVERY)) // filter returns an intermediate KStream Object
            // Produced object provides Key-Value Serdes for internal Producer
            .to(AppConfigs.shipmentTopicName, Produced.with(AppSerdes.String(), AppSerdes.PosInvoice())); // Sink/terminal processor

        // Next processor in the chain
        KS0.filter((k, v) ->
            v.getCustomerType().equalsIgnoreCase(AppConfigs.CUSTOMER_TYPE_PRIME))
            .mapValues(invoice -> RecordBuilder.getNotification(invoice))
            .to(AppConfigs.notificationTopic, Produced.with(AppSerdes.String(), AppSerdes.Notification())); // Sink/terminal processor

        // Next processor in the chain
        KS0.mapValues(invoice -> RecordBuilder.getMaskedInvoice(invoice))
            .flatMapValues(invoice -> RecordBuilder.getHadoopRecords(invoice))
            .to(AppConfigs.hadoopTopic, Produced.with(AppSerdes.String(), AppSerdes.HadoopRecord())); // Sink/terminal processor

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Stream");
            streams.close();
        }));

    }
}
