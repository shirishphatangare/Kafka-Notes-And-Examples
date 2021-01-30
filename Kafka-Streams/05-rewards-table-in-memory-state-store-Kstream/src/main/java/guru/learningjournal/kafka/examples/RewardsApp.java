package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

/*   Use Kafka-Basics/11-pos-simulator-json-serialized-invoices as a main Producer for this example */
public class RewardsApp {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, PosInvoice> KS0 = builder.stream(
            AppConfigs.posTopicName,
            Consumed.with(AppSerdes.String(), AppSerdes.PosInvoice())
        ).filter((key, value) ->
            value.getCustomerType().equalsIgnoreCase(AppConfigs.CUSTOMER_TYPE_PRIME));

        // Store can be 1)KeyValueStore 2)SessionStore and 3)WindowStore
        // It can be further classified into 6 types like 1) inMemoryKeyValueStore 2) persistentKeyValueStore ... and so on
        StoreBuilder kvStoreBuilder = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(AppConfigs.REWARDS_STORE_NAME),
            AppSerdes.String(), AppSerdes.Double()
        );
        // Build StreamsBuilder with StoreBuilder
        builder.addStateStore(kvStoreBuilder);

        // through Processor to transform Kstream partitioned by StoreID to Kstream partitioned by customerID using custom RewardsTransformer
        KS0.through(AppConfigs.REWARDS_TEMP_TOPIC, // Name of a temporary Topic used by through Processor
            Produced.with(AppSerdes.String(),AppSerdes.PosInvoice(),new RewardsPartitioner()))
            .transformValues(() -> new RewardsTransformer(), AppConfigs.REWARDS_STORE_NAME)
            .to(AppConfigs.notificationTopic,
                Produced.with(AppSerdes.String(), AppSerdes.Notification()));

        // Repartitioning with a custom partitioner is an expensive operation and should be avoided as it impacts application performance
        // Other way is to redesign message Key (Use CustomerId as a message Key instead of StoreId so that all invoices from same customer
        // are stored in the same partition. )
        logger.info("Starting Stream");
        KafkaStreams stream = new KafkaStreams(builder.build(), props);
        stream.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams");
            stream.cleanUp();
        }));
    }
}
