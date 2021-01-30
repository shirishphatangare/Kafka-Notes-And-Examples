package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.Notification;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

/*
* Rewrite example 05-rewards-in-memory-state-store with KStream aggregation using reduce
* In earlier example, we used custom state-store with a custom partitioner (RewardsPartitioner) and a custom Transformer (RewardsTransformer) transformValues()
* In this example, we will use KStream aggregation using reduce() to achieve same functionality without need of a custom partitioner (auto-repartitioning of aggregation takes care )
* RewardsTransformer (transformValues()) is replaced by  map(), groupby() and reduce()
*/

/* Use Kafka-Basics/11-pos-simulator-json-serialized-invoices as a main Producer for this example */


public class RewardsApp {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,0);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Notification> KS0 = builder.stream(AppConfigs.posTopicName,
            Consumed.with(AppSerdes.String(), AppSerdes.PosInvoice()))
            .filter((key, value) -> value.getCustomerType().equalsIgnoreCase(AppConfigs.CUSTOMER_TYPE_PRIME))
            // We are using Key-changing API map() method which ensures auto-repartitioning on new key (customerCardNo)
            .map((key, invoice) -> new KeyValue<>(
                invoice.getCustomerCardNo(),
                Notifications.getNotificationFrom(invoice)
            ));

        // KStream aggregation using reduce
        // Setting Serdes using Grouped.with as we have not set default Serdes
        KGroupedStream<String, Notification> KGS0 = KS0.groupByKey(Grouped.with(AppSerdes.String(), AppSerdes.Notification()));
        // input key-value types and output key-value types remains same for reduce() method.
        // Alternative is aggregate() method
        KTable<String, Notification> KT0 = KGS0.reduce((aggValue, newValue) -> {
            newValue.setTotalLoyaltyPoints(newValue.getEarnedLoyaltyPoints() + aggValue.getTotalLoyaltyPoints());
            return newValue;
        });

        KT0.toStream().to(AppConfigs.notificationTopic, Produced.with(AppSerdes.String(), AppSerdes.Notification()));

        logger.info("Starting Stream");
        KafkaStreams stream = new KafkaStreams(builder.build(), props);
        stream.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams");
            stream.cleanUp();
        }));
    }
}
