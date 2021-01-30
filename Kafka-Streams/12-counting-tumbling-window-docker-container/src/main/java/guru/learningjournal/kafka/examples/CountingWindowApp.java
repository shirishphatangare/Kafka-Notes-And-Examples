package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.SimpleInvoice;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Properties;


/*
* Windowed Aggregation - Performing aggregation (count) on data streams sub-grouped by Time windows
* Use custom Timestamp extractor (InvoiceTimeExtractor) to read time from the invoice
* This example doesn't work with Windows OS. We have CentOs docker image in scripts. However, you will need Docker desktop on Windows to run it
* To run we need to send invoices from resources/data/sample-invoices.txt line-by-line to the start-producer.cmd
* Tumbling Window - Fixed-size, Non-overlapping, Gap-less Time-Window
*/

public class CountingWindowApp {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(StreamsConfig.STATE_DIR_CONFIG, AppConfigs.stateStoreName);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, SimpleInvoice> KS0 = streamsBuilder.stream(AppConfigs.posTopicName,
            Consumed.with(AppSerdes.String(), AppSerdes.SimpleInvoice())
                .withTimestampExtractor(new InvoiceTimeExtractor())
        );

        // First group by StoreId and then re-group by Time-window
        // KTable key is Windowed<String> i.e. Time-Windowed StoreId and value is count of records
        KTable<Windowed<String>, Long> KT0 = KS0.groupByKey(Grouped.with(AppSerdes.String(), AppSerdes.SimpleInvoice()))
            .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
            .count();

        KT0.toStream().foreach(
            (wKey, value) -> logger.info(
                "Store ID: " + wKey.key() + " Window ID: " + wKey.window().hashCode() +
                    " Window start: " + Instant.ofEpochMilli(wKey.window().start()).atOffset(ZoneOffset.UTC) +
                    " Window end: " + Instant.ofEpochMilli(wKey.window().end()).atOffset(ZoneOffset.UTC) +
                    " Count: " + value
            )
        );


        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams");
            streams.close();
        }));

    }
}
