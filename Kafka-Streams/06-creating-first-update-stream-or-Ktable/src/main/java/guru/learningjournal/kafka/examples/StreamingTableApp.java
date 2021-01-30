package guru.learningjournal.kafka.examples;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class StreamingTableApp {
    private static final Logger logger = LogManager.getLogger();

    public static void main(final String[] args) {

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(StreamsConfig.STATE_DIR_CONFIG, AppConfigs.stateStoreLocation); // Location for local State-Store
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // To disable KTable Caching, set any of below properties to 0
        // Set Commit Interval to 1 Second
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        // Enable Record Cache of Size 10MB
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);



        StreamsBuilder streamsBuilder = new StreamsBuilder();
        // Read the topic as KTable
        KTable<String, String> KT0 = streamsBuilder.table(AppConfigs.topicName); // Here Consumed.with argument is not given since we already
        // provided DEFAULT_KEY_SERDE_CLASS_CONFIG and DEFAULT_VALUE_SERDE_CLASS_CONFIG
        KT0.toStream().print(Printed.<String, String>toSysOut().withLabel("KT0"));

        // Apply a filter on KT0 which results in KT1
        KTable<String, String> KT1 = KT0.filter((k, v) -> k.matches(AppConfigs.regExSymbol) && !v.isEmpty(),
            Materialized.as(AppConfigs.stateStoreName));
        // Since KTable do not provide print(), peek() or foreach() methods, we need to convert it to Kstream for printing
        KT1.toStream().print(Printed.<String, String>toSysOut().withLabel("KT1"));

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);

        //Query Server - REST API to query the local state store
        QueryServer queryServer = new QueryServer(streams, AppConfigs.queryServerHost, AppConfigs.queryServerPort);
        streams.setStateListener((newState, oldState) -> {
            logger.info("State Changing to " + newState + " from " + oldState);
            queryServer.setActive(newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING);
        });

        streams.start();
        queryServer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down servers");
            queryServer.stop();
            streams.close();
        }));

    }
}
