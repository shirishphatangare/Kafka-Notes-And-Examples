import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {
    private static final Logger logger = LogManager.getLogger();
    public static void main(final String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        // Creating a KTable will internally create a local State-Store.
        // Specify a root directory for Rocks DB State-Store.
        props.put(StreamsConfig.STATE_DIR_CONFIG,AppConfigs.stateStoreLocation);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String,String> KS0 = streamsBuilder.stream(AppConfigs.topicName);

        // flatMapValues - Break a single message into multiple messages
        // Break a line into a list of individual words.
        // keys are null here for KS1 like null - Hello, null - World  etc.
        KStream<String,String> KS1 = KS0.flatMapValues(v-> Arrays.asList(v.toLowerCase().split(" ")));

        // Below groupBy and count operations work like a SQL query - SELECT count(value) FROM KS1 GROUP BY value;
        // Computing aggregations is a two-step process - 1) groupBy 2) Apply Aggregation formula (count)
        // groupBy words - groupBy operation is designed to auto-repartitioning data based on Keys without using through() method
        KGroupedStream<String,String> KGS1 = KS1.groupBy((k,v)-> v);
        // count words and store against each word like Hello - 2, World - 1 etc.
        KTable<String,Long> KT1 = KGS1.count();

        // Aggregation operations (groupBy and count) can be applied to a KStream or a KTable but outcome is always a KTable

        // Auto-repartitioning happens only in below cases -
        // 1) Use a key changing API - map(), flatMap() or groupBy() and
        // 2) key changing API followed by an aggregation or join operation

        // Below KTable lookup can also be done by using QueryServer (REST call to QueryServer)
        KT1.toStream().print(Printed.<String,Long>toSysOut().withLabel("KT1"));

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down stream");
            streams.close();
        }));
    }
}
