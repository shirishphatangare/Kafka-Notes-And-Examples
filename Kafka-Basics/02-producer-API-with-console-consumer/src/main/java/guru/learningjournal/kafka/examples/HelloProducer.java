package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

/*
 * This program demonstrates use of Kafka Producer API. When all 3 Kafka brokers are running with Zookeeper, and a
 * topic is created, below producer sends messages to the Topic in Kafka Broker.
 * We can see partition directories and segment log files (*.log) in tmp directory. This directory also contains partition-wise
 * *.index and *.timeindex files.
 * This example uses Kafka Producer API and Kafka console Consumer
 */

/*
    1) Set Kafka Producer Configuration
    2) Create a Producer Object using Kafka Producer API
    3) Send all the messages
    4) Close the producer
*/


public class HelloProducer {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {

        logger.info("Creating Kafka Producer...");
        Properties props = new Properties();
        /* Kafka Producer Configuration - Below 4 props are mandatory*/
        /* CLIENT_ID_CONFIG - To track source of message */
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        /* BOOTSTRAP_SERVERS_CONFIG - To establish initial connection to the Kafka Cluster */
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        /* KEY-VALUE SERIALIZER Configuration - Kafka Message must be structured as a KEY-VALUE pair (null KEY is allowed)
        *  Kafka messages are sent over a network and hence should be serialized.
        *  IntegerSerializer and StringSerializer are Kafka API serializers.
        */
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

        logger.info("Start sending messages...");
        for (int i = 1; i <= AppConfigs.numEvents; i++) {
            // Only Topic Name and Message Value is mandatory (KEY can be null)
            // Other 2 optional arguments of a Producer Record are Partition and Timestamp
            producer.send(new ProducerRecord<>(AppConfigs.topicName, i, "Simple Message-" + i));
        }

        logger.info("Finished - Closing Kafka Producer.");
        producer.close();

    }
}
