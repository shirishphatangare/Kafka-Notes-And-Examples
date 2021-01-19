package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class HelloProducer {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {

        logger.info("Creating Kafka Producer...");
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /*  TRANSACTIONAL_ID_CONFIG setting is a mandatory requirement for a Producer implementing transactions.
            Ideally this setting should be kept in kafka.properties file.
            With below property setting for TRANSACTIONAL_ID_CONFIG -
            1) Idempotence is automatically enabled because transaction depends on idempotence.
            2) TRANSACTIONAL_ID_CONFIG must be unique for each Producer instance.
         */
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, AppConfigs.transaction_id);

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        // Step 1 - initTransactions
        producer.initTransactions();

        // COMMIT Scenario in the first Transaction
        logger.info("Starting First Transaction...");
        // All messages sent between beginTransaction and commitTransaction are part of a single transaction.
        // Step 2 - beginTransaction
        producer.beginTransaction();
        try {
            for (int i = 1; i <= AppConfigs.numEvents; i++) {
                // Same message is delivered to both the topics in a single transaction
                producer.send(new ProducerRecord<>(AppConfigs.topicName1, i, "Simple Message-T1-" + i));
                producer.send(new ProducerRecord<>(AppConfigs.topicName2, i, "Simple Message-T1-" + i));
            }
            logger.info("Committing First Transaction.");
            // Step 3 - commitTransaction
            producer.commitTransaction();
        }catch (Exception e){
            logger.error("Exception in First Transaction. Aborting...");
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException(e);
        }

        // ROLLBACK Scenario in the Second Transaction
        logger.info("Starting Second Transaction...");
        producer.beginTransaction();
        try {
            for (int i = 1; i <= AppConfigs.numEvents; i++) {
                producer.send(new ProducerRecord<>(AppConfigs.topicName1, i, "Simple Message-T2-" + i));
                producer.send(new ProducerRecord<>(AppConfigs.topicName2, i, "Simple Message-T2-" + i));
            }
            logger.info("Aborting Second Transaction.");
            producer.abortTransaction(); // To simulate ROLLBACK
        }catch (Exception e){
            logger.error("Exception in Second Transaction. Aborting...");
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException(e);
        }

        logger.info("Finished - Closing Kafka Producer.");
        producer.close();

    }
}
