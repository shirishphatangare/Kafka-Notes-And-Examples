package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.JsonDeserializer;
import guru.learningjournal.kafka.examples.serde.JsonSerializer;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/*
* Implement a real-time data validation service for invoices using Kafka Consumer API.
* Read raw invoices from Invoice Topic in Kafka Cluster. If invoice is valid, send it to Valid Invoice Topic,
* otherwise send it to Invalid Invoice Topic.
* Business rules for Valid and Invalid invoices --
* Invalid Invoice - DeliveryType = "HOME-DELIVERY" AND DeliveryAddress->getContactNumber = NULL OR EMPTY
* Valid Invoice - All other invoices which are not invalid with above business rule.
*
* Basically this program implements Kafka Consume - Transform - Produce Pipeline
* Use 07-pos-simulator-json-serialized-invoices application as a main Producer of invoices and consume with this Consumer
*/

public class PosValidator {
    private static final Logger logger = LogManager.getLogger();

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(String[] args) {
        // Step 1 - Consumer Properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        // Target Deserialized Java class name - PosInvoice.class
        consumerProps.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, PosInvoice.class);
        // Kafka Consumer Group ID
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, AppConfigs.groupID);
        // Kafka offsets and Consumer Positions
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Step 2 - Create Kafka Consumer
        KafkaConsumer<String, PosInvoice> consumer = new KafkaConsumer<>(consumerProps);

        // Step 3 - Subscribe Consumer to List of Topics to read from Kafka Cluster
        consumer.subscribe(Arrays.asList(AppConfigs.sourceTopicNames));

        // Producer Properties
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        KafkaProducer<String, PosInvoice> producer = new KafkaProducer<>(producerProps);

        // Step 4 - Read messages in an infinite loop
        while (true) {
            // Poll records from Kafka Cluster. If no records are available, wait till timeout of 100 millisecs
            ConsumerRecords<String, PosInvoice> records = consumer.poll(Duration.ofMillis(100));
            // Traverse through records and validate them
            for (ConsumerRecord<String, PosInvoice> record : records) {
                if (record.value().getDeliveryType().equals("HOME-DELIVERY") &&
                    record.value().getDeliveryAddress().getContactNumber().equals("")) {
                    //Invalid - Send to Invalid Topic using a Producer API
                    producer.send(new ProducerRecord<>(AppConfigs.invalidTopicName, record.value().getStoreID(),
                        record.value()));
                    logger.info("invalid record - " + record.value().getInvoiceNumber());
                } else {
                    //Valid - Send to Valid Topic using a Producer API
                    producer.send(new ProducerRecord<>(AppConfigs.validTopicName, record.value().getStoreID(),
                        record.value()));
                    logger.info("valid record - " + record.value().getInvoiceNumber());
                }
            }
        }
    }
}
