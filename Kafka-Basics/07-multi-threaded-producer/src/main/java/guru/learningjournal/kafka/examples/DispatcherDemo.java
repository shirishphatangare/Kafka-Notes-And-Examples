package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/*
* Create a multi-threaded Kafka Producer that sends data from a list of files to a Kafka topic such that independent
* thread streams each file.
*
*/


public class DispatcherDemo {
    private static final Logger logger = LogManager.getLogger();
    public static void main(String[] args){

        Properties props = new Properties();
        try{
            // Kafka configuration properties can be kept in a separate file outside the source code - kafka.properties
            InputStream inputStream = new FileInputStream(AppConfigs.kafkaConfigFileLocation);
            props.load(inputStream);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        }catch (IOException e){
            throw new RuntimeException(e);
        }

        KafkaProducer<Integer,String> producer = new KafkaProducer<>(props);
        Thread[] dispatchers = new Thread[AppConfigs.eventFiles.length];
        logger.info("Starting Dispatcher threads...");

        for(int i=0;i<AppConfigs.eventFiles.length;i++){
            // As a best practice, sharing same producer instance across multiple threads.
            dispatchers[i] = new Thread(new Dispatcher(producer, AppConfigs.topicName, AppConfigs.eventFiles[i]));
            dispatchers[i].start();
        }

        try {
            // Main Thread will wait for all the threads to complete...
            for (Thread t : dispatchers) t.join();
        }catch (InterruptedException e){
            logger.error("Main Thread Interrupted");
        }finally {
            producer.close();
            logger.info("Finished Dispatcher Demo");
        }
    }
}
