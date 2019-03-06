package com.github.mmcneilly.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // System.out.println("Hello World!!");

        // Create a logger for my class
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        // Create properties object
        Properties properties = new Properties();
        // estable ips of bootstrap servers
        // properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // seralize/convert strings into bytes for the key value pairs
        // properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create the producer record
        // for topic and value to appear just start typing a string
        for (int i=0; i<10; i++){
            String topic = "first_topic";
            String value = "Hello World!" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            final ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: " + key); // log the key
            // id_0 is going to parititon 1
            // id_1 part 0
            // id_2 part 2
            // keys define what partition to send the messages to


            // send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes everytime there is a record sent successfully or an error is thrown
                    if (e == null) {
                        // the record was successfully sent
                        logger.info(("Recieved new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp()));
                    } else {
                        //exception.printStackTrace();
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); // blocked the .send() makes it synchronus, DONT DO IN PRODUCTION
        }

        // flush data required for message to appear in consumer
        producer.flush();
        // flush and close
        producer.close();

    }
}
