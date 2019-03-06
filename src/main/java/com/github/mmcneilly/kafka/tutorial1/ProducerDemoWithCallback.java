package com.github.mmcneilly.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        // System.out.println("Hello World!!");

        // Create a logger for my class
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

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
            final ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("first_topic", "Hello World!" + Integer.toString(i));

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
            });
        }

        // flush data required for message to appear in consumer
        producer.flush();
        // flush and close
        producer.close();

    }
}
