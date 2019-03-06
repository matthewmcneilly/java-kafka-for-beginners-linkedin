package com.github.mmcneilly.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        // System.out.println("Hello World!!");

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
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("first_topic", "Hello World!");

        // send data
        producer.send(record);

        // flush data required for message to appear in consumer
        producer.flush();
        // flush and close
        producer.close();

    }
}
