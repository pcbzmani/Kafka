package com.github.pcbzmani;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoCallback {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoCallback.class);
        String bootstrapservers = "127.0.0.1:9092";
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create producer record
        for (int i = 0; i < 5; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello Worid" + Integer.toString(i));


            //send messages
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes for sucessfull
                    if (e == null) {
                        logger.info("Received new Metadata \n" +
                                "Topic : " + recordMetadata.topic() + "\n" +
                                "Partition :" + recordMetadata.partition() + "\n" +
                                "OffSet :" + recordMetadata.offset() + "\n" +
                                "TimeStamp :" + recordMetadata.timestamp());
                        //for error
                    } else {
                        logger.error("Error while Producing", e);
                    }
                }

            });

            //flush
            producer.flush();
            //flush and close
            producer.close();

        }
    }
}
