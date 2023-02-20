package com.codingharbour.mockproducer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerAdapter {
    private final Producer<String, String> kafkaProducer;
    private final String topic;

    public ProducerAdapter(Producer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    /**
     * Method send a message of (key, key+valuePart) to a kafka topic
     * @param key a message key
     * @param valuePart a value part to be transformed to message payload
     */
    public void writeDataToKafka(String key, String valuePart){
        String messageValue = key + valuePart;
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, messageValue);
        kafkaProducer.send(record);
    }
}
