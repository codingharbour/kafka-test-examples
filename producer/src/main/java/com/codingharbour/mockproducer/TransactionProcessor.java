package com.codingharbour.mockproducer;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TransactionProcessor {
    public static final double HIGH_PRIORITY_THRESHOLD = 100.000;
    private final Producer<String, String> kafkaProducer;
    private final String highPrioTopic;
    private final String regularPrioTopic;

    private final Gson gson = new Gson();

    public TransactionProcessor(Producer<String, String> kafkaProducer, String highPrioTopic, String regularPrioTopic) {
        this.kafkaProducer = kafkaProducer;
        this.highPrioTopic = highPrioTopic;
        this.regularPrioTopic = regularPrioTopic;
    }

    public void process(Transaction transaction){
        String selectedTopic = regularPrioTopic;
        if (transaction.getAmount() >= HIGH_PRIORITY_THRESHOLD) {
            selectedTopic = highPrioTopic;
        }
        String transactionJson = gson.toJson(transaction);
        ProducerRecord<String, String> record = new ProducerRecord<>(selectedTopic, transaction.getUserId(), transactionJson);
        kafkaProducer.send(record);
    }
}
