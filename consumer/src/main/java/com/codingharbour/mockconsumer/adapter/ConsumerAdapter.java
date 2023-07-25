package com.codingharbour.mockconsumer.adapter;

import com.codingharbour.mockconsumer.EventData;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.stream.StreamSupport;
import com.codingharbour.mockconsumer.adapter.DatabaseAdapter;
import org.apache.kafka.common.errors.WakeupException;

public class ConsumerAdapter {

    private Consumer<String, String> consumer;
    private DatabaseAdapter dbAdapter;

    public ConsumerAdapter(Consumer<String, String> consumer, DatabaseAdapter dbAdapter) {
        this.consumer = consumer;
        this.dbAdapter = dbAdapter;
    }

    public void consumeData(String topic) {
        consumer.subscribe(Collections.singletonList(topic));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                List<EventData> eventDataList = StreamSupport.stream(records.spliterator(), false)
                        .map(record -> new EventData(record.value()))
                        .toList();
                dbAdapter.save(eventDataList);
                consumer.commitSync();
            }
        } catch (RuntimeException e) {
            System.out.println("Shutting down the consumer...");
        } finally {
            consumer.close();
        }
    }

}