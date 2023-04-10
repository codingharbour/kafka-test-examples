package com.codingharbour.mockproducer;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TransactionProcessorTest {

    private static final String HIGH_PRIO_TOPIC = "transactions_high_prio";
    private static final String REGULAR_PRIO_TOPIC = "transactions_regular_prio";
    MockProducer<String, String> mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
    TransactionProcessor processor = new TransactionProcessor(mockProducer, HIGH_PRIO_TOPIC, REGULAR_PRIO_TOPIC);

    @Test
    public void testPrioritySelection(){
        Double lowAmount = 50.2d;
        Double highAmount = 250000d;
        Transaction regularPrioTransaction = new Transaction("user1", lowAmount);
        processor.process(regularPrioTransaction);
        Transaction highPrioTransaction = new Transaction("user2", highAmount);
        processor.process(highPrioTransaction);

        assertThat(mockProducer.history()).hasSize(2);

        ProducerRecord<String, String> regTransactionRecord = mockProducer.history().get(0);
        assertThat(regTransactionRecord.value()).contains(lowAmount.toString());
        assertThat(regTransactionRecord.topic()).isEqualTo(REGULAR_PRIO_TOPIC);

        ProducerRecord<String, String> highTransactionRecord = mockProducer.history().get(1);
        assertThat(highTransactionRecord.value()).contains(highAmount.toString());
        assertThat(highTransactionRecord.topic()).isEqualTo(HIGH_PRIO_TOPIC);
    }
}