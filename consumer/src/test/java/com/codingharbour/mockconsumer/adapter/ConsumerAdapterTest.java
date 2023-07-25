package com.codingharbour.mockconsumer.adapter;

import com.codingharbour.mockconsumer.EventData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.assertj.core.api.Assertions.assertThat;

class ConsumerAdapterTest {

    private static final String TEST_TOPIC = "topic1";
    private static final String SENT_MSG = "msg to send";
    @Test
    public void validateHappyScenario() {
        //Given
        DatabaseAdapter mockDbAdapter = mock(DatabaseAdapter.class);
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        ConsumerAdapter consumerAdapter = new ConsumerAdapter(mockConsumer, mockDbAdapter);

        //schedule poll task tells mock consumer what to do in that poll() call
        mockConsumer.schedulePollTask(() -> {
            mockConsumer.rebalance(Collections.singletonList(new TopicPartition(TEST_TOPIC, 0)));
            ConsumerRecord<String, String> record = new ConsumerRecord<>(TEST_TOPIC, 0, 0, null, SENT_MSG);
            mockConsumer.addRecord(record);
        });

        //when you want to stop consumer from polling further (to finish the test) call wakeup()
        mockConsumer.schedulePollTask(() -> {
            mockConsumer.wakeup(); //calling wakeup will stop consumer from polling
        });

        HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        TopicPartition tp = new TopicPartition(TEST_TOPIC, 0);
        startOffsets.put(tp, 0L);
        mockConsumer.updateBeginningOffsets(startOffsets);

        //when
        consumerAdapter.consumeData(TEST_TOPIC);

        //then
        ArgumentCaptor<List<EventData>> argumentCaptor = ArgumentCaptor.forClass(List.class);
        //capture the parameter sent to DbAdapter.save method
        verify(mockDbAdapter).save(argumentCaptor.capture());
        // validate the parameter
        List<EventData> capturedArgument = argumentCaptor.getValue();
        assertThat(capturedArgument)
                .isNotNull()
                .hasSize(1);
        assertThat(capturedArgument.get(0).getInfo()).isEqualTo(SENT_MSG);
    }

    public void validateExceptionHandling() {
        //throw a KafkaException with some reasonable error, eg RecordDeserializationException
    }
}