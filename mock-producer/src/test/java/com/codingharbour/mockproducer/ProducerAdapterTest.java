package com.codingharbour.mockproducer;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProducerAdapterTest {

    private static final String TOPIC = "topic1";
    MockProducer<String, String> mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
    ProducerAdapter someService = new ProducerAdapter(mockProducer, TOPIC);

    @Test
    public void testWriteDataToKafka(){
        String key="key1";
        String valuePart = "value1";
        String expectedValue = key+valuePart;
        someService.writeDataToKafka(key, valuePart);
        assertThat(mockProducer.history()).hasSize(1);
        assertThat(mockProducer.history().get(0).value()).isEqualTo(expectedValue);
    }
}