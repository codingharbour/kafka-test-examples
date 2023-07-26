package com.codingharbour.mockproducer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class TransactionProcessorIntegrationTest {

    private static final String HIGH_PRIO_TOPIC = "transactions_high_prio";
    private static final String REGULAR_PRIO_TOPIC = "transactions_regular_prio";

    private final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));

    @BeforeEach
    void setUp() {
        kafka.withKraft().start();
    }

    @AfterEach
    void tearDown() {
        kafka.stop();
    }

    @Test
    void testTransactionProcessing() {
        // create kafka producer
        String bootstrapServers = kafka.getBootstrapServers();
        TransactionProcessor processor = getTransactionProcessor(bootstrapServers);

        // create a few transactions
        double lowAmount = 50.2d;
        double highAmount = 250000d;
        Transaction regularPrioTransaction = new Transaction("user1", lowAmount);
        processor.process(regularPrioTransaction);
        Transaction highPrioTransaction = new Transaction("user2", highAmount);
        processor.process(highPrioTransaction);
        // validate whether messages ended up on the right topic


        //let's consume messages and validate them
        KafkaConsumer<String, String> highPrioTestConsumer =
                getKafkaConsumer(kafka.getBootstrapServers(), "integration-test-high-prio-consumer");
        KafkaConsumer<String, String> regularTestConsumer =
                getKafkaConsumer(kafka.getBootstrapServers(), "integration-test-regular-consumer");

        highPrioTestConsumer.subscribe(Collections.singletonList(HIGH_PRIO_TOPIC));
        regularTestConsumer.subscribe(Collections.singletonList(REGULAR_PRIO_TOPIC));

        ConsumerRecords<String, String> highPrioRecords = highPrioTestConsumer.poll(Duration.ofSeconds(10));
        ConsumerRecords<String, String> regularPrioRecords = regularTestConsumer.poll(Duration.ofSeconds(10));

        assertThat(highPrioRecords.count()).isEqualTo(1);
        assertThat(regularPrioRecords.count()).isEqualTo(1);

        // TODO see if you can use awaitility instead
//        await("Polling until transactions are received")
//                .pollInterval(1, SECONDS)
//                .atMost(5, SECONDS)
//                .until(() -> !service.getComments(movieId).isEmpty());
    }

    @NotNull
    private static TransactionProcessor getTransactionProcessor(String bootstrapServers) {
        KafkaProducer<String, String> producer = getKafkaProducer(bootstrapServers);
        return new TransactionProcessor(producer, HIGH_PRIO_TOPIC, REGULAR_PRIO_TOPIC);
    }

    @NotNull
    private static KafkaProducer<String, String> getKafkaProducer(String bootstrapServers) {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    @NotNull
    private static KafkaConsumer<String, String> getKafkaConsumer(String bootstrapServers, String groupId) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(props);
    }
}