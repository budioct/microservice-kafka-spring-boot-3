package budhioct.dev.producer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"wikimedia-streams"}, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
class WikimediaProducerTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private Consumer<String, String> kafkaConsumer;

    @BeforeEach
    void setUp(EmbeddedKafkaBroker embeddedKafkaBroker) {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("liverpool", "true", embeddedKafkaBroker);
        kafkaConsumer = new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), new StringDeserializer()).createConsumer();
        kafkaConsumer.subscribe(Collections.singletonList("wikimedia-streams"));
    }

    @Test
    void testSendMessageFirst() {
        String testMessage = "Test message for Kafka";
        kafkaTemplate.send("wikimedia-streams", testMessage);

        ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(kafkaConsumer);
        Assertions.assertTrue(records.count() > 0);
        Assertions.assertEquals(testMessage, records.iterator().next().value());
    }

    @Test
    void testSendMessageSecond() throws InterruptedException {
        String testMessage = "Test message for Kafka";
        kafkaTemplate.send("wikimedia-streams", testMessage);

        // Validate consumer received the message
        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));
        Assertions.assertTrue(records.count() > 0);
        Assertions.assertEquals(testMessage, records.iterator().next().value());
    }

}
