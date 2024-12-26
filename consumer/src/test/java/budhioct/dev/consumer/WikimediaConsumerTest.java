package budhioct.dev.consumer;

import budhioct.dev.consumer.consume.WikimediaConsumer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"wikimedia-streams"})
public class WikimediaConsumerTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @MockitoSpyBean
    private WikimediaConsumer wikimediaConsumer;

    @Test
    void testConsumeMessage() throws InterruptedException {
        String testMessage = "Test message for Kafka";
        kafkaTemplate.send("wikimedia-streams", testMessage);

        Thread.sleep(2000); // Beri waktu untuk Kafka listener memproses pesan

        verify(wikimediaConsumer, times(1)).consumeMsg(testMessage);
    }

    @Test
    void testConsumerErrorHandling() {
        String invalidMessage = ""; // Simulasi pesan tidak valid
        kafkaTemplate.send("wikimedia-streams", invalidMessage);

        // Verifikasi konsumer menangani error
        verify(wikimediaConsumer, times(0)).consumeMsg(invalidMessage);
    }


}
