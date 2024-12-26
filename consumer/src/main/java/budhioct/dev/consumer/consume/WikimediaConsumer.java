package budhioct.dev.consumer.consume;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class WikimediaConsumer {

    @KafkaListener(topics = "wikimedia-streams", groupId = "liverpool")
    public void consumeMsg(String msg) {
        log.info("Consuming the message from wikimedia-stream Topic:: {}", msg);
        // Please feel free to do anything you want with the data
    }
}
