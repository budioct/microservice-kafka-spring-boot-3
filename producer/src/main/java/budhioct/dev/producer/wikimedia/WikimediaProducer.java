package budhioct.dev.producer.wikimedia;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class WikimediaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    // not used retryable
    //public void sendMessage(String message) {
    //    log.info("Message sent to Kafka topic orders: {}", message);
    //    kafkaTemplate
    //            .send("wikimedia-streams", message);
    //}

    // use retryable
    @Retryable(
            value = {KafkaException.class},
            maxAttempts = 5,
            backoff = @Backoff(delay = 2000))
    public void sendMessage(String message) {
        log.info("Attempting to send message to Kafka topic: {}", message);
        kafkaTemplate.send("wikimedia-streams", message)
        //        .addCallback(
        //                result -> log.info("Message sent successfully: {}", message),
        //                ex -> log.error("Failed to send message: {}", message, ex)
        //        ); // ver older return ListenableFuture
                .thenAccept(result -> log.info("Message sent successfully: {}", message))
                .exceptionally(ex -> {
                    log.error("Failed to send message: {}", message, ex);
                    return null;
                }); // ver new return CompletableFuture
    }

    // recover retry
    @Recover
    public void recover(KafkaException e, String message) {
        log.error("Failed to send message after retries: {}", message);
        // Implement fallback logic (e.g., save to a database or an alternative queue)
    }

}
