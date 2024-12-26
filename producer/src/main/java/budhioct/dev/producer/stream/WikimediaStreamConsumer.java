package budhioct.dev.producer.stream;

import budhioct.dev.producer.wikimedia.WikimediaProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

@Slf4j
@Service
public class WikimediaStreamConsumer {

    private final WebClient webClient;
    private final WikimediaProducer producer;

    public WikimediaStreamConsumer(WebClient.Builder webClientBuilder, WikimediaProducer wikimediaProducer) {
        this.webClient = webClientBuilder.baseUrl("https://stream.wikimedia.org/v2").build();
        this.producer = wikimediaProducer;
    }

    //public void consumeStreamAndPublish() {
    //    webClient.get()
    //            .uri("/stream/recentchange")
    //            .retrieve()
    //            .bodyToFlux(String.class)
    //            .subscribe(producer::sendMessage);
    //}

    public void consumeStreamAndPublish() {
        webClient.get()
                .uri("/stream/recentchange")
                .retrieve()
                .bodyToFlux(String.class)
                .onErrorResume(error -> {
                    log.error("Error while consuming stream: {}", error.getMessage());
                    // Handle error: Retry logic or fallback
                    return Flux.empty(); // Skip this error and continue
                })
                .subscribe(producer::sendMessage); // send to kafka topic
    }

}
