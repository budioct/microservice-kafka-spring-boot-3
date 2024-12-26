package budhioct.dev.consumer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class WikimediaTopicConfig {

    @Bean
    public NewTopic wikimediaTopic() {
        // Kafka topic orders with 3 partitions
        return TopicBuilder
                .name("wikimedia-streams")
                .partitions(3)
                .replicas(1)
                .build();
    }

}
