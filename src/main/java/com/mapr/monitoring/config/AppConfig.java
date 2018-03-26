package com.mapr.monitoring.config;

import com.google.common.io.Resources;
import com.mapr.monitoring.client.WriteClient;
import feign.Feign;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

@Configuration
public class AppConfig {

    @Value("${telegraph.path}")
    private String telegraphUrl;

    @Bean
    public KafkaConsumer<String, String> consumer() throws IOException {
        try (InputStream props = Resources.getResource("consumer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            if (properties.getProperty("group.id") == null) {
                properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
            }
            return new KafkaConsumer<>(properties);
        }
    }

    @Bean
    public WriteClient writeClient() {
        return Feign.builder()
                .decoder(new JacksonDecoder())
                .encoder(new JacksonEncoder())
                .target(WriteClient.class, telegraphUrl);
    }
}
