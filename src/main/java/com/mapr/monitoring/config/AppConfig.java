package com.mapr.monitoring.config;

import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

@Configuration
public class AppConfig {

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
}
