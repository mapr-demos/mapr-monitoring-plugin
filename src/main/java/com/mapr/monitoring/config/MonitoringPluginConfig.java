package com.mapr.monitoring.config;

import com.mapr.monitoring.client.KafkaClient;
import com.mapr.monitoring.client.TelegrafClient;
import com.mapr.monitoring.properties.KafkaClientProperties;
import feign.Feign;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;

@Configuration
@RequiredArgsConstructor
public class MonitoringPluginConfig {

    @Value("${telegraph.path}")
    private String telegraphUrl;

    @Bean
    public AdminClient adminClient() {
        return AdminClient.create(new HashMap<>());
    }

    @Bean
    public KafkaClient kafkaClient(KafkaClientProperties properties) {
        return new KafkaClient(properties);
    }

    @Bean
    public TelegrafClient telegrafClient() {
        return Feign.builder()
                .target(TelegrafClient.class, telegraphUrl);
    }
}
