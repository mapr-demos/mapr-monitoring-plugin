package com.mapr.monitoring.config;

import com.mapr.monitoring.client.KafkaClient;
import com.mapr.monitoring.client.TelegrafClient;
import com.mapr.monitoring.properties.KafkaClientProperties;
import com.mapr.streams.impl.admin.MarlinAdminImpl;
import feign.Feign;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class MonitoringPluginConfig {

    @Value("${telegraph.path}")
    private String telegraphUrl;

    @SneakyThrows
    public MarlinAdminImpl adminClient(){
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        return new MarlinAdminImpl(configuration);
    }

    @Bean
    public KafkaClient kafkaClient(KafkaClientProperties properties) {
        return new KafkaClient(properties);
    }

    @Bean
    public TelegrafClient telegrafClient() {
        return Feign.builder()
                .decoder(new JacksonDecoder())
                .encoder(new JacksonEncoder())
                .target(TelegrafClient.class, telegraphUrl);
    }
}
