package com.mapr.monitoring.config;

import com.mapr.monitoring.client.KafkaClient;
import com.mapr.monitoring.client.TelegrafClient;
import com.mapr.monitoring.properties.KafkaClientProperties;
import feign.Feign;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.HashMap;
import java.util.Map;

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
        return new KafkaClient(receiverOptions(properties));
    }

    @Bean
    public ReceiverOptions<String, String> receiverOptions(KafkaClientProperties properties) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, properties.getAutoCommitIntervalMS());
        return ReceiverOptions.create(props);
    }

    @Bean
    public TelegrafClient telegrafClient() {
        return Feign.builder()
                .target(TelegrafClient.class, telegraphUrl);
    }
}
