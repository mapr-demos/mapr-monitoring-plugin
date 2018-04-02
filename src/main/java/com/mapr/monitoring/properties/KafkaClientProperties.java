package com.mapr.monitoring.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("kafka")
@Data
public class KafkaClientProperties {
    private String groupId;
    private Integer autoCommitIntervalMS;
}
