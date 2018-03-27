package com.mapr.monitoring;

import com.mapr.monitoring.properties.KafkaClientProperties;
import com.mapr.monitoring.properties.MonitoringPluginProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({MonitoringPluginProperties.class, KafkaClientProperties.class})
public class MonitoringPlugin {

    public static void main(String[] args) {
        SpringApplication.run(MonitoringPlugin.class, args);
    }
}
