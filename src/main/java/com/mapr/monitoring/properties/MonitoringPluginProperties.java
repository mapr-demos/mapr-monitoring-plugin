package com.mapr.monitoring.properties;


import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("monitoring")
@Data
public class MonitoringPluginProperties {
    private String[] streams;
    private String clusterHost;
}
