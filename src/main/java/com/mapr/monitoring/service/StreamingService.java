package com.mapr.monitoring.service;

import com.google.common.collect.Lists;
import com.mapr.monitoring.client.KafkaClient;
import com.mapr.monitoring.client.TelegrafClient;
import com.mapr.monitoring.properties.MonitoringPluginProperties;
import com.mapr.streams.impl.admin.MarlinAdminImpl;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.scheduler.Schedulers;

import javax.annotation.PostConstruct;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
@RequiredArgsConstructor
public class StreamingService {
    private final MarlinAdminImpl adminClient;
    private final KafkaClient kafkaClient;
    private final TelegrafClient telegrafClient;
    private final MonitoringPluginProperties properties;

    @PostConstruct
    public void init() {
        String[] streams = properties.getStreams();

        Set<String> convertedTopics = Lists.newArrayList(streams)
                .stream()
                .flatMap(this::convertToStringStream)
                .collect(Collectors.toSet());

        kafkaClient.subscribeOnMessages(convertedTopics, String.class)
                .map(this::convertToTelegrafFormat)
                .doOnNext(telegrafClient::writeMetric)
                .doOnError(t -> log.error("Error while sending metric to telegraf", t))
                .subscribeOn(Schedulers.single())
                .subscribe();
    }

    private Stream<String> convertToStringStream(String stream) {
        return getAllTopicsForStream(stream)
                .stream()
                .map(topic -> convertHostStreamTopic(properties.getClusterHost(), stream, topic));
    }

    @SneakyThrows
    private Set<String> getAllTopicsForStream(String streamName) {
        return adminClient.listTopics(streamName).keySet();
    }

    private String convertHostStreamTopic(String host, String stream, String topic) {
        return String.format("%s:%s_%s", stream, host, topic);
    }

    //TODO: write method that converts from kafka format to telegraf format
    private String convertToTelegrafFormat(String initial) {
        return initial;
    }
}
