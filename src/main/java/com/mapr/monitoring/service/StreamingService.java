package com.mapr.monitoring.service;

import com.google.common.collect.Lists;
import com.mapr.monitoring.client.KafkaClient;
import com.mapr.monitoring.client.TelegrafClient;
import com.mapr.monitoring.properties.MonitoringPluginProperties;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.stereotype.Service;
import reactor.core.scheduler.Schedulers;

import javax.annotation.PostConstruct;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
@RequiredArgsConstructor
public class StreamingService {
    private final AdminClient adminClient;
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
                .log()
                .doOnNext(telegrafClient::writeMetric)
                .doOnError(t -> log.error("Error while sending metric to telegraf", t))
                .subscribeOn(Schedulers.single())
                .subscribe();
    }

    private Stream<String> convertToStringStream(String stream) {
        return getAllTopicsForStream(stream)
                .stream()
                .map(topic -> convertHostStreamTopic(stream, topic));
    }

    @SneakyThrows
    private Set<String> getAllTopicsForStream(String streamName) {
        return adminClient.listTopics(streamName).names().get();
    }

    private String convertHostStreamTopic(String stream, String topic) {
        return String.format("%s:%s", stream, topic);
    }

    private String convertToTelegrafFormat(String initial) {

        String withoutPut = normalize(initial);
        String tags = getTags(initial);
        withoutPut = removeTags(withoutPut);

        String[] split = withoutPut.split(" ");

        String timestamp = TimeUnit.NANOSECONDS.convert(Long.parseLong(split[1]), TimeUnit.MILLISECONDS) + "";

        return String.format("%s,%s value=%s %s", split[0], tags, split[2], timestamp);
    }

    private static String normalize(String str) {
        String regex = "\\s*\\bput\\b\\s*";
        return str.replaceAll(regex, "");
    }

    private static String getTags(String str) {
        return str.substring(str.indexOf("fqdn"))
                .replaceAll("\\s+", ",");
    }

    private static String removeTags(String str) {
        return str.substring(0, str.indexOf("fqdn"));
    }
}
