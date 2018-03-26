package com.mapr.monitoring.reader;

import com.mapr.monitoring.client.WriteClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Component
@RequiredArgsConstructor
public class TopicReader {

    private final WriteClient client;
    private final KafkaConsumer<String, String> consumer;

    @Value("${metric.stream}")
    private String stream;

    @Value("${metric.host}")
    private String host;

    @Value("${metric.topic}")
    private String topic;

    @Value("${metric.read.timeout}")
    private int readTimeout;

    public void readTopic() {
        consumer.subscribe(Collections.singletonList(String.format("%s:%s_%s", stream, host, topic)));
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(readTimeout);
                for (ConsumerRecord<String, String> record : records) {
                    log.info("=======> VALUE: " + record.value().trim());
//                    log.info(client.writeMetric(record.value().trim()));
                }
            }
        });
    }
}

