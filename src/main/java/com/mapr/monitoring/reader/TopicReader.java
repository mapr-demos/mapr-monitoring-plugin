package com.mapr.monitoring.reader;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Component;

import java.util.Collections;

@Slf4j
@Component
@RequiredArgsConstructor
public class TopicReader {

    private final KafkaConsumer<String, String> consumer;

    public void readTopic(String metricStream, String host, String topic) {
        consumer.subscribe(Collections.singletonList(metricStream + ":" + host + "_" + topic));
        int timeouts = 0;
        while (true) {
            // read records with a short timeout. If we time out, we don't really care.
            ConsumerRecords<String, String> records = consumer.poll(200);
            if (records.count() == 0) {
                timeouts++;
            } else {
                log.info("Got {} records after {} timeouts\n", records.count(), timeouts);
                timeouts = 0;
            }

            for (ConsumerRecord<String, String> record : records) {

                log.info("=======> VALUE: " + record.value().trim());
            }
        }
    }
}

