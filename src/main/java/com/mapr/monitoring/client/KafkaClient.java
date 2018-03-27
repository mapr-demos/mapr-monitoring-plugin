package com.mapr.monitoring.client;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mapr.monitoring.properties.KafkaClientProperties;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static reactor.kafka.sender.SenderRecord.create;

@Slf4j

public class KafkaClient {
    private final KafkaSender<String, String> kafkaSenderWithAck;
    private final ReceiverOptions<String, String> receiverOptions;
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

    public KafkaClient(KafkaClientProperties properties) {
        Map<String, Object> producerProperties = new HashMap<>();
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        SenderOptions<String, String> senderOptionsWithAsk = SenderOptions.create(producerProperties);
        kafkaSenderWithAck = KafkaSender.create(senderOptionsWithAsk);

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, properties.getAutoCommitIntervalMS());
        receiverOptions = ReceiverOptions.create(props);
    }

    public void close() {
        kafkaSenderWithAck.close();
    }

    public Mono<Void> publishToKafka(String topic, Object payload, Long correlationId) {
        return kafkaSenderWithAck.send(Mono.just(createSenderRecord(topic, payload, correlationId)))
                .doOnSubscribe(aVoid -> log.trace("Publishing to '{}' {}", topic, payload))
                .doOnError(t -> log.error("Failed to publish to '{}' {}", topic, payload, t))
                .then();
    }

    public <T> Flux<T> subscribeOnMessages(String topic, Class<T> messageClass) {
        return subscribeOnMessages(singletonList(topic), messageClass);
    }

    public <T> Flux<T> subscribeOnMessages(Collection<String> topics, Class<T> messageClass) {
        if (topics.isEmpty()) {
            throw new RuntimeException("You can't subscribe on empty list of topics!!!");
        }
        ReceiverOptions<String, String> options = receiverOptions.subscription(topics)
                .addAssignListener(partitions -> log.debug("onPartitionsAssigned {}", partitions))
                .addRevokeListener(partitions -> log.debug("onPartitionsRevoked {}", partitions));

        KafkaReceiver<String, String> stringStringKafkaReceiver = KafkaReceiver.create(options);

        return stringStringKafkaReceiver
                .receiveAutoAck()
                .flatMap(Function.identity())
                .doOnSubscribe(aVoid -> log.debug("Subscribing to '{}'", topics))
                .doFinally(signalType -> log.debug("Unsubscribed from '{}'", topics))
                .map(message -> {
                    T payload = deserialize(message.value(), messageClass);
                    if (log.isTraceEnabled()) {
                        log.trace("Incoming message: {}, topic: {}", payload, message.topic());
                    }
                    return payload;
                });
    }

    @SneakyThrows
    private static <T> T deserialize(String json, Class<T> clazz) {
        return MAPPER.readValue(json, clazz);
    }

    @SneakyThrows
    private SenderRecord<String, String, Long> createSenderRecord(String topic, Object payload, Long correlationId) {
        return create(new ProducerRecord<>(topic, correlationId.toString(), MAPPER.writeValueAsString(payload)), correlationId);
    }
}
