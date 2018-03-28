package com.mapr.monitoring.client;

import com.mapr.monitoring.properties.KafkaClientProperties;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

@Slf4j
public class KafkaClient {

    private final ReceiverOptions<String, String> receiverOptions;

    public KafkaClient(KafkaClientProperties properties) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, properties.getAutoCommitIntervalMS());
        receiverOptions = ReceiverOptions.create(props);
    }

    @SneakyThrows
    private static <T> T deserialize(String json) {
        return (T) json.trim();
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
                    T payload = deserialize(message.value());
                    if (log.isTraceEnabled()) {
                        log.trace("Incoming message: {}, topic: {}", payload, message.topic());
                    }
                    return payload;
                });
    }
}
