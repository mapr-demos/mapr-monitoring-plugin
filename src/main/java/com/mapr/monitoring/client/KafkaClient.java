package com.mapr.monitoring.client;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collection;
import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
public class KafkaClient {

    private final ReceiverOptions<String, String> receiverOptions;

    public Flux<String> subscribeOnMessages(Collection<String> topics) {
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
                    String payload = message.value().trim();
                    if (log.isTraceEnabled()) {
                        log.trace("Incoming message: {}, topic: {}", payload, message.topic());
                    }
                    return payload;
                });
    }
}
