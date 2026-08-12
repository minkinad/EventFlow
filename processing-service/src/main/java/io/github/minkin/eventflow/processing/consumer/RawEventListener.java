package io.github.minkin.eventflow.processing.consumer;

import io.github.minkin.eventflow.contracts.EventTopics;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class RawEventListener {
    private final ProcessingCoordinator coordinator;

    public RawEventListener(ProcessingCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @KafkaListener(topics = "${eventflow.kafka.raw-topic:" + EventTopics.RAW_EVENTS + "}")
    public void consume(String message, Acknowledgment acknowledgment) {
        coordinator.handle(message);
        acknowledgment.acknowledge();
    }
}
