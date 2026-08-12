package io.github.minkin.eventflow.delivery.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.minkin.eventflow.contracts.DeliveryCommand;
import io.github.minkin.eventflow.contracts.EventTopics;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class DeliveryCommandListener {
    private final ObjectMapper objectMapper;
    private final DeliveryJobRepository repository;

    public DeliveryCommandListener(ObjectMapper objectMapper, DeliveryJobRepository repository) {
        this.objectMapper = objectMapper;
        this.repository = repository;
    }

    @KafkaListener(topics = "${eventflow.kafka.delivery-topic:" + EventTopics.DELIVERY_COMMANDS + "}")
    public void consume(String rawMessage, Acknowledgment acknowledgment) {
        try {
            repository.accept(objectMapper.readValue(rawMessage, DeliveryCommand.class), rawMessage);
        } catch (JsonProcessingException exception) {
            repository.malformed(rawMessage, exception.getMessage());
        }
        acknowledgment.acknowledge();
    }
}
