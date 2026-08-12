package io.github.minkin.eventflow.delivery.job;

import io.github.minkin.eventflow.contracts.DeliveryCommand;
import io.github.minkin.eventflow.contracts.DeliveryTarget;

import java.util.UUID;

public record DeliveryJob(UUID id, DeliveryCommand command, DeliveryTarget target, int attempt) {
}
