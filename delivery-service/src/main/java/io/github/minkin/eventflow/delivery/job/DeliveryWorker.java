package io.github.minkin.eventflow.delivery.job;

import io.github.minkin.eventflow.contracts.TargetType;
import io.github.minkin.eventflow.delivery.sink.DeliveryAdapter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class DeliveryWorker {
    private final DeliveryJobRepository repository;
    private final Map<TargetType, DeliveryAdapter> adapters = new EnumMap<>(TargetType.class);
    private final String owner = UUID.randomUUID().toString();
    private final int batchSize;
    private final int maxAttempts;
    private final int leaseSeconds;
    private final MeterRegistry registry;
    private final DeliveryFailureClassifier failureClassifier;

    public DeliveryWorker(DeliveryJobRepository repository, List<DeliveryAdapter> adapters, MeterRegistry registry,
                          DeliveryFailureClassifier failureClassifier,
                          @Value("${eventflow.delivery.batch-size:50}") int batchSize,
                          @Value("${eventflow.delivery.max-attempts:8}") int maxAttempts,
                          @Value("${eventflow.delivery.lease-seconds:60}") int leaseSeconds) {
        this.repository = repository;
        this.registry = registry;
        this.failureClassifier = failureClassifier;
        this.batchSize = batchSize;
        this.maxAttempts = maxAttempts;
        this.leaseSeconds = leaseSeconds;
        adapters.forEach(adapter -> this.adapters.put(adapter.targetType(), adapter));
    }

    @Scheduled(fixedDelayString = "${eventflow.delivery.poll-interval:500ms}")
    public void deliverBatch() {
        for (DeliveryJob job : repository.claim(batchSize, owner, leaseSeconds)) {
            try {
                DeliveryAdapter adapter = adapters.get(job.target().type());
                if (adapter == null) {
                    throw new IllegalArgumentException("Unsupported target type: " + job.target().type());
                }
                adapter.deliver(job);
                repository.succeeded(job.id());
                registry.counter("eventflow.delivery.succeeded", "target", job.target().type().name()).increment();
            } catch (RuntimeException exception) {
                registry.counter("eventflow.delivery.failed", "target", job.target().type().name()).increment();
                if (!failureClassifier.isRetryable(exception) || job.attempt() >= maxAttempts) {
                    repository.dead(job, exception.getMessage());
                } else {
                    repository.retry(job.id(), Instant.now().plusSeconds(backoffSeconds(job.attempt())),
                            exception.getMessage());
                }
            }
        }
    }

    long backoffSeconds(int attempt) {
        long exponential = Math.min(300, 1L << Math.min(attempt, 8));
        return exponential + ThreadLocalRandom.current().nextLong(0, Math.max(1, exponential / 4));
    }
}
