package io.github.minkin.eventflow.delivery.job;

import io.github.minkin.eventflow.contracts.TargetType;
import io.github.minkin.eventflow.delivery.sink.DeliveryAdapter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

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

  public DeliveryWorker(
      DeliveryJobRepository repository,
      List<DeliveryAdapter> adapters,
      MeterRegistry registry,
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
    for (int index = 0; index < batchSize; index++) {
      if (Thread.currentThread().isInterrupted()) {
        return;
      }
      var claimed = repository.claim(1, owner, leaseSeconds);
      if (claimed.isEmpty()) {
        return;
      }
      DeliveryJob job = claimed.getFirst();
      if (!repository.renew(job, leaseSeconds)) {
        registry.counter("eventflow.delivery.lease.lost").increment();
        continue;
      }
      try {
        if (job.attempt() > maxAttempts) {
          repository.dead(job, "DELIVERY_ATTEMPTS_EXHAUSTED", "Recovery attempt budget exhausted");
          continue;
        }
        DeliveryAdapter adapter = adapters.get(job.target().type());
        if (adapter == null) {
          throw new IllegalArgumentException("Unsupported target type: " + job.target().type());
        }
        adapter.deliver(job);
        repository.succeeded(job);
        registry
            .counter("eventflow.delivery.succeeded", "target", job.target().type().name())
            .increment();
      } catch (LeaseLostException exception) {
        registry.counter("eventflow.delivery.lease.lost").increment();
      } catch (RuntimeException exception) {
        registry
            .counter("eventflow.delivery.failed", "target", job.target().type().name())
            .increment();
        if (!failureClassifier.isRetryable(exception) || job.attempt() >= maxAttempts) {
          repository.dead(
              job,
              failureClassifier.isRetryable(exception)
                  ? "DELIVERY_ATTEMPTS_EXHAUSTED"
                  : "DELIVERY_TERMINAL_FAILURE",
              exception.getMessage());
        } else {
          repository.retry(
              job,
              Instant.now().plusSeconds(backoffSeconds(job.attempt())),
              exception.getMessage());
        }
      }
    }
  }

  long backoffSeconds(int attempt) {
    long ceiling = Math.min(300, 1L << Math.min(Math.max(attempt, 1), 9));
    return ThreadLocalRandom.current().nextLong(Math.max(1, ceiling / 2), ceiling + 1);
  }
}
