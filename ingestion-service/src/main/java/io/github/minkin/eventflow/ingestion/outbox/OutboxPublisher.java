package io.github.minkin.eventflow.ingestion.outbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OutboxPublisher {
  private final OutboxRepository repository;
  private final KafkaTemplate<String, String> kafka;
  private final int batchSize;
  private final int leaseSeconds;
  private final Counter published;
  private final Counter failures;

  public OutboxPublisher(
      OutboxRepository repository,
      KafkaTemplate<String, String> kafka,
      MeterRegistry meterRegistry,
      @Value("${eventflow.outbox.batch-size:100}") int batchSize,
      @Value("${eventflow.outbox.lease-seconds:30}") int leaseSeconds) {
    this.repository = repository;
    this.kafka = kafka;
    this.batchSize = batchSize;
    this.leaseSeconds = leaseSeconds;
    this.published = meterRegistry.counter("eventflow.outbox.published", "service", "ingestion");
    this.failures = meterRegistry.counter("eventflow.outbox.failures", "service", "ingestion");
  }

  @Scheduled(fixedDelayString = "${eventflow.outbox.poll-interval:250ms}")
  public void publishBatch() {
    String owner = UUID.randomUUID().toString();
    for (OutboxMessage message : repository.claim(batchSize, owner, leaseSeconds)) {
      if (!repository.renew(message.id(), owner, leaseSeconds)) {
        continue;
      }
      try {
        kafka
            .send(message.topic(), message.messageKey(), message.payload())
            .get(Duration.ofSeconds(10).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
        if (repository.markPublished(message.id(), owner)) {
          published.increment();
        }
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        return;
      } catch (Exception exception) {
        failures.increment();
        repository.release(message.id(), owner, "KAFKA_PUBLICATION_FAILED");
      }
    }
  }
}
