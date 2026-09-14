package io.github.minkin.eventflow.ingestion;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.minkin.eventflow.ingestion.api.EventSubmission;
import io.github.minkin.eventflow.ingestion.domain.CanonicalJson;
import io.github.minkin.eventflow.ingestion.domain.EventIngestionService;
import io.github.minkin.eventflow.ingestion.outbox.OutboxPublisher;
import io.github.minkin.eventflow.ingestion.outbox.OutboxRepository;
import io.github.minkin.eventflow.ingestion.persistence.IngestionRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.kafka.KafkaContainer;

class OutboxKafkaIT extends PostgresIntegrationSupport {
  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");

  @Test
  void brokerAcceptsBeforeRelayCrashesAndSameEventIsRepublishedAfterRecovery() throws Exception {
    var repository = new IngestionRepository(jdbc, mapper);
    var service =
        transactional(new EventIngestionService(repository, new CanonicalJson(mapper), mapper));
    var event =
        new EventSubmission(
            UUID.randomUUID(),
            "order.created",
            "test",
            1,
            Instant.now(),
            mapper.createObjectNode(),
            Map.of());
    service.ingest(event, null);
    var outbox = transactional(new OutboxRepository(jdbc));
    var first = outbox.claim(1, "crashed-relay", 60).getFirst();
    var factory =
        new DefaultKafkaProducerFactory<String, String>(
            Map.of(
                "bootstrap.servers",
                KAFKA.getBootstrapServers(),
                "acks",
                "all",
                "enable.idempotence",
                true),
            new StringSerializer(),
            new StringSerializer());
    try (var consumer =
        new KafkaConsumer<String, String>(
            Map.of(
                "bootstrap.servers",
                KAFKA.getBootstrapServers(),
                "group.id",
                UUID.randomUUID().toString(),
                "auto.offset.reset",
                "earliest",
                "enable.auto.commit",
                false),
            new StringDeserializer(),
            new StringDeserializer())) {
      var kafka = new KafkaTemplate<>(factory);
      kafka
          .send(first.topic(), first.messageKey(), first.payload())
          .get(20, java.util.concurrent.TimeUnit.SECONDS);
      jdbc.sql("UPDATE ingestion_outbox SET claimed_until=now() - interval '1 second'").update();
      new OutboxPublisher(outbox, kafka, new SimpleMeterRegistry(), 10, 30).publishBatch();
      consumer.subscribe(List.of(first.topic()));
      var records = new java.util.ArrayList<String>();
      long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
      while (records.size() < 2 && System.nanoTime() < deadline) {
        consumer
            .poll(Duration.ofMillis(500))
            .forEach(
                record -> {
                  assertThat(record.key()).isEqualTo(event.eventId().toString());
                  records.add(record.value());
                });
      }
      assertThat(records).hasSize(2).allMatch(first.payload()::equals);
      assertThat(repository.findStatus(event.eventId()).orElseThrow().status())
          .isEqualTo("PUBLISHED");
    } finally {
      factory.destroy();
    }
  }
}
