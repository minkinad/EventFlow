package io.github.minkin.eventflow.ingestion.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OutboxMetrics {
  private final JdbcClient jdbc;
  private volatile double pending;
  private volatile double oldestAge;

  public OutboxMetrics(JdbcClient jdbc, MeterRegistry meters) {
    this.jdbc = jdbc;
    meters.gauge(
        "eventflow.outbox.pending",
        java.util.List.of(io.micrometer.core.instrument.Tag.of("service", "ingestion")),
        this,
        value -> value.pending);
    meters.gauge(
        "eventflow.outbox.oldest.age.seconds",
        java.util.List.of(io.micrometer.core.instrument.Tag.of("service", "ingestion")),
        this,
        value -> value.oldestAge);
  }

  @Scheduled(fixedDelayString = "${eventflow.metrics.poll-interval:10s}")
  public void refresh() {
    var values =
        jdbc.sql(
                """
                SELECT count(*) AS pending, COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0) AS age
                FROM ingestion_outbox WHERE published_at IS NULL
                """)
            .query()
            .singleRow();
    pending = ((Number) values.get("pending")).doubleValue();
    oldestAge = Math.max(0, ((Number) values.get("age")).doubleValue());
  }
}
