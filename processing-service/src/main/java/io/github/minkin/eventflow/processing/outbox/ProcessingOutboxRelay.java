package io.github.minkin.eventflow.processing.outbox;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Component
public class ProcessingOutboxRelay {
  private final JdbcClient jdbc;
  private final KafkaTemplate<String, String> kafka;
  private final int batchSize;
  private final TransactionTemplate transactionTemplate;

  public ProcessingOutboxRelay(
      JdbcClient jdbc,
      KafkaTemplate<String, String> kafka,
      PlatformTransactionManager transactionManager,
      @Value("${eventflow.outbox.batch-size:100}") int batchSize) {
    this.jdbc = jdbc;
    this.kafka = kafka;
    this.batchSize = batchSize;
    this.transactionTemplate = new TransactionTemplate(transactionManager);
  }

  @Scheduled(fixedDelayString = "${eventflow.outbox.poll-interval:250ms}")
  public void run() {
    String owner = UUID.randomUUID().toString();
    for (Message message : claimInTransaction(owner)) {
      int renewed =
          jdbc.sql(
                  """
                    UPDATE processing_outbox SET claimed_until=now() + interval '30 seconds'
                    WHERE id=:id AND published_at IS NULL AND claimed_by=:owner AND claimed_until > now()
                    """)
              .param("id", message.id())
              .param("owner", owner)
              .update();
      if (renewed != 1) {
        continue;
      }
      try {
        kafka.send(message.topic(), message.key(), message.payload()).get(10, TimeUnit.SECONDS);
        jdbc.sql(
                "UPDATE processing_outbox SET published_at=now(), claimed_until=NULL WHERE id=:id AND published_at IS NULL AND claimed_by=:owner AND claimed_until > now()")
            .param("id", message.id())
            .param("owner", owner)
            .update();
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        return;
      } catch (Exception exception) {
        jdbc.sql(
                """
                                UPDATE processing_outbox SET claimed_until=NULL, claimed_by=NULL, last_error=:error,
                        next_attempt_at=now() + (LEAST(300, power(2, LEAST(attempts, 9))) * (0.5 + random() * 0.5) * interval '1 second')
                                WHERE id=:id AND published_at IS NULL AND claimed_by=:owner AND claimed_until > now()
                                """)
            .param("id", message.id())
            .param("owner", owner)
            .param("error", "KAFKA_PUBLICATION_FAILED")
            .update();
      }
    }
  }

  private List<Message> claimInTransaction(String owner) {
    List<Message> claimed =
        transactionTemplate.execute(
            status ->
                jdbc.sql(
                        """
                        WITH candidates AS (
                            SELECT id FROM processing_outbox
                            WHERE published_at IS NULL AND next_attempt_at <= now() AND (claimed_until IS NULL OR claimed_until < now())
                            ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT :limit
                        )
                        UPDATE processing_outbox o SET claimed_by=:owner,
                            claimed_until=now() + interval '30 seconds', attempts=attempts+1
                        FROM candidates c WHERE o.id=c.id
                        RETURNING o.id, o.topic, o.message_key, o.payload::text
                        """)
                    .param("limit", batchSize)
                    .param("owner", owner)
                    .query(
                        (rs, n) ->
                            new Message(
                                rs.getObject(1, UUID.class),
                                rs.getString(2),
                                rs.getString(3),
                                rs.getString(4)))
                    .list());
    return claimed == null ? List.of() : claimed;
  }

  private String truncate(String value) {
    String safe = value == null ? "unknown publication failure" : value;
    return safe.substring(0, Math.min(2000, safe.length()));
  }

  record Message(UUID id, String topic, String key, String payload) {}
}
