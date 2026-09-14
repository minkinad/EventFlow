package io.github.minkin.eventflow.ingestion.outbox;

import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class OutboxRepository {
  private final JdbcClient jdbc;

  public OutboxRepository(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  @Transactional
  public List<OutboxMessage> claim(int batchSize, String owner, int leaseSeconds) {
    return jdbc.sql(
            """
                        WITH candidates AS (
                            SELECT id FROM ingestion_outbox
                            WHERE published_at IS NULL AND next_attempt_at <= now()
                              AND (claimed_until IS NULL OR claimed_until < now())
                            ORDER BY created_at
                            FOR UPDATE SKIP LOCKED
                            LIMIT :batchSize
                        )
                        UPDATE ingestion_outbox o
                        SET claimed_by = :owner,
                            claimed_until = now() + (:leaseSeconds * interval '1 second'),
                            attempts = attempts + 1
                        FROM candidates c
                        WHERE o.id = c.id
                        RETURNING o.id, o.topic, o.message_key, o.payload::text
                        """)
        .param("batchSize", batchSize)
        .param("owner", owner)
        .param("leaseSeconds", leaseSeconds)
        .query(
            (rs, rowNum) ->
                new OutboxMessage(
                    rs.getObject("id", UUID.class), rs.getString("topic"),
                    rs.getString("message_key"), rs.getString("payload")))
        .list();
  }

  @Transactional
  public boolean markPublished(UUID id, String owner) {
    UUID eventId =
        jdbc.sql(
                """
                        UPDATE ingestion_outbox
                        SET published_at = now(), claimed_by = NULL, claimed_until = NULL, last_error = NULL
                        WHERE id = :id AND published_at IS NULL AND claimed_by = :owner AND claimed_until > now()
                        RETURNING aggregate_id
                        """)
            .param("id", id)
            .param("owner", owner)
            .query(UUID.class)
            .optional()
            .orElse(null);
    if (eventId == null) {
      return false;
    }
    jdbc.sql("UPDATE ingested_event SET status = 'PUBLISHED' WHERE event_id = :eventId")
        .param("eventId", eventId)
        .update();
    return true;
  }

  public boolean renew(UUID id, String owner, int leaseSeconds) {
    return jdbc.sql(
                """
                UPDATE ingestion_outbox SET claimed_until=now() + (:lease * interval '1 second')
                WHERE id=:id AND published_at IS NULL AND claimed_by=:owner AND claimed_until > now()
                """)
            .param("id", id)
            .param("owner", owner)
            .param("lease", leaseSeconds)
            .update()
        == 1;
  }

  public void release(UUID id, String owner, String error) {
    jdbc.sql(
            """
                        UPDATE ingestion_outbox
                        SET claimed_by = NULL, claimed_until = NULL, last_error = :error,
                    next_attempt_at = now() + (LEAST(300, power(2, LEAST(attempts, 9))) * (0.5 + random() * 0.5) * interval '1 second')
                        WHERE id = :id AND published_at IS NULL AND claimed_by = :owner AND claimed_until > now()
                        """)
        .param("id", id)
        .param("owner", owner)
        .param(
            "error",
            error == null
                ? "unknown publication failure"
                : error.substring(0, Math.min(error.length(), 2000)))
        .update();
  }
}
