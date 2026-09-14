package io.github.minkin.eventflow.processing.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.minkin.eventflow.contracts.DeadLetterEvent;
import io.github.minkin.eventflow.contracts.DeliveryCommand;
import io.github.minkin.eventflow.contracts.EventTopics;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class ProcessingRepository {
  private final JdbcClient jdbc;
  private final ObjectMapper objectMapper;

  public ProcessingRepository(JdbcClient jdbc, ObjectMapper objectMapper) {
    this.jdbc = jdbc;
    this.objectMapper = objectMapper;
  }

  @Transactional
  public ClaimResult claim(UUID eventId, String contentHash, String rawMessage, int leaseSeconds) {
    UUID token = UUID.randomUUID();
    int inserted =
        jdbc.sql(
                """
                        INSERT INTO processing_record
                            (event_id, content_hash, raw_message, status, attempt, lease_until, lease_token, first_seen_at, updated_at)
                        VALUES (:eventId, :hash, CAST(:raw AS jsonb), 'PROCESSING', 1,
                                now() + (:lease * interval '1 second'), :token, now(), now())
                        ON CONFLICT (event_id) DO NOTHING
                        """)
            .param("eventId", eventId)
            .param("hash", contentHash)
            .param("raw", rawMessage)
            .param("lease", leaseSeconds)
            .param("token", token)
            .update();
    if (inserted == 1) {
      return new ClaimResult(ClaimDecision.CLAIMED, 1, token);
    }

    Map<String, Object> current =
        jdbc.sql(
                """
                        SELECT raw_message = CAST(:raw AS jsonb) AS same_content, status, lease_until, attempt
                        FROM processing_record WHERE event_id = :eventId
                        """)
            .param("eventId", eventId)
            .param("raw", rawMessage)
            .query()
            .singleRow();
    if (!Boolean.TRUE.equals(current.get("same_content"))) {
      return new ClaimResult(
          ClaimDecision.CONTENT_CONFLICT, ((Number) current.get("attempt")).intValue(), null);
    }
    if ("SUCCEEDED".equals(current.get("status")) || "FAILED".equals(current.get("status"))) {
      return new ClaimResult(
          ClaimDecision.DUPLICATE, ((Number) current.get("attempt")).intValue(), null);
    }

    Optional<Integer> reclaimed =
        jdbc.sql(
                """
                        UPDATE processing_record
                        SET status = 'PROCESSING', attempt = attempt + 1, lease_token = :token,
                            lease_until = now() + (:lease * interval '1 second'), updated_at = now()
                        WHERE event_id = :eventId
                          AND ((status IN ('RETRYABLE', 'REPLAY_REQUESTED') AND next_attempt_at <= now())
                            OR (status = 'PROCESSING' AND lease_until < now()))
                        RETURNING attempt
                        """)
            .param("lease", leaseSeconds)
            .param("eventId", eventId)
            .param("token", token)
            .query(Integer.class)
            .optional();
    return reclaimed
        .map(attempt -> new ClaimResult(ClaimDecision.CLAIMED, attempt, token))
        .orElseGet(
            () ->
                new ClaimResult(
                    ClaimDecision.BUSY, ((Number) current.get("attempt")).intValue(), null));
  }

  public void releaseForRetry(
      UUID eventId, UUID token, Instant nextAttempt, String code, String message) {
    int changed =
        jdbc.sql(
                """
                        UPDATE processing_record
                        SET status = 'RETRYABLE', next_attempt_at = :nextAttempt, lease_token = NULL,
                            failure_code = :code, failure_message = :message,
                            lease_until = NULL, updated_at = now()
                        WHERE event_id = :eventId AND status = 'PROCESSING'
                          AND lease_token = :token AND lease_until > now()
                        """)
            .param("eventId", eventId)
            .param("code", code)
            .param("message", truncate(message, 2000))
            .param("token", token)
            .param("nextAttempt", nextAttempt.atOffset(java.time.ZoneOffset.UTC))
            .update();
    if (changed != 1) {
      throw new LeaseLostException();
    }
  }

  @Transactional
  public void complete(
      UUID eventId, UUID token, String pipeline, int pipelineVersion, DeliveryCommand command) {
    int changed =
        jdbc.sql(
                """
                        UPDATE processing_record
                        SET status = 'SUCCEEDED', pipeline_name = :pipeline, pipeline_version = :version,
                            lease_until = NULL, updated_at = now(), failure_code = NULL, failure_message = NULL
                        WHERE event_id = :eventId AND status = 'PROCESSING'
                          AND lease_token = :token AND lease_until > now()
                        """)
            .param("pipeline", pipeline)
            .param("version", pipelineVersion)
            .param("eventId", eventId)
            .param("token", token)
            .update();
    if (changed != 1) {
      throw new LeaseLostException();
    }
    insertOutbox(eventId, EventTopics.DELIVERY_COMMANDS, write(command));
    jdbc.sql(
            """
                        UPDATE processing_dead_letter SET status = 'RESOLVED'
                        WHERE event_id = :eventId AND status = 'REPLAY_REQUESTED'
                        """)
        .param("eventId", eventId)
        .update();
  }

  @Transactional
  public UUID fail(
      UUID eventId, UUID token, String code, String message, DeadLetterEvent deadLetter) {
    jdbc.sql(
            """
                        UPDATE processing_dead_letter SET status = 'REPLAY_FAILED'
                        WHERE event_id = :eventId AND status = 'REPLAY_REQUESTED'
                        """)
        .param("eventId", eventId)
        .update();
    int changed =
        jdbc.sql(
                """
                        UPDATE processing_record
                        SET status = 'FAILED', failure_code = :code, failure_message = :message,
                            lease_until = NULL, updated_at = now()
                        WHERE event_id = :eventId AND status = 'PROCESSING'
                          AND lease_token = :token AND lease_until > now()
                        """)
            .param("code", code)
            .param("message", truncate(message, 2000))
            .param("eventId", eventId)
            .param("token", token)
            .update();
    if (changed != 1) {
      throw new LeaseLostException();
    }
    insertDeadLetter(deadLetter, true);
    insertOutbox(eventId, EventTopics.PROCESSING_DLQ, write(deadLetter));
    return deadLetter.deadLetterId();
  }

  @Transactional
  public UUID reject(DeadLetterEvent deadLetter) {
    if (insertDeadLetter(deadLetter, false)) {
      insertOutbox(deadLetter.eventId(), EventTopics.PROCESSING_DLQ, write(deadLetter));
    }
    return deadLetter.deadLetterId();
  }

  @Transactional
  public void replay(UUID deadLetterId) {
    Map<String, Object> row =
        jdbc.sql(
                """
                        SELECT event_id, stage, original_message::text AS original_message
                        FROM processing_dead_letter
                        WHERE id = :id AND status IN ('OPEN', 'REPLAY_FAILED')
                        FOR UPDATE
                        """)
            .param("id", deadLetterId)
            .query(
                (rs, rowNum) ->
                    Map.<String, Object>of(
                        "event_id", rs.getObject("event_id", UUID.class),
                        "stage", rs.getString("stage"),
                        "original_message", rs.getString("original_message")))
            .optional()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Replayable dead letter not found: " + deadLetterId));
    UUID eventId = (UUID) row.get("event_id");
    if (!"PROCESSING".equals(row.get("stage"))) {
      throw new IllegalArgumentException(
          "Only processing-stage dead letters can be replayed without supplying corrected content");
    }
    int changed =
        jdbc.sql(
                """
                        UPDATE processing_record SET status = 'REPLAY_REQUESTED', attempt = 0,
                            lease_token = NULL, lease_until = NULL, next_attempt_at = now(), updated_at = now()
                        WHERE event_id = :eventId AND status = 'FAILED'
                        """)
            .param("eventId", eventId)
            .update();
    if (changed != 1) {
      throw new IllegalArgumentException("Event is no longer failed; replay cannot be applied");
    }
    jdbc.sql(
            """
                        UPDATE processing_dead_letter SET status = 'REPLAY_REQUESTED', replayed_at = now()
                        WHERE id = :id
                        """)
        .param("id", deadLetterId)
        .update();
    insertOutbox(eventId, EventTopics.RAW_EVENTS, (String) row.get("original_message"));
  }

  public Optional<String> findDeadLetterStatus(UUID id) {
    return jdbc.sql("SELECT status FROM processing_dead_letter WHERE id = :id")
        .param("id", id)
        .query(String.class)
        .optional();
  }

  public Optional<ProcessingStatus> findStatus(UUID eventId) {
    return jdbc.sql(
            """
                        SELECT event_id, status, attempt, pipeline_name, pipeline_version,
                               failure_code, failure_message, first_seen_at, updated_at
                        FROM processing_record WHERE event_id = :eventId
                        """)
        .param("eventId", eventId)
        .query(
            (rs, rowNum) ->
                new ProcessingStatus(
                    rs.getObject("event_id", UUID.class),
                    rs.getString("status"),
                    rs.getInt("attempt"),
                    rs.getString("pipeline_name"),
                    (Integer) rs.getObject("pipeline_version"),
                    rs.getString("failure_code"),
                    rs.getString("failure_message"),
                    rs.getTimestamp("first_seen_at").toInstant(),
                    rs.getTimestamp("updated_at").toInstant()))
        .optional();
  }

  private boolean insertDeadLetter(DeadLetterEvent deadLetter, boolean useInbox) {
    String original =
        useInbox
            ? jdbc.sql("SELECT raw_message::text FROM processing_record WHERE event_id=:id")
                .param("id", deadLetter.eventId())
                .query(String.class)
                .single()
            : write(deadLetter.originalMessage());
    return jdbc.sql(
                """
                        INSERT INTO processing_dead_letter
                            (id, event_id, stage, failure_code, failure_message, attempt,
                             original_message, status, failed_at)
                        VALUES (:id, :eventId, :stage, :code, :message, :attempt,
                                CAST(:original AS jsonb), 'OPEN', :failedAt)
                        ON CONFLICT (id) DO NOTHING
                        """)
            .param("id", deadLetter.deadLetterId())
            .param("eventId", deadLetter.eventId())
            .param("stage", deadLetter.stage())
            .param("code", deadLetter.failureCode())
            .param("message", truncate(deadLetter.failureMessage(), 2000))
            .param("attempt", deadLetter.attempt())
            .param("original", original)
            .param("failedAt", deadLetter.failedAt().atOffset(java.time.ZoneOffset.UTC))
            .update()
        == 1;
  }

  private void insertOutbox(UUID eventId, String topic, String payload) {
    jdbc.sql(
            """
                        INSERT INTO processing_outbox(id, aggregate_id, topic, message_key, payload, created_at)
                        VALUES (:id, :aggregateId, :topic, :key, CAST(:payload AS jsonb), now())
                        """)
        .param("id", UUID.randomUUID())
        .param("aggregateId", eventId)
        .param("topic", topic)
        .param("key", eventId.toString())
        .param("payload", payload)
        .update();
  }

  private String write(Object value) {
    try {
      return objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException exception) {
      throw new IllegalArgumentException("Message cannot be serialized", exception);
    }
  }

  private String truncate(String value, int size) {
    String safe = value == null ? "unknown error" : value;
    return safe.substring(0, Math.min(safe.length(), size));
  }
}
