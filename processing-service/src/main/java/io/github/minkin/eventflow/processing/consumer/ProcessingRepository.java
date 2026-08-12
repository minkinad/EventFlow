package io.github.minkin.eventflow.processing.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.minkin.eventflow.contracts.DeadLetterEvent;
import io.github.minkin.eventflow.contracts.DeliveryCommand;
import io.github.minkin.eventflow.contracts.EventTopics;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

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
        int inserted = jdbc.sql("""
                        INSERT INTO processing_record
                            (event_id, content_hash, raw_message, status, attempt, lease_until, first_seen_at, updated_at)
                        VALUES (:eventId, :hash, CAST(:raw AS jsonb), 'PROCESSING', 1,
                                now() + (:lease * interval '1 second'), now(), now())
                        ON CONFLICT (event_id) DO NOTHING
                        """)
                .param("eventId", eventId).param("hash", contentHash).param("raw", rawMessage)
                .param("lease", leaseSeconds).update();
        if (inserted == 1) {
            return new ClaimResult(ClaimDecision.CLAIMED, 1);
        }

        Map<String, Object> current = jdbc.sql("""
                        SELECT content_hash, status, lease_until, attempt
                        FROM processing_record WHERE event_id = :eventId
                        """)
                .param("eventId", eventId).query().singleRow();
        if (!contentHash.equals(current.get("content_hash"))) {
            return new ClaimResult(ClaimDecision.CONTENT_CONFLICT, ((Number) current.get("attempt")).intValue());
        }
        if ("SUCCEEDED".equals(current.get("status"))) {
            return new ClaimResult(ClaimDecision.DUPLICATE, ((Number) current.get("attempt")).intValue());
        }

        Optional<Integer> reclaimed = jdbc.sql("""
                        UPDATE processing_record
                        SET status = 'PROCESSING', attempt = attempt + 1,
                            lease_until = now() + (:lease * interval '1 second'), updated_at = now()
                        WHERE event_id = :eventId
                          AND (status IN ('RETRYABLE', 'REPLAY_REQUESTED') OR lease_until < now())
                        RETURNING attempt
                        """)
                .param("lease", leaseSeconds).param("eventId", eventId)
                .query(Integer.class).optional();
        return reclaimed
                .map(attempt -> new ClaimResult(ClaimDecision.CLAIMED, attempt))
                .orElseGet(() -> new ClaimResult(
                        ClaimDecision.BUSY, ((Number) current.get("attempt")).intValue()));
    }

    public void releaseForRetry(UUID eventId, String code, String message) {
        int changed = jdbc.sql("""
                        UPDATE processing_record
                        SET status = 'RETRYABLE', failure_code = :code, failure_message = :message,
                            lease_until = NULL, updated_at = now()
                        WHERE event_id = :eventId AND status = 'PROCESSING'
                        """).param("eventId", eventId).param("code", code)
                .param("message", truncate(message, 2000)).update();
        if (changed != 1) {
            throw new IllegalStateException("Processing lease was lost for event " + eventId);
        }
    }

    @Transactional
    public void complete(UUID eventId, String pipeline, int pipelineVersion, DeliveryCommand command) {
        int changed = jdbc.sql("""
                        UPDATE processing_record
                        SET status = 'SUCCEEDED', pipeline_name = :pipeline, pipeline_version = :version,
                            lease_until = NULL, updated_at = now(), failure_code = NULL, failure_message = NULL
                        WHERE event_id = :eventId AND status = 'PROCESSING'
                        """)
                .param("pipeline", pipeline).param("version", pipelineVersion).param("eventId", eventId).update();
        if (changed != 1) {
            throw new IllegalStateException("Processing lease was lost for event " + eventId);
        }
        insertOutbox(eventId, EventTopics.DELIVERY_COMMANDS, write(command));
        jdbc.sql("""
                        UPDATE processing_dead_letter SET status = 'RESOLVED'
                        WHERE event_id = :eventId AND status = 'REPLAY_REQUESTED'
                        """).param("eventId", eventId).update();
    }

    @Transactional
    public UUID fail(UUID eventId, String code, String message, DeadLetterEvent deadLetter) {
        jdbc.sql("""
                        UPDATE processing_dead_letter SET status = 'REPLAY_FAILED'
                        WHERE event_id = :eventId AND status = 'REPLAY_REQUESTED'
                        """).param("eventId", eventId).update();
        int changed = jdbc.sql("""
                        UPDATE processing_record
                        SET status = 'FAILED', failure_code = :code, failure_message = :message,
                            lease_until = NULL, updated_at = now()
                        WHERE event_id = :eventId AND status = 'PROCESSING'
                        """)
                .param("code", code).param("message", truncate(message, 2000)).param("eventId", eventId).update();
        if (changed != 1) {
            throw new IllegalStateException("Processing lease was lost for event " + eventId);
        }
        insertDeadLetter(deadLetter);
        insertOutbox(eventId, EventTopics.PROCESSING_DLQ, write(deadLetter));
        return deadLetter.deadLetterId();
    }

    @Transactional
    public UUID reject(DeadLetterEvent deadLetter) {
        insertDeadLetter(deadLetter);
        insertOutbox(deadLetter.eventId(), EventTopics.PROCESSING_DLQ, write(deadLetter));
        return deadLetter.deadLetterId();
    }

    @Transactional
    public void replay(UUID deadLetterId) {
        Map<String, Object> row = jdbc.sql("""
                        SELECT event_id, stage, original_message::text AS original_message
                        FROM processing_dead_letter
                        WHERE id = :id AND status IN ('OPEN', 'REPLAY_FAILED')
                        FOR UPDATE
                        """)
                .param("id", deadLetterId)
                .query((rs, rowNum) -> Map.<String, Object>of(
                        "event_id", rs.getObject("event_id", UUID.class),
                        "stage", rs.getString("stage"),
                        "original_message", rs.getString("original_message")))
                .optional()
                .orElseThrow(() -> new IllegalArgumentException("Replayable dead letter not found: " + deadLetterId));
        UUID eventId = (UUID) row.get("event_id");
        if (!"PROCESSING".equals(row.get("stage"))) {
            throw new IllegalArgumentException(
                    "Only processing-stage dead letters can be replayed without supplying corrected content");
        }
        jdbc.sql("""
                        UPDATE processing_record SET status = 'REPLAY_REQUESTED', updated_at = now()
                        WHERE event_id = :eventId AND status = 'FAILED'
                        """).param("eventId", eventId).update();
        jdbc.sql("""
                        UPDATE processing_dead_letter SET status = 'REPLAY_REQUESTED', replayed_at = now()
                        WHERE id = :id
                        """).param("id", deadLetterId).update();
        insertOutbox(eventId, EventTopics.RAW_EVENTS, (String) row.get("original_message"));
    }

    public Optional<String> findDeadLetterStatus(UUID id) {
        return jdbc.sql("SELECT status FROM processing_dead_letter WHERE id = :id")
                .param("id", id).query(String.class).optional();
    }

    public Optional<ProcessingStatus> findStatus(UUID eventId) {
        return jdbc.sql("""
                        SELECT event_id, status, attempt, pipeline_name, pipeline_version,
                               failure_code, failure_message, first_seen_at, updated_at
                        FROM processing_record WHERE event_id = :eventId
                        """).param("eventId", eventId)
                .query((rs, rowNum) -> new ProcessingStatus(
                        rs.getObject("event_id", UUID.class), rs.getString("status"), rs.getInt("attempt"),
                        rs.getString("pipeline_name"), (Integer) rs.getObject("pipeline_version"),
                        rs.getString("failure_code"), rs.getString("failure_message"),
                        rs.getTimestamp("first_seen_at").toInstant(), rs.getTimestamp("updated_at").toInstant()))
                .optional();
    }

    private void insertDeadLetter(DeadLetterEvent deadLetter) {
        jdbc.sql("""
                        INSERT INTO processing_dead_letter
                            (id, event_id, stage, failure_code, failure_message, attempt,
                             original_message, status, failed_at)
                        VALUES (:id, :eventId, :stage, :code, :message, :attempt,
                                CAST(:original AS jsonb), 'OPEN', :failedAt)
                        """)
                .param("id", deadLetter.deadLetterId()).param("eventId", deadLetter.eventId())
                .param("stage", deadLetter.stage()).param("code", deadLetter.failureCode())
                .param("message", truncate(deadLetter.failureMessage(), 2000)).param("attempt", deadLetter.attempt())
                .param("original", write(deadLetter.originalMessage())).param("failedAt", deadLetter.failedAt()).update();
    }

    private void insertOutbox(UUID eventId, String topic, String payload) {
        jdbc.sql("""
                        INSERT INTO processing_outbox(id, aggregate_id, topic, message_key, payload, created_at)
                        VALUES (:id, :aggregateId, :topic, :key, CAST(:payload AS jsonb), now())
                        """)
                .param("id", UUID.randomUUID()).param("aggregateId", eventId).param("topic", topic)
                .param("key", eventId.toString()).param("payload", payload).update();
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
