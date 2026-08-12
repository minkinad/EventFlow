package io.github.minkin.eventflow.delivery.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.minkin.eventflow.contracts.DeadLetterEvent;
import io.github.minkin.eventflow.contracts.DeliveryCommand;
import io.github.minkin.eventflow.contracts.DeliveryTarget;
import io.github.minkin.eventflow.contracts.EventTopics;
import io.github.minkin.eventflow.contracts.TargetType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public class DeliveryJobRepository {
    private final JdbcClient jdbc;
    private final ObjectMapper objectMapper;

    public DeliveryJobRepository(JdbcClient jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public boolean accept(DeliveryCommand command, String rawMessage) {
        int inserted = jdbc.sql("""
                        INSERT INTO delivery_message(command_id, event_id, raw_message, received_at)
                        VALUES (:commandId, :eventId, CAST(:raw AS jsonb), now())
                        ON CONFLICT (command_id) DO NOTHING
                        """).param("commandId", command.commandId()).param("eventId", command.eventId())
                .param("raw", rawMessage).update();
        if (inserted == 0) {
            return false;
        }
        for (DeliveryTarget target : command.targets()) {
            jdbc.sql("""
                            INSERT INTO delivery_job
                                (id, command_id, event_id, target_type, destination, target_options,
                                 status, attempt, next_attempt_at, created_at, updated_at)
                            VALUES (:id, :commandId, :eventId, :type, :destination, CAST(:options AS jsonb),
                                    'PENDING', 0, now(), now(), now())
                            ON CONFLICT (command_id, target_type, destination) DO NOTHING
                            """).param("id", UUID.randomUUID()).param("commandId", command.commandId())
                    .param("eventId", command.eventId()).param("type", target.type().name())
                    .param("destination", target.destination()).param("options", write(target.options())).update();
        }
        return true;
    }

    @Transactional
    public List<DeliveryJob> claim(int limit, String owner, int leaseSeconds) {
        return jdbc.sql("""
                        WITH candidates AS (
                            SELECT j.id, m.raw_message
                            FROM delivery_job j
                            JOIN delivery_message m ON m.command_id = j.command_id
                            WHERE j.status IN ('PENDING', 'RETRY') AND j.next_attempt_at <= now()
                              AND (j.lease_until IS NULL OR j.lease_until < now())
                            ORDER BY j.next_attempt_at, j.created_at
                            FOR UPDATE OF j SKIP LOCKED LIMIT :limit
                        )
                        UPDATE delivery_job j
                        SET status='DELIVERING', lease_owner=:owner,
                            lease_until=now() + (:lease * interval '1 second'),
                            attempt=attempt+1, updated_at=now()
                        FROM candidates c WHERE j.id=c.id
                        RETURNING j.id, j.command_id, j.target_type, j.destination,
                                  j.target_options::text, j.attempt, c.raw_message::text
                        """).param("limit", limit).param("owner", owner).param("lease", leaseSeconds)
                .query((rs, rowNum) -> {
                    DeliveryCommand command = readCommand(rs.getString("raw_message"));
                    java.util.Map<String, String> options;
                    try {
                        options = objectMapper.readValue(rs.getString("target_options"),
                                objectMapper.getTypeFactory().constructMapType(
                                        java.util.Map.class, String.class, String.class));
                    } catch (JsonProcessingException exception) {
                        throw new IllegalStateException("Stored target options are invalid", exception);
                    }
                    var target = new DeliveryTarget(TargetType.valueOf(rs.getString("target_type")),
                            rs.getString("destination"), options);
                    return new DeliveryJob(rs.getObject("id", UUID.class), command, target, rs.getInt("attempt"));
                }).list();
    }

    public void succeeded(UUID jobId) {
        jdbc.sql("""
                        UPDATE delivery_job SET status='SUCCEEDED', delivered_at=now(),
                            lease_owner=NULL, lease_until=NULL, last_error=NULL, updated_at=now()
                        WHERE id=:id AND status='DELIVERING'
                        """).param("id", jobId).update();
    }

    public void retry(UUID jobId, Instant nextAttempt, String error) {
        jdbc.sql("""
                        UPDATE delivery_job SET status='RETRY', next_attempt_at=:nextAttempt,
                            lease_owner=NULL, lease_until=NULL, last_error=:error, updated_at=now()
                        WHERE id=:id AND status='DELIVERING'
                        """).param("id", jobId).param("nextAttempt", nextAttempt)
                .param("error", truncate(error)).update();
    }

    public List<DeliveryStatus> findStatuses(UUID eventId) {
        return jdbc.sql("""
                        SELECT id, event_id, target_type, destination, status, attempt,
                               next_attempt_at, delivered_at, last_error
                        FROM delivery_job WHERE event_id = :eventId ORDER BY created_at
                        """).param("eventId", eventId)
                .query((rs, rowNum) -> new DeliveryStatus(
                        rs.getObject("id", UUID.class), rs.getObject("event_id", UUID.class),
                        rs.getString("target_type"), rs.getString("destination"), rs.getString("status"),
                        rs.getInt("attempt"), rs.getTimestamp("next_attempt_at").toInstant(),
                        rs.getTimestamp("delivered_at") == null ? null : rs.getTimestamp("delivered_at").toInstant(),
                        rs.getString("last_error")))
                .list();
    }

    @Transactional
    public void dead(DeliveryJob job, String error) {
        jdbc.sql("""
                        UPDATE delivery_job SET status='DEAD', lease_owner=NULL, lease_until=NULL,
                            last_error=:error, updated_at=now() WHERE id=:id AND status='DELIVERING'
                        """).param("id", job.id()).param("error", truncate(error)).update();
        UUID deadLetterId = UUID.randomUUID();
        jdbc.sql("""
                        INSERT INTO delivery_dead_letter
                            (id, job_id, event_id, target_type, destination, failure_message,
                             original_message, status, failed_at)
                        VALUES (:id, :jobId, :eventId, :type, :destination, :error,
                                CAST(:message AS jsonb), 'OPEN', now())
                """).param("id", deadLetterId).param("jobId", job.id())
                .param("eventId", job.command().eventId()).param("type", job.target().type().name())
                .param("destination", job.target().destination()).param("error", truncate(error))
                .param("message", write(job.command())).update();
        var deadLetter = new DeadLetterEvent(1, deadLetterId, job.command().eventId(), "DELIVERY",
                "DELIVERY_ATTEMPTS_EXHAUSTED", truncate(error), job.attempt(),
                objectMapper.valueToTree(job.command()), Instant.now(), job.command().traceparent());
        insertOutbox(job.command().eventId(), EventTopics.DELIVERY_DLQ, write(deadLetter));
    }

    @Transactional
    public void replay(UUID deadLetterId) {
        UUID jobId = jdbc.sql("""
                        SELECT job_id FROM delivery_dead_letter
                        WHERE id=:id AND status='OPEN' FOR UPDATE
                        """).param("id", deadLetterId).query(UUID.class).optional()
                .orElseThrow(() -> new IllegalArgumentException("Open dead letter not found: " + deadLetterId));
        jdbc.sql("""
                        UPDATE delivery_job SET status='RETRY', attempt=0, next_attempt_at=now(),
                            last_error=NULL, updated_at=now() WHERE id=:jobId AND status='DEAD'
                        """).param("jobId", jobId).update();
        jdbc.sql("UPDATE delivery_dead_letter SET status='REPLAYED', replayed_at=now() WHERE id=:id")
                .param("id", deadLetterId).update();
    }

    @Transactional
    public void malformed(String raw, String error) {
        JsonNode value = objectMapper.createObjectNode().put("raw", raw);
        UUID deadLetterId = UUID.randomUUID();
        UUID eventId = UUID.nameUUIDFromBytes(raw.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        jdbc.sql("""
                        INSERT INTO delivery_dead_letter
                            (id, event_id, target_type, destination, failure_message,
                             original_message, status, failed_at)
                        VALUES (:id, :eventId, 'UNKNOWN', 'unknown', :error,
                                CAST(:message AS jsonb), 'OPEN', now())
                        """).param("id", deadLetterId)
                .param("eventId", eventId)
                .param("error", truncate(error)).param("message", write(value)).update();
        var deadLetter = new DeadLetterEvent(1, deadLetterId, eventId, "DELIVERY_DESERIALIZATION",
                "MALFORMED_DELIVERY_COMMAND", truncate(error), 1, value, Instant.now(), null);
        insertOutbox(eventId, EventTopics.DELIVERY_DLQ, write(deadLetter));
    }

    private void insertOutbox(UUID eventId, String topic, String payload) {
        jdbc.sql("""
                        INSERT INTO delivery_outbox(id, aggregate_id, topic, message_key, payload, created_at)
                        VALUES (:id, :eventId, :topic, :key, CAST(:payload AS jsonb), now())
                        """).param("id", UUID.randomUUID()).param("eventId", eventId).param("topic", topic)
                .param("key", eventId.toString()).param("payload", payload).update();
    }

    private DeliveryCommand readCommand(String raw) {
        try {
            return objectMapper.readValue(raw, DeliveryCommand.class);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Stored delivery command is invalid", exception);
        }
    }

    private String write(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Value cannot be serialized", exception);
        }
    }

    private String truncate(String value) {
        String safe = value == null ? "unknown delivery failure" : value;
        return safe.substring(0, Math.min(safe.length(), 2000));
    }
}
