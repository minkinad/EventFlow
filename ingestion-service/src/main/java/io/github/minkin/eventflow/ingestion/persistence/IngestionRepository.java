package io.github.minkin.eventflow.ingestion.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.minkin.eventflow.ingestion.api.EventSubmission;
import io.github.minkin.eventflow.ingestion.api.IngestionStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Repository
public class IngestionRepository {
    private final JdbcClient jdbc;
    private final ObjectMapper objectMapper;

    public IngestionRepository(JdbcClient jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    public boolean insertEvent(EventSubmission event, Instant receivedAt, String contentHash) {
        return jdbc.sql("""
                        INSERT INTO ingested_event
                            (event_id, event_type, source, schema_version, occurred_at, received_at,
                             payload, metadata, content_hash, status)
                        VALUES (:eventId, :eventType, :source, :schemaVersion, :occurredAt, :receivedAt,
                                CAST(:payload AS jsonb), CAST(:metadata AS jsonb), :contentHash, 'ACCEPTED')
                        ON CONFLICT (event_id) DO NOTHING
                        """)
                .param("eventId", event.eventId())
                .param("eventType", event.eventType())
                .param("source", event.source())
                .param("schemaVersion", event.schemaVersion())
                .param("occurredAt", event.occurredAt())
                .param("receivedAt", receivedAt)
                .param("payload", json(event.payload()))
                .param("metadata", json(event.metadata()))
                .param("contentHash", contentHash)
                .update() == 1;
    }

    public Optional<String> findContentHash(UUID eventId) {
        return jdbc.sql("SELECT content_hash FROM ingested_event WHERE event_id = :eventId")
                .param("eventId", eventId)
                .query(String.class)
                .optional();
    }

    public Optional<IngestionStatus> findStatus(UUID eventId) {
        return jdbc.sql("""
                        SELECT e.event_id, e.event_type, e.source, e.status, e.received_at,
                               o.published_at, o.attempts, o.last_error
                        FROM ingested_event e
                        LEFT JOIN ingestion_outbox o ON o.aggregate_id = e.event_id
                        WHERE e.event_id = :eventId
                        """).param("eventId", eventId)
                .query((rs, rowNum) -> new IngestionStatus(
                        rs.getObject("event_id", UUID.class), rs.getString("event_type"), rs.getString("source"),
                        rs.getString("status"), rs.getTimestamp("received_at").toInstant(),
                        rs.getTimestamp("published_at") == null ? null : rs.getTimestamp("published_at").toInstant(),
                        rs.getInt("attempts"), rs.getString("last_error")))
                .optional();
    }

    public void insertOutbox(UUID eventId, String topic, String payload, Instant createdAt) {
        jdbc.sql("""
                        INSERT INTO ingestion_outbox
                            (id, aggregate_id, topic, message_key, payload, created_at)
                        VALUES (:id, :aggregateId, :topic, :messageKey, CAST(:payload AS jsonb), :createdAt)
                        """)
                .param("id", UUID.randomUUID())
                .param("aggregateId", eventId)
                .param("topic", topic)
                .param("messageKey", eventId.toString())
                .param("payload", payload)
                .param("createdAt", createdAt)
                .update();
    }

    private String json(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Value cannot be serialized as JSON", exception);
        }
    }
}
