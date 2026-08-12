package io.github.minkin.eventflow.processing.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Repository
public class PipelineRepository {
    private final JdbcClient jdbc;
    private final ObjectMapper objectMapper;

    public PipelineRepository(JdbcClient jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    public Optional<PipelineDefinition> findActive(String eventType) {
        return jdbc.sql("""
                        SELECT definition::text FROM pipeline_definition
                        WHERE event_type = :eventType AND enabled = true
                        ORDER BY version DESC LIMIT 1
                        """)
                .param("eventType", eventType)
                .query(String.class)
                .optional()
                .map(value -> read(value, PipelineDefinition.class));
    }

    public Optional<JsonNode> findSchema(String name) {
        return jdbc.sql("SELECT definition::text FROM schema_definition WHERE name = :name AND active = true")
                .param("name", name)
                .query(String.class)
                .optional()
                .map(value -> read(value, JsonNode.class));
    }

    public Optional<PipelineDefinition> findById(UUID id) {
        return jdbc.sql("SELECT definition::text FROM pipeline_definition WHERE id = :id")
                .param("id", id).query(String.class).optional()
                .map(value -> read(value, PipelineDefinition.class));
    }

    public Optional<PipelineSummary> findSummary(UUID id) {
        return jdbc.sql("""
                        SELECT id, name, version, event_type, enabled, created_at
                        FROM pipeline_definition WHERE id = :id
                        """).param("id", id)
                .query((rs, rowNum) -> new PipelineSummary(
                        rs.getObject("id", UUID.class), rs.getString("name"), rs.getInt("version"),
                        rs.getString("event_type"), rs.getBoolean("enabled"),
                        rs.getTimestamp("created_at").toInstant()))
                .optional();
    }

    @Transactional
    public void saveSchema(String name, JsonNode definition) {
        String serialized = write(definition);
        int inserted = jdbc.sql("""
                        INSERT INTO schema_definition(name, definition, active, updated_at)
                        VALUES (:name, CAST(:definition AS jsonb), true, now())
                        ON CONFLICT (name) DO NOTHING
                        """)
                .param("name", name)
                .param("definition", serialized)
                .update();
        if (inserted == 0) {
            boolean identical = jdbc.sql("""
                            SELECT definition = CAST(:definition AS jsonb)
                            FROM schema_definition WHERE name = :name
                            """).param("name", name).param("definition", serialized)
                    .query(Boolean.class).single();
            if (!identical) {
                throw new IllegalArgumentException(
                        "Schema names are immutable; publish a new version instead of overwriting " + name);
            }
        }
    }

    @Transactional
    public UUID saveDraft(PipelineDefinition definition) {
        UUID id = UUID.randomUUID();
        jdbc.sql("""
                        INSERT INTO pipeline_definition(id, name, version, event_type, definition, enabled, created_at)
                        VALUES (:id, :name, :version, :eventType, CAST(:definition AS jsonb), false, now())
                        """)
                .param("id", id).param("name", definition.name()).param("version", definition.version())
                .param("eventType", definition.eventType()).param("definition", write(definition)).update();
        return id;
    }

    @Transactional
    public void activate(UUID id) {
        PipelineSummary pipeline = findSummary(id)
                .orElseThrow(() -> new IllegalArgumentException("Pipeline not found: " + id));
        jdbc.sql("UPDATE pipeline_definition SET enabled = false WHERE event_type = :eventType AND enabled = true")
                .param("eventType", pipeline.eventType()).update();
        int changed = jdbc.sql("UPDATE pipeline_definition SET enabled = true WHERE id = :id")
                .param("id", id).update();
        if (changed != 1) {
            throw new IllegalStateException("Pipeline disappeared during activation: " + id);
        }
    }

    private <T> T read(String value, Class<T> type) {
        try {
            return objectMapper.readValue(value, type);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Stored pipeline configuration is invalid", exception);
        }
    }

    private String write(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Configuration is not valid JSON", exception);
        }
    }
}
