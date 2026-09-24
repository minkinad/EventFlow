package io.github.minkin.eventflow.processing.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class PipelineRepository {
  private final JdbcClient jdbc;
  private final ObjectMapper objectMapper;

  public PipelineRepository(JdbcClient jdbc, ObjectMapper objectMapper) {
    this.jdbc = jdbc;
    this.objectMapper = objectMapper;
  }

  public Optional<PipelineDefinition> findActive(String tenant, String eventType) {
    return jdbc.sql(
            """
                        SELECT definition::text FROM pipeline_definition
                        WHERE tenant_id=:tenant AND event_type = :eventType AND enabled = true
                        ORDER BY version DESC LIMIT 1
                        """)
        .param("tenant", tenant)
        .param("eventType", eventType)
        .query(String.class)
        .optional()
        .map(value -> read(value, PipelineDefinition.class));
  }

  public Optional<JsonNode> findSchema(String tenant, String name) {
    return jdbc.sql(
            "SELECT definition::text FROM schema_definition WHERE tenant_id=:tenant AND name = :name AND active = true")
        .param("tenant", tenant)
        .param("name", name)
        .query(String.class)
        .optional()
        .map(value -> read(value, JsonNode.class));
  }

  public Optional<PipelineDefinition> findById(String tenant, UUID id) {
    return jdbc.sql(
            "SELECT definition::text FROM pipeline_definition WHERE tenant_id=:tenant AND id = :id")
        .param("tenant", tenant)
        .param("id", id)
        .query(String.class)
        .optional()
        .map(value -> read(value, PipelineDefinition.class));
  }

  public Optional<PipelineSummary> findSummary(String tenant, UUID id) {
    return jdbc.sql(
            """
                        SELECT id, name, version, event_type, enabled, state, revision, created_at
                        FROM pipeline_definition WHERE tenant_id=:tenant AND id = :id
                        """)
        .param("tenant", tenant)
        .param("id", id)
        .query(
            (rs, rowNum) ->
                new PipelineSummary(
                    rs.getObject("id", UUID.class),
                    rs.getString("name"),
                    rs.getInt("version"),
                    rs.getString("event_type"),
                    rs.getBoolean("enabled"),
                    rs.getString("state"),
                    rs.getLong("revision"),
                    rs.getTimestamp("created_at").toInstant()))
        .optional();
  }

  @Transactional
  public void saveSchema(String tenant, String name, JsonNode definition) {
    String serialized = write(definition);
    int inserted =
        jdbc.sql(
                """
                        INSERT INTO schema_definition(tenant_id, name, definition, active, updated_at)
                        VALUES (:tenant, :name, CAST(:definition AS jsonb), true, now())
                        ON CONFLICT (tenant_id, name) DO NOTHING
                        """)
            .param("tenant", tenant)
            .param("name", name)
            .param("definition", serialized)
            .update();
    if (inserted == 0) {
      boolean identical =
          jdbc.sql(
                  """
                            SELECT definition = CAST(:definition AS jsonb)
                            FROM schema_definition WHERE tenant_id=:tenant AND name = :name
                            """)
              .param("tenant", tenant)
              .param("name", name)
              .param("definition", serialized)
              .query(Boolean.class)
              .single();
      if (!identical) {
        throw new IllegalArgumentException(
            "Schema names are immutable; publish a new version instead of overwriting " + name);
      }
    }
  }

  @Transactional
  public UUID saveDraft(String tenant, PipelineDefinition definition) {
    UUID id = UUID.randomUUID();
    jdbc.sql(
            """
                        INSERT INTO pipeline_definition(tenant_id, id, name, version, event_type, definition, enabled, created_at)
                        VALUES (:tenant, :id, :name, :version, :eventType, CAST(:definition AS jsonb), false, now())
                        """)
        .param("tenant", tenant)
        .param("id", id)
        .param("name", definition.name())
        .param("version", definition.version())
        .param("eventType", definition.eventType())
        .param("definition", write(definition))
        .update();
    audit(tenant, id, "PIPELINE_CREATED", null, summary(tenant, id), "Draft created");
    return id;
  }

  public PipelineSummary summary(String tenant, UUID id) {
    return findSummary(tenant, id)
        .orElseThrow(
            () ->
                new org.springframework.web.server.ResponseStatusException(
                    org.springframework.http.HttpStatus.NOT_FOUND, "Pipeline not found"));
  }

  @Transactional
  public PipelineSummary editDraft(
      String tenant, UUID id, long revision, PipelineDefinition definition) {
    PipelineSummary before = summary(tenant, id);
    int changed =
        jdbc.sql(
                """
        UPDATE pipeline_definition SET definition=CAST(:definition AS jsonb), state='DRAFT', revision=revision+1, updated_at=now()
        WHERE tenant_id=:tenant AND id=:id AND revision=:revision AND state IN ('DRAFT','VALIDATED')
            AND name=:name AND version=:version AND event_type=:eventType
        """)
            .param("tenant", tenant)
            .param("id", id)
            .param("revision", revision)
            .param("definition", write(definition))
            .param("name", definition.name())
            .param("version", definition.version())
            .param("eventType", definition.eventType())
            .update();
    requireChange(changed);
    var after = summary(tenant, id);
    audit(tenant, id, "PIPELINE_EDITED", before, after, "Draft edited");
    return after;
  }

  @Transactional
  public PipelineSummary validated(String tenant, UUID id, long revision) {
    var before = summary(tenant, id);
    int changed =
        jdbc.sql(
                """
        UPDATE pipeline_definition SET state='VALIDATED', revision=revision+1, updated_at=now()
        WHERE tenant_id=:tenant AND id=:id AND revision=:revision AND state IN ('DRAFT','VALIDATED')
        """)
            .param("tenant", tenant)
            .param("id", id)
            .param("revision", revision)
            .update();
    requireChange(changed);
    var after = summary(tenant, id);
    audit(tenant, id, "PIPELINE_VALIDATED", before, after, "Definition validated");
    return after;
  }

  @Transactional
  public PipelineSummary activate(
      String tenant, UUID id, long revision, boolean rollback, String reason) {
    var initial = summary(tenant, id);
    // All activations in the same event-type scope take this lock before changing either version.
    jdbc.sql("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))")
        .param("scope", tenant + ":" + initial.eventType())
        .query()
        .singleRow();
    var before = summary(tenant, id);
    String required = rollback ? "SUPERSEDED" : "VALIDATED";
    if (before.revision() != revision || !required.equals(before.state())) {
      throw new PipelineConflictException();
    }
    jdbc.sql(
            """
        UPDATE pipeline_definition SET enabled=false, state='SUPERSEDED', revision=revision+1, updated_at=now()
        WHERE tenant_id=:tenant AND event_type=:scope AND enabled=true
        """)
        .param("tenant", tenant)
        .param("scope", before.eventType())
        .update();
    int changed =
        jdbc.sql(
                """
        UPDATE pipeline_definition SET enabled=true, state='ACTIVE', revision=revision+1, updated_at=now()
        WHERE tenant_id=:tenant AND id=:id AND revision=:revision AND state=:state
        """)
            .param("tenant", tenant)
            .param("id", id)
            .param("revision", revision)
            .param("state", required)
            .update();
    requireChange(changed);
    var after = summary(tenant, id);
    audit(tenant, id, rollback ? "PIPELINE_ROLLBACK" : "PIPELINE_ACTIVATED", before, after, reason);
    return after;
  }

  @Transactional
  public PipelineSummary disable(String tenant, UUID id, long revision, String reason) {
    var initial = summary(tenant, id);
    jdbc.sql("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))")
        .param("scope", tenant + ":" + initial.eventType())
        .query()
        .singleRow();
    var before = summary(tenant, id);
    int changed =
        jdbc.sql(
                """
        UPDATE pipeline_definition SET enabled=false, state='DISABLED', revision=revision+1, updated_at=now()
        WHERE tenant_id=:tenant AND id=:id AND revision=:revision AND state <> 'DISABLED'
        """)
            .param("tenant", tenant)
            .param("id", id)
            .param("revision", revision)
            .update();
    requireChange(changed);
    var after = summary(tenant, id);
    audit(tenant, id, "PIPELINE_DISABLED", before, after, reason);
    return after;
  }

  private void requireChange(int changed) {
    if (changed != 1) {
      throw new PipelineConflictException();
    }
  }

  private void audit(
      String tenant,
      UUID id,
      String action,
      PipelineSummary before,
      PipelineSummary after,
      String reason) {
    if (reason == null || reason.isBlank() || reason.length() > 1000) {
      throw new IllegalArgumentException("An operator reason of 1-1000 characters is required");
    }
    // No payload, configuration options, credentials or raw schema are copied into audit snapshots.
    jdbc.sql(
            """
        INSERT INTO audit_log(id, tenant_id, actor, action, entity_type, entity_id, before_state, after_state, reason, trace_id)
        VALUES (:id, :tenant, :actor, :action, 'PIPELINE', :entity, CAST(:before AS jsonb), CAST(:after AS jsonb), :reason, :trace)
        """)
        .param("tenant", tenant)
        .param("actor", io.github.minkin.eventflow.security.Caller.auditActor())
        .param("id", UUID.randomUUID())
        .param("action", action)
        .param("entity", id.toString())
        .param("before", write(before))
        .param("after", write(after))
        .param("reason", reason)
        .param("trace", org.slf4j.MDC.get("traceId"))
        .update();
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
