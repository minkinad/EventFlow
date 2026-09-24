package io.github.minkin.eventflow.processing.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.minkin.eventflow.processing.PostgresIntegrationSupport;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PipelineRepositoryIT extends PostgresIntegrationSupport {
  private PipelineRepository repository;

  @BeforeEach
  void setup() {
    repository = transactional(new PipelineRepository(jdbc, mapper));
  }

  private PipelineDefinition version(int n) {
    return new PipelineDefinition(
        "orders",
        n,
        "order.created",
        List.of(new PipelineStepDefinition("route", null, null, "POSTGRES", "events", Map.of())));
  }

  @Test
  void immutableSchemaCanBeRepublishedIdenticallyButNotOverwritten() throws Exception {
    var schema = mapper.readTree("{\"type\":\"string\"}");
    repository.saveSchema("demo", "test-v1", schema);
    repository.saveSchema("demo", "test-v1", schema);
    assertThat(repository.findSchema("demo", "test-v1")).contains(schema);
    assertThatThrownBy(
            () ->
                repository.saveSchema(
                    "demo", "test-v1", mapper.createObjectNode().put("type", "number")))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(repository.findSchema("demo", "test-v1")).contains(schema);
  }

  @Test
  void draftIsInactiveUntilAtomicActivation() {
    var definition = version(2);
    var id = repository.saveDraft("demo", definition);
    assertThat(repository.findSummary("demo", id).orElseThrow().active()).isFalse();
    assertThat(repository.findById("demo", id)).contains(definition);
    assertThat(repository.findActive("demo", "order.created").orElseThrow().version()).isEqualTo(1);
    repository.validated("demo", id, 0);
    repository.activate("demo", id, 1, false, "test activation");
    assertThat(repository.findActive("demo", "order.created")).contains(definition);
    assertThat(
            jdbc.sql("SELECT count(*) FROM pipeline_definition WHERE enabled")
                .query(Long.class)
                .single())
        .isEqualTo(1);
  }

  @Test
  void activationFailureRestoresPreviousActiveVersion() {
    var id = repository.saveDraft("demo", version(2));
    jdbc.sql(
            "ALTER TABLE pipeline_definition ADD CONSTRAINT injected_failure CHECK (NOT enabled OR version=1)")
        .update();
    repository.validated("demo", id, 0);
    assertThatThrownBy(() -> repository.activate("demo", id, 1, false, "test failure"))
        .isInstanceOf(RuntimeException.class);
    assertThat(repository.findActive("demo", "order.created").orElseThrow().version()).isEqualTo(1);
  }

  @Test
  void staleRevisionAndEditingActiveVersionAreRejected() {
    var id = repository.saveDraft("demo", version(2));
    var edited = repository.editDraft("demo", id, 0, version(2));
    assertThat(edited.revision()).isEqualTo(1);
    assertThatThrownBy(() -> repository.validated("demo", id, 0))
        .isInstanceOf(PipelineConflictException.class);
    repository.validated("demo", id, 1);
    repository.activate("demo", id, 2, false, "Activate reviewed draft");
    assertThatThrownBy(() -> repository.editDraft("demo", id, 3, version(2)))
        .isInstanceOf(PipelineConflictException.class);
  }

  @Test
  void rollbackIsAtomicAndAuditedAndHistoryCannotBeChanged() {
    var seed = java.util.UUID.fromString("10000000-0000-0000-0000-000000000001");
    var id = repository.saveDraft("demo", version(2));
    repository.validated("demo", id, 0);
    repository.activate("demo", id, 1, false, "Roll forward");
    assertThat(repository.summary("demo", seed).state()).isEqualTo("SUPERSEDED");
    repository.activate("demo", seed, 1, true, "Revert downstream incompatibility");
    assertThat(repository.findActive("demo", "order.created").orElseThrow().version()).isEqualTo(1);
    assertThat(repository.summary("demo", id).state()).isEqualTo("SUPERSEDED");
    assertThat(
            jdbc.sql("SELECT count(*) FROM audit_log WHERE action='PIPELINE_ROLLBACK'")
                .query(Long.class)
                .single())
        .isEqualTo(1);
    assertThatThrownBy(() -> jdbc.sql("DELETE FROM audit_log").update())
        .isInstanceOf(RuntimeException.class);
    assertThatThrownBy(() -> jdbc.sql("UPDATE audit_log SET reason='changed'").update())
        .isInstanceOf(RuntimeException.class);
    repository.disable("demo", seed, 2, "Pause this pipeline");
    assertThat(repository.findActive("demo", "order.created")).isEmpty();
  }

  @Test
  void concurrentActivationOfSameRevisionHasOneWinner() throws Exception {
    var id = repository.saveDraft("demo", version(2));
    repository.validated("demo", id, 0);
    try (var executor = java.util.concurrent.Executors.newFixedThreadPool(2)) {
      java.util.concurrent.Callable<Boolean> activate =
          () -> {
            try {
              repository.activate("demo", id, 1, false, "Concurrent activation");
              return true;
            } catch (PipelineConflictException conflict) {
              return false;
            }
          };
      var first = executor.submit(activate);
      var second = executor.submit(activate);
      assertThat(first.get()).isNotEqualTo(second.get());
    }
    assertThat(
            jdbc.sql("SELECT count(*) FROM pipeline_definition WHERE enabled")
                .query(Long.class)
                .single())
        .isEqualTo(1);
  }

  @Test
  void dryRunNeverCreatesOutboxOrCallsEnrichment() {
    var gateway = org.mockito.Mockito.mock(EnrichmentGateway.class);
    var engine = new PipelineEngine(repository, gateway);
    var pipeline =
        new PipelineDefinition(
            "dry-run",
            1,
            "test.event",
            java.util.List.of(
                new PipelineStepDefinition("enrich", null, "customers", null, null, null),
                new PipelineStepDefinition("route", null, null, "POSTGRES", "events", null)));
    var result = engine.dryRun("demo", pipeline, mapper.createObjectNode());
    assertThat(result.valid()).isFalse();
    assertThat(result.warnings()).hasSize(1);
    assertThat(result.executedSteps()).hasSize(2);
    assertThat(result.selectedRoutes()).hasSize(1);
    assertThat(result.durationNanos()).isPositive();
    org.mockito.Mockito.verifyNoInteractions(gateway);
    assertThat(count("processing_outbox")).isZero();
    assertThat(count("audit_log")).isZero();
  }

  @Test
  void schemasNamesAndActivationAreIsolatedByTenant() throws Exception {
    var a = mapper.readTree("{\"type\":\"string\"}");
    var b = mapper.readTree("{\"type\":\"number\"}");
    repository.saveSchema("demo", "same-name", a);
    repository.saveSchema("other", "same-name", b);
    assertThat(repository.findSchema("demo", "same-name")).contains(a);
    assertThat(repository.findSchema("other", "same-name")).contains(b);
    assertThat(repository.findSchema("other", "order-v1")).isEmpty();
    var id = repository.saveDraft("other", version(1));
    repository.validated("other", id, 0);
    repository.activate("other", id, 1, false, "Other tenant activation");
    assertThat(repository.findActive("demo", "order.created")).isPresent();
    assertThat(repository.findActive("other", "order.created")).isPresent();
    assertThat(count("pipeline_definition")).isEqualTo(2);
    assertThat(repository.findSummary("demo", id)).isEmpty();
    assertThatThrownBy(() -> repository.disable("demo", id, 2, "Cross tenant"))
        .isInstanceOf(org.springframework.web.server.ResponseStatusException.class);
    assertThatThrownBy(() -> repository.editDraft("demo", id, 2, version(1)))
        .isInstanceOf(org.springframework.web.server.ResponseStatusException.class);
    assertThat(repository.summary("other", id).state()).isEqualTo("ACTIVE");
  }
}
