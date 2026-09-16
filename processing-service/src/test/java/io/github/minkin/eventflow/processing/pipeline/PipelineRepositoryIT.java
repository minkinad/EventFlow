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
    repository.saveSchema("test-v1", schema);
    repository.saveSchema("test-v1", schema);
    assertThat(repository.findSchema("test-v1")).contains(schema);
    assertThatThrownBy(
            () -> repository.saveSchema("test-v1", mapper.createObjectNode().put("type", "number")))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(repository.findSchema("test-v1")).contains(schema);
  }

  @Test
  void draftIsInactiveUntilAtomicActivation() {
    var definition = version(2);
    var id = repository.saveDraft(definition);
    assertThat(repository.findSummary(id).orElseThrow().active()).isFalse();
    assertThat(repository.findById(id)).contains(definition);
    assertThat(repository.findActive("order.created").orElseThrow().version()).isEqualTo(1);
    repository.validated(id, 0);
    repository.activate(id, 1, false, "test activation");
    assertThat(repository.findActive("order.created")).contains(definition);
    assertThat(
            jdbc.sql("SELECT count(*) FROM pipeline_definition WHERE enabled")
                .query(Long.class)
                .single())
        .isEqualTo(1);
  }

  @Test
  void activationFailureRestoresPreviousActiveVersion() {
    var id = repository.saveDraft(version(2));
    jdbc.sql(
            "ALTER TABLE pipeline_definition ADD CONSTRAINT injected_failure CHECK (NOT enabled OR version=1)")
        .update();
    repository.validated(id, 0);
    assertThatThrownBy(() -> repository.activate(id, 1, false, "test failure"))
        .isInstanceOf(RuntimeException.class);
    assertThat(repository.findActive("order.created").orElseThrow().version()).isEqualTo(1);
  }

  @Test
  void staleRevisionAndEditingActiveVersionAreRejected() {
    var id = repository.saveDraft(version(2));
    var edited = repository.editDraft(id, 0, version(2));
    assertThat(edited.revision()).isEqualTo(1);
    assertThatThrownBy(() -> repository.validated(id, 0))
        .isInstanceOf(PipelineConflictException.class);
    repository.validated(id, 1);
    repository.activate(id, 2, false, "Activate reviewed draft");
    assertThatThrownBy(() -> repository.editDraft(id, 3, version(2)))
        .isInstanceOf(PipelineConflictException.class);
  }

  @Test
  void rollbackIsAtomicAndAuditedAndHistoryCannotBeChanged() {
    var seed = java.util.UUID.fromString("10000000-0000-0000-0000-000000000001");
    var id = repository.saveDraft(version(2));
    repository.validated(id, 0);
    repository.activate(id, 1, false, "Roll forward");
    assertThat(repository.summary(seed).state()).isEqualTo("SUPERSEDED");
    repository.activate(seed, 1, true, "Revert downstream incompatibility");
    assertThat(repository.findActive("order.created").orElseThrow().version()).isEqualTo(1);
    assertThat(repository.summary(id).state()).isEqualTo("SUPERSEDED");
    assertThat(
            jdbc.sql("SELECT count(*) FROM audit_log WHERE action='PIPELINE_ROLLBACK'")
                .query(Long.class)
                .single())
        .isEqualTo(1);
    assertThatThrownBy(() -> jdbc.sql("DELETE FROM audit_log").update())
        .isInstanceOf(RuntimeException.class);
    assertThatThrownBy(() -> jdbc.sql("UPDATE audit_log SET reason='changed'").update())
        .isInstanceOf(RuntimeException.class);
    repository.disable(seed, 2, "Pause this pipeline");
    assertThat(repository.findActive("order.created")).isEmpty();
  }

  @Test
  void concurrentActivationOfSameRevisionHasOneWinner() throws Exception {
    var id = repository.saveDraft(version(2));
    repository.validated(id, 0);
    try (var executor = java.util.concurrent.Executors.newFixedThreadPool(2)) {
      java.util.concurrent.Callable<Boolean> activate =
          () -> {
            try {
              repository.activate(id, 1, false, "Concurrent activation");
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
    var result = engine.dryRun(pipeline, mapper.createObjectNode());
    assertThat(result.valid()).isFalse();
    assertThat(result.warnings()).hasSize(1);
    assertThat(result.executedSteps()).hasSize(2);
    assertThat(result.selectedRoutes()).hasSize(1);
    assertThat(result.durationNanos()).isPositive();
    org.mockito.Mockito.verifyNoInteractions(gateway);
    assertThat(count("processing_outbox")).isZero();
    assertThat(count("audit_log")).isZero();
  }
}
