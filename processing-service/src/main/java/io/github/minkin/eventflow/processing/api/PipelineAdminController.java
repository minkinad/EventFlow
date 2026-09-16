package io.github.minkin.eventflow.processing.api;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.minkin.eventflow.processing.pipeline.DryRunResult;
import io.github.minkin.eventflow.processing.pipeline.PipelineDefinition;
import io.github.minkin.eventflow.processing.pipeline.PipelineDefinitionCodec;
import io.github.minkin.eventflow.processing.pipeline.PipelineDefinitionValidator;
import io.github.minkin.eventflow.processing.pipeline.PipelineEngine;
import io.github.minkin.eventflow.processing.pipeline.PipelineRepository;
import io.github.minkin.eventflow.processing.pipeline.PipelineSummary;
import io.github.minkin.eventflow.processing.pipeline.SchemaDefinitionValidator;
import java.net.URI;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
public class PipelineAdminController {
  private final PipelineRepository repository;
  private final PipelineDefinitionValidator validator;
  private final SchemaDefinitionValidator schemaValidator;
  private final PipelineDefinitionCodec codec;
  private final PipelineEngine engine;

  public PipelineAdminController(
      PipelineRepository repository,
      PipelineDefinitionValidator validator,
      SchemaDefinitionValidator schemaValidator,
      PipelineDefinitionCodec codec,
      PipelineEngine engine) {
    this.repository = repository;
    this.validator = validator;
    this.schemaValidator = schemaValidator;
    this.codec = codec;
    this.engine = engine;
  }

  @PutMapping("/schemas/{name}")
  ResponseEntity<Void> putSchema(@PathVariable String name, @RequestBody JsonNode schema) {
    schemaValidator.validate(name, schema);
    repository.saveSchema(name, schema);
    return ResponseEntity.noContent().build();
  }

  @PostMapping("/pipelines")
  ResponseEntity<PipelineSummary> createPipeline(@RequestBody JsonNode json) {
    var id = repository.saveDraft(codec.decode(json));
    return ResponseEntity.created(URI.create("/api/v1/pipelines/" + id))
        .body(repository.summary(id));
  }

  @GetMapping("/pipelines/{id}")
  PipelineSummary getPipeline(@PathVariable UUID id) {
    return repository.summary(id);
  }

  @PutMapping("/pipelines/{id}")
  PipelineSummary edit(
      @PathVariable UUID id, @RequestParam long revision, @RequestBody JsonNode json) {
    return repository.editDraft(id, revision, codec.decode(json));
  }

  @PostMapping("/pipelines/{id}/validate")
  PipelineSummary validate(@PathVariable UUID id, @RequestParam long revision) {
    validator.validate(definition(id));
    return repository.validated(id, revision);
  }

  @PostMapping("/pipelines/{id}/dry-run")
  DryRunResult dryRun(@PathVariable UUID id, @RequestBody JsonNode payload) {
    var pipeline = definition(id);
    validator.validate(pipeline);
    return engine.dryRun(pipeline, payload);
  }

  @PostMapping("/pipelines/{id}/activate")
  PipelineSummary activate(
      @PathVariable UUID id, @RequestParam long revision, @RequestBody ChangeReason change) {
    validator.validate(definition(id));
    return repository.activate(id, revision, false, change.reason());
  }

  @PostMapping("/pipelines/{id}/rollback")
  PipelineSummary rollback(
      @PathVariable UUID id, @RequestParam long revision, @RequestBody ChangeReason change) {
    validator.validate(definition(id));
    return repository.activate(id, revision, true, change.reason());
  }

  @PostMapping("/pipelines/{id}/disable")
  PipelineSummary disable(
      @PathVariable UUID id, @RequestParam long revision, @RequestBody ChangeReason change) {
    return repository.disable(id, revision, change.reason());
  }

  private PipelineDefinition definition(UUID id) {
    repository.summary(id);
    return repository.findById(id).orElseThrow();
  }

  public record ChangeReason(String reason) {}
}
