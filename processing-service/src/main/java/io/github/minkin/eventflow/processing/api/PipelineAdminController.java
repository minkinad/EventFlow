package io.github.minkin.eventflow.processing.api;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.minkin.eventflow.processing.pipeline.PipelineDefinition;
import io.github.minkin.eventflow.processing.pipeline.PipelineDefinitionValidator;
import io.github.minkin.eventflow.processing.pipeline.PipelineRepository;
import io.github.minkin.eventflow.processing.pipeline.PipelineSummary;
import io.github.minkin.eventflow.processing.pipeline.SchemaDefinitionValidator;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;

@RestController
@RequestMapping("/api/v1")
public class PipelineAdminController {
    private final PipelineRepository repository;
    private final PipelineDefinitionValidator validator;
    private final SchemaDefinitionValidator schemaValidator;

    public PipelineAdminController(PipelineRepository repository, PipelineDefinitionValidator validator,
                                   SchemaDefinitionValidator schemaValidator) {
        this.repository = repository;
        this.validator = validator;
        this.schemaValidator = schemaValidator;
    }

    @PutMapping("/schemas/{name}")
    ResponseEntity<Void> putSchema(@PathVariable String name, @RequestBody JsonNode schema) {
        schemaValidator.validate(name, schema);
        repository.saveSchema(name, schema);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/pipelines")
    ResponseEntity<Void> createPipeline(@RequestBody PipelineDefinition pipeline) {
        validator.validate(pipeline);
        var id = repository.saveDraft(pipeline);
        return ResponseEntity.created(URI.create("/api/v1/pipelines/" + id)).build();
    }

    @GetMapping("/pipelines/{id}")
    ResponseEntity<PipelineSummary> getPipeline(@PathVariable java.util.UUID id) {
        return repository.findSummary(id).map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/pipelines/{id}/activate")
    ResponseEntity<Void> activate(@PathVariable java.util.UUID id) {
        PipelineDefinition definition = repository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Pipeline not found: " + id));
        validator.validate(definition);
        repository.activate(id);
        return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
    }
}
