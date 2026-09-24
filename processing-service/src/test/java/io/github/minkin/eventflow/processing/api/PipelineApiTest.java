package io.github.minkin.eventflow.processing.api;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.json.JsonMapper;
import io.github.minkin.eventflow.processing.pipeline.PipelineConflictException;
import io.github.minkin.eventflow.processing.pipeline.PipelineDefinitionCodec;
import io.github.minkin.eventflow.processing.pipeline.PipelineDefinitionValidator;
import io.github.minkin.eventflow.processing.pipeline.PipelineEngine;
import io.github.minkin.eventflow.processing.pipeline.PipelineRepository;
import io.github.minkin.eventflow.processing.pipeline.SchemaDefinitionValidator;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class PipelineApiTest {
  @org.junit.jupiter.api.AfterEach
  void clearSecurity() {
    org.springframework.security.core.context.SecurityContextHolder.clearContext();
  }

  @Test
  void staleChangeReturns409AndRevisionIsRequired() throws Exception {
    var jwt =
        org.springframework.security.oauth2.jwt.Jwt.withTokenValue("test")
            .header("alg", "RS256")
            .subject("test-producer")
            .claim("tenant_id", "demo")
            .build();
    org.springframework.security.core.context.SecurityContextHolder.getContext()
        .setAuthentication(
            new org.springframework.security.oauth2.server.resource.authentication
                .JwtAuthenticationToken(
                jwt,
                java.util.List.of(
                    new org.springframework.security.core.authority.SimpleGrantedAuthority(
                        "ROLE_ADMIN"))));
    var repository = mock(PipelineRepository.class);
    UUID id = UUID.randomUUID();
    when(repository.disable("demo", id, 7, "pause")).thenThrow(new PipelineConflictException());
    var controller =
        new PipelineAdminController(
            repository,
            mock(PipelineDefinitionValidator.class),
            new SchemaDefinitionValidator(),
            new PipelineDefinitionCodec(JsonMapper.builder().build()),
            mock(PipelineEngine.class));
    var mvc =
        MockMvcBuilders.standaloneSetup(controller)
            .setControllerAdvice(new ApiExceptionHandler())
            .build();
    mvc.perform(
            post("/api/v1/pipelines/{id}/disable?revision=7", id)
                .contentType("application/json")
                .content("{\"reason\":\"pause\"}"))
        .andExpect(status().isConflict());
    mvc.perform(
            post("/api/v1/pipelines/{id}/disable", id)
                .contentType("application/json")
                .content("{\"reason\":\"pause\"}"))
        .andExpect(status().isBadRequest());
  }
}
