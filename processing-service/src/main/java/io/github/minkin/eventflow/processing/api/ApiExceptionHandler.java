package io.github.minkin.eventflow.processing.api;

import java.net.URI;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {
  @ExceptionHandler(io.github.minkin.eventflow.processing.pipeline.PipelineConflictException.class)
  ProblemDetail revisionConflict(RuntimeException exception) {
    return problem(
        HttpStatus.CONFLICT,
        "pipeline-revision-conflict",
        "Pipeline conflict",
        exception.getMessage());
  }

  @ExceptionHandler(IllegalArgumentException.class)
  ProblemDetail invalidRequest(IllegalArgumentException exception) {
    return problem(
        HttpStatus.BAD_REQUEST,
        "invalid-request",
        "Request cannot be applied",
        exception.getMessage());
  }

  @ExceptionHandler(DataIntegrityViolationException.class)
  ProblemDetail conflict(DataIntegrityViolationException exception) {
    return problem(
        HttpStatus.CONFLICT,
        "configuration-conflict",
        "Configuration conflicts with existing state",
        "A pipeline with this name and version already exists");
  }

  private ProblemDetail problem(HttpStatus status, String type, String title, String detail) {
    ProblemDetail problem =
        ProblemDetail.forStatusAndDetail(status, detail == null ? title : detail);
    problem.setType(URI.create("urn:eventflow:problem:" + type));
    problem.setTitle(title);
    return problem;
  }
}
