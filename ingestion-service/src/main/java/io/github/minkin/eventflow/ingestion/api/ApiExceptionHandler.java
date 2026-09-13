package io.github.minkin.eventflow.ingestion.api;

import io.github.minkin.eventflow.ingestion.domain.IdempotencyConflictException;
import java.net.URI;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {
  @ExceptionHandler(IdempotencyConflictException.class)
  ProblemDetail idempotencyConflict(IdempotencyConflictException exception) {
    var detail = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, exception.getMessage());
    detail.setType(URI.create("urn:eventflow:problem:idempotency-conflict"));
    detail.setTitle("The eventId is already bound to another payload");
    return detail;
  }

  @ExceptionHandler(MethodArgumentNotValidException.class)
  ProblemDetail validation(MethodArgumentNotValidException exception) {
    var detail =
        ProblemDetail.forStatusAndDetail(
            HttpStatus.BAD_REQUEST, "Event envelope validation failed");
    detail.setType(URI.create("urn:eventflow:problem:invalid-event"));
    detail.setProperty(
        "violations",
        exception.getBindingResult().getFieldErrors().stream()
            .map(error -> error.getField() + ": " + error.getDefaultMessage())
            .toList());
    return detail;
  }
}
