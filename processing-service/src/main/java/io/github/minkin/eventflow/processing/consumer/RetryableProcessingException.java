package io.github.minkin.eventflow.processing.consumer;

public class RetryableProcessingException extends RuntimeException {
  public RetryableProcessingException(String message, Throwable cause) {
    super(message, cause);
  }
}
