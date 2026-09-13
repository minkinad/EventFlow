package io.github.minkin.eventflow.processing.pipeline;

public class PipelineException extends RuntimeException {
  private final String code;
  private final boolean retryable;

  public PipelineException(String code, String message, boolean retryable) {
    super(message);
    this.code = code;
    this.retryable = retryable;
  }

  public PipelineException(String code, String message, boolean retryable, Throwable cause) {
    super(message, cause);
    this.code = code;
    this.retryable = retryable;
  }

  public String code() {
    return code;
  }

  public boolean retryable() {
    return retryable;
  }
}
