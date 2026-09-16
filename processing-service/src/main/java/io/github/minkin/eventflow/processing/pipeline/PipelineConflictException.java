package io.github.minkin.eventflow.processing.pipeline;

public class PipelineConflictException extends RuntimeException {
  public PipelineConflictException() {
    super("Pipeline revision or lifecycle state has changed; reload before retrying");
  }
}
