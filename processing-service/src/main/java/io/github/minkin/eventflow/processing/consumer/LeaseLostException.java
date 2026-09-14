package io.github.minkin.eventflow.processing.consumer;

public class LeaseLostException extends RuntimeException {
  public LeaseLostException() {
    super("Processing lease expired or was superseded");
  }
}
