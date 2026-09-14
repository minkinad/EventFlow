package io.github.minkin.eventflow.delivery.job;

public class LeaseLostException extends RuntimeException {
  public LeaseLostException() {
    super("Delivery lease expired or was superseded");
  }
}
