package io.github.minkin.eventflow.processing.consumer;

public class ProcessingBusyException extends RuntimeException {
    public ProcessingBusyException(String message) {
        super(message);
    }
}
