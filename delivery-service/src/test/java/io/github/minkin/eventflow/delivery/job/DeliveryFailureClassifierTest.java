package io.github.minkin.eventflow.delivery.job;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;

import static org.assertj.core.api.Assertions.assertThat;

class DeliveryFailureClassifierTest {
    private final DeliveryFailureClassifier classifier = new DeliveryFailureClassifier();

    @Test
    void classifiesTerminalAndTransientHttpFailures() {
        assertThat(classifier.isRetryable(new HttpClientErrorException(HttpStatus.BAD_REQUEST))).isFalse();
        assertThat(classifier.isRetryable(new HttpClientErrorException(HttpStatus.TOO_MANY_REQUESTS))).isTrue();
        assertThat(classifier.isRetryable(new HttpServerErrorException(HttpStatus.SERVICE_UNAVAILABLE))).isTrue();
        assertThat(classifier.isRetryable(new ResourceAccessException("timeout"))).isTrue();
        assertThat(classifier.isRetryable(new IllegalArgumentException("bad destination"))).isFalse();
    }
}
