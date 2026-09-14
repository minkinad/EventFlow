package io.github.minkin.eventflow.delivery.job;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientResponseException;

@Component
public class DeliveryFailureClassifier {
  public boolean isRetryable(Throwable failure) {
    Throwable current = failure;
    while (current != null) {
      if (current instanceof IllegalArgumentException) {
        return false;
      }
      if (current instanceof RestClientResponseException response) {
        int status = response.getStatusCode().value();
        return status == 408 || status == 425 || status == 429 || status >= 500;
      }
      if (current instanceof ResourceAccessException
          || current instanceof TransientDataAccessException
          || current instanceof CallNotPermittedException) {
        return true;
      }
      current = current.getCause();
    }
    return true;
  }
}
