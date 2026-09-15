package io.github.minkin.eventflow.delivery.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.minkin.eventflow.contracts.TargetType;
import io.github.minkin.eventflow.delivery.job.DeliveryJob;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.util.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientResponseException;

@Component
public class ExternalHttpDeliveryAdapter implements DeliveryAdapter {
  private final HttpDestinationPolicy policy;
  private final ObjectMapper mapper;
  private final CircuitBreakerRegistry breakers;
  private final MeterRegistry meters;
  private final CloseableHttpClient client;
  private final ScheduledExecutorService deadlines =
      Executors.newSingleThreadScheduledExecutor(
          r -> {
            var thread = new Thread(r, "http-delivery-deadline");
            thread.setDaemon(true);
            return thread;
          });

  @Autowired
  public ExternalHttpDeliveryAdapter(
      HttpDestinationPolicy policy,
      ObjectMapper mapper,
      CircuitBreakerRegistry breakers,
      MeterRegistry meters) {
    this(
        policy,
        mapper,
        breakers,
        meters,
        HttpClients.custom()
            .setConnectionManager(
                PoolingHttpClientConnectionManagerBuilder.create()
                    .setDnsResolver(policy)
                    .setDefaultConnectionConfig(
                        ConnectionConfig.custom()
                            .setConnectTimeout(Timeout.ofSeconds(2))
                            .setSocketTimeout(Timeout.ofSeconds(5))
                            .build())
                    .build())
            .setDefaultRequestConfig(
                RequestConfig.custom()
                    .setConnectionRequestTimeout(Timeout.ofSeconds(2))
                    .setResponseTimeout(Timeout.ofSeconds(5))
                    .build())
            .disableRedirectHandling()
            .disableAutomaticRetries()
            .build());
  }

  // Tests inject a TLS client for WireMock's self-signed certificate; production always uses the
  // policy resolver above.
  ExternalHttpDeliveryAdapter(
      HttpDestinationPolicy policy,
      ObjectMapper mapper,
      CircuitBreakerRegistry breakers,
      MeterRegistry meters,
      CloseableHttpClient client) {
    this.policy = policy;
    this.mapper = mapper;
    this.breakers = breakers;
    this.meters = meters;
    this.client = client;
  }

  @Override
  public TargetType targetType() {
    return TargetType.EXTERNAL_HTTP;
  }

  @Override
  public void deliver(DeliveryJob job) {
    URI destination = policy.requireAllowed(job.target().destination());
    String name = destinationKey(destination);
    var breaker = breakers.circuitBreaker(name);
    meters.gauge(
        "eventflow.delivery.circuit.breaker.state",
        java.util.List.of(io.micrometer.core.instrument.Tag.of("destination", name)),
        breaker,
        value ->
            switch (value.getState()) {
              case OPEN, FORCED_OPEN -> 1;
              case HALF_OPEN -> 2;
              default -> 0;
            });
    try {
      breaker.executeRunnable(() -> send(destination, job));
    } catch (RuntimeException failure) {
      meters
          .counter("eventflow.delivery.circuit.breaker.failures", "destination", name)
          .increment();
      throw failure;
    }
  }

  private void send(URI destination, DeliveryJob job) {
    byte[] body;
    try {
      body = mapper.writeValueAsBytes(job.command());
    } catch (JsonProcessingException failure) {
      throw new IllegalArgumentException("Invalid delivery body", failure);
    }
    if (body.length > 1_048_576) {
      throw new IllegalArgumentException("HTTP delivery body exceeds 1 MiB");
    }
    var request = new HttpPost(destination);
    request.setHeader("Idempotency-Key", job.command().eventId().toString());
    request.setEntity(new ByteArrayEntity(body, ContentType.APPLICATION_JSON));
    var deadline = deadlines.schedule(request::cancel, 10, TimeUnit.SECONDS);
    ClassicHttpResponse response = null;
    try {
      response = client.executeOpen(null, request, null);
      int status = response.getCode();
      if (status < 200 || status >= 300) {
        throw new RestClientResponseException(
            "HTTP destination returned " + status,
            status,
            "Destination response",
            null,
            new byte[0],
            StandardCharsets.UTF_8);
      }
    } catch (IOException failure) {
      Throwable cause = failure;
      while (cause != null) {
        if (cause instanceof HttpDestinationPolicy.ForbiddenAddressException) {
          throw new IllegalArgumentException("HTTP destination violates network policy", failure);
        }
        cause = cause.getCause();
      }
      throw new ResourceAccessException("HTTP destination unavailable", failure);
    } finally {
      // No response payload is consumed or logged. Abort before close to prevent unbounded
      // draining.
      request.cancel();
      if (response != null) {
        org.apache.hc.client5.http.impl.classic.CloseableHttpResponse.adapt(response)
            .close(org.apache.hc.core5.io.CloseMode.IMMEDIATE);
      }
      deadline.cancel(false);
    }
  }

  private String destinationKey(URI uri) {
    try {
      return "http-"
          + HexFormat.of()
              .formatHex(
                  MessageDigest.getInstance("SHA-256")
                      .digest(uri.toString().getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException failure) {
      throw new IllegalStateException(failure);
    }
  }

  @PreDestroy
  public void close() throws IOException {
    deadlines.shutdownNow();
    client.close();
  }
}
