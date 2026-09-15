package io.github.minkin.eventflow.delivery.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.json.JsonMapper;
import io.github.minkin.eventflow.contracts.DeliveryCommand;
import io.github.minkin.eventflow.contracts.DeliveryTarget;
import io.github.minkin.eventflow.contracts.TargetType;
import io.github.minkin.eventflow.delivery.job.DeliveryJob;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactoryBuilder;
import org.apache.hc.core5.ssl.SSLContexts;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class HttpDeliveryIT {
  @Container
  static final GenericContainer<?> WIREMOCK =
      new GenericContainer<>("wiremock/wiremock:3.9.1")
          .withExposedPorts(8080, 8443)
          .withCommand("--https-port", "8443")
          .waitingFor(Wait.forHttp("/__admin/mappings").forPort(8080));

  @Test
  void stableIdempotencyKeyNoRedirectAndExplicitRetryStatus() throws Exception {
    var mapper = JsonMapper.builder().findAndAddModules().build();
    var admin =
        RestClient.builder()
            .baseUrl("http://" + WIREMOCK.getHost() + ":" + WIREMOCK.getMappedPort(8080))
            .build();
    for (int status : new int[] {200, 302, 429}) {
      admin
          .post()
          .uri("/__admin/mappings")
          .body(
              Map.of(
                  "request",
                  Map.of("method", "POST", "url", "/status/" + status),
                  "response",
                  Map.of("status", status, "headers", Map.of("Location", "/redirect-target"))))
          .retrieve()
          .toBodilessEntity();
    }
    String origin = "https://" + WIREMOCK.getHost() + ":" + WIREMOCK.getMappedPort(8443);
    var policy =
        new HttpDestinationPolicy(
            origin + "/status/200," + origin + "/status/302," + origin + "/status/429");
    // The injected client is test-only: permit the container address and WireMock self-signed TLS.
    var tls =
        SSLConnectionSocketFactoryBuilder.create()
            .setSslContext(SSLContexts.custom().loadTrustMaterial((chain, type) -> true).build())
            .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
            .build();
    var client =
        HttpClients.custom()
            .setConnectionManager(
                PoolingHttpClientConnectionManagerBuilder.create().setSSLSocketFactory(tls).build())
            .disableRedirectHandling()
            .disableAutomaticRetries()
            .build();
    var adapter =
        new ExternalHttpDeliveryAdapter(
            policy, mapper, CircuitBreakerRegistry.ofDefaults(), new SimpleMeterRegistry(), client);
    try {
      UUID eventId = UUID.randomUUID();
      var command =
          new DeliveryCommand(
              1,
              UUID.randomUUID(),
              eventId,
              "order.created",
              "orders",
              1,
              mapper.createObjectNode(),
              List.of(),
              Map.of(),
              Instant.now(),
              null);
      var success =
          new DeliveryJob(
              UUID.randomUUID(),
              command,
              new DeliveryTarget(TargetType.EXTERNAL_HTTP, origin + "/status/200", Map.of()),
              1,
              UUID.randomUUID());
      var production =
          new ExternalHttpDeliveryAdapter(
              policy, mapper, CircuitBreakerRegistry.ofDefaults(), new SimpleMeterRegistry());
      try {
        assertThatThrownBy(() -> production.deliver(success))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("network policy");
      } finally {
        production.close();
      }
      adapter.deliver(success);
      adapter.deliver(success);
      var requests =
          admin
              .get()
              .uri("/__admin/requests")
              .retrieve()
              .body(com.fasterxml.jackson.databind.JsonNode.class)
              .path("requests");
      assertThat(requests).hasSize(2);
      var matching =
          admin
              .post()
              .uri("/__admin/requests/count")
              .body(
                  Map.of(
                      "method",
                      "POST",
                      "url",
                      "/status/200",
                      "headers",
                      Map.of("Idempotency-Key", Map.of("equalTo", eventId.toString()))))
              .retrieve()
              .body(com.fasterxml.jackson.databind.JsonNode.class);
      assertThat(matching.path("count").asInt()).isEqualTo(2);
      for (int status : new int[] {302, 429}) {
        var job =
            new DeliveryJob(
                UUID.randomUUID(),
                command,
                new DeliveryTarget(
                    TargetType.EXTERNAL_HTTP, origin + "/status/" + status, Map.of()),
                1,
                UUID.randomUUID());
        assertThatThrownBy(() -> adapter.deliver(job))
            .isInstanceOfSatisfying(
                RestClientResponseException.class,
                failure -> assertThat(failure.getStatusCode().value()).isEqualTo(status));
      }
      assertThat(
              admin
                  .get()
                  .uri("/__admin/requests")
                  .retrieve()
                  .body(com.fasterxml.jackson.databind.JsonNode.class)
                  .path("requests"))
          .hasSize(4);
    } finally {
      adapter.close();
    }
  }
}
