package io.github.minkin.eventflow.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.MountableFile;

/** Runs the packaged applications, their real migrations, schedulers, Kafka listeners and sinks. */
class EventLifecycleE2E {
  private com.nimbusds.jose.jwk.RSAKey signingKey;

  @Test
  void acceptanceDuplicateBothSinksAndProcessingDlqReplay() throws Exception {
    signingKey = new com.nimbusds.jose.jwk.gen.RSAKeyGenerator(2048).keyID("e2e").generate();
    try (var network = Network.newNetwork();
        var jwks =
            new GenericContainer<>("nginx:1.27-alpine")
                .withNetwork(network)
                .withNetworkAliases("jwks")
                .withCopyToContainer(
                    org.testcontainers.images.builder.Transferable.of(
                        new com.nimbusds.jose.jwk.JWKSet(signingKey.toPublicJWK()).toString()),
                    "/usr/share/nginx/html/jwks.json");
        var postgres =
            new PostgreSQLContainer<>("postgres:17-alpine")
                .withDatabaseName("postgres")
                .withNetwork(network)
                .withNetworkAliases("postgres");
        var kafka =
            new KafkaContainer("apache/kafka:3.9.1")
                .withListener("kafka:19092")
                .withNetwork(network)
                .withNetworkAliases("kafka");
        var redis =
            new GenericContainer<>("redis:7.4-alpine")
                .withNetwork(network)
                .withNetworkAliases("redis");
        var clickhouse =
            new GenericContainer<>("clickhouse/clickhouse-server:25.3-alpine")
                .withNetwork(network)
                .withNetworkAliases("clickhouse")
                .withEnv("CLICKHOUSE_USER", "eventflow")
                .withEnv("CLICKHOUSE_PASSWORD", "test-password")
                .withExposedPorts(8123)
                .waitingFor(Wait.forHttp("/ping"))) {
      jwks.start();
      postgres.start();
      kafka.start();
      redis.start();
      clickhouse.start();
      for (String db : new String[] {"ingestion", "processing", "delivery"}) {
        var result =
            postgres.execInContainer(
                "psql",
                "-U",
                postgres.getUsername(),
                "-d",
                "postgres",
                "-c",
                "CREATE DATABASE eventflow_" + db);
        assertThat(result.getExitCode()).as(result.getStderr()).isZero();
      }
      var ddl =
          clickhouse.execInContainer(
              "clickhouse-client",
              "--user",
              "eventflow",
              "--password",
              "test-password",
              "--multiquery",
              "--query",
              Files.readString(Path.of("..", "infrastructure", "clickhouse", "init.sql")));
      assertThat(ddl.getExitCode()).as(ddl.getStderr()).isZero();
      try (var ingestion = application("ingestion", 8080, network, postgres);
          var processing = application("processing", 8081, network, postgres);
          var delivery = application("delivery", 8082, network, postgres)) {
        ingestion.start();
        processing.start();
        delivery.start();
        var ingestApi = api(ingestion, 8080);
        var processApi = api(processing, 8081);
        var deliveryApi = api(delivery, 8082);
        var foreignIngest = api(ingestion, 8080, token("other", "foreign-admin", "ADMIN"));
        var foreignProcess = api(processing, 8081, token("other", "foreign-admin", "ADMIN"));
        var foreignDelivery = api(delivery, 8082, token("other", "foreign-admin", "ADMIN"));
        expectStatus(api(ingestion, 8080, null), "POST", "/api/v1/events", java.util.Map.of(), 401);
        expectStatus(
            api(processing, 8081, token("demo", "producer", "PRODUCER")),
            "POST",
            "/api/v1/pipelines",
            java.util.Map.of(),
            403);
        var mapper = JsonMapper.builder().findAndAddModules().build();
        UUID id = UUID.randomUUID();
        var event =
            mapper
                .createObjectNode()
                .put("eventId", id.toString())
                .put("eventType", "order.created")
                .put("source", "e2e")
                .put("tenantId", "other")
                .put("producerId", "forged")
                .put("schemaVersion", 1)
                .put("occurredAt", Instant.now().minusSeconds(5).toString());
        event.set(
            "payload",
            mapper.readTree(
                "{\"orderId\":\"42\",\"customerId\":\"7\",\"total\":12.50,\"currency\":\"USD\"}"));
        var accepted =
            ingestApi.post().uri("/api/v1/events").body(event).retrieve().toEntity(JsonNode.class);
        assertThat(accepted.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(accepted.getBody().path("duplicate").asBoolean()).isFalse();
        var duplicate =
            ingestApi.post().uri("/api/v1/events").body(event).retrieve().body(JsonNode.class);
        assertThat(duplicate.path("duplicate").asBoolean()).isTrue();
        await()
            .atMost(Duration.ofSeconds(90))
            .ignoreExceptions()
            .untilAsserted(
                () -> {
                  var statuses =
                      deliveryApi
                          .get()
                          .uri("/api/v1/events/{id}/deliveries", id)
                          .retrieve()
                          .body(JsonNode.class);
                  assertThat(statuses).hasSize(2);
                  statuses.forEach(
                      status -> assertThat(status.path("status").asText()).isEqualTo("SUCCEEDED"));
                });
        for (var scoped : java.util.List.of(foreignIngest, foreignProcess)) {
          expectStatus(scoped, "GET", "/api/v1/events/" + id, null, 404);
        }
        expectStatus(foreignDelivery, "GET", "/api/v1/events/" + id + "/deliveries", null, 404);
        expectStatus(foreignIngest, "POST", "/api/v1/events", event, 404);
        for (var spec :
            java.util.List.of(
                java.util.Map.entry(ingestion, 8080),
                java.util.Map.entry(processing, 8081),
                java.util.Map.entry(delivery, 8082))) {
          String path = "/api/v1/events/" + id + (spec.getValue() == 8082 ? "/deliveries" : "");
          expectStatus(
              api(spec.getKey(), spec.getValue(), token("demo", "another-producer", "PRODUCER")),
              "GET",
              path,
              null,
              404);
          expectStatus(
              api(spec.getKey(), spec.getValue(), token("demo", "test-producer", "PRODUCER")),
              "GET",
              path,
              null,
              200);
        }
        var deliveryDb =
            JdbcClient.create(
                new DriverManagerDataSource(
                    postgres.getJdbcUrl().replace("/postgres", "/eventflow_delivery"),
                    postgres.getUsername(),
                    postgres.getPassword()));
        assertThat(
                deliveryDb
                    .sql(
                        "SELECT count(*) FROM operational_event WHERE event_id=:id AND tenant_id='demo' AND producer_id='test-producer'")
                    .param("id", id)
                    .query(Long.class)
                    .single())
            .isEqualTo(1);
        var analytics =
            clickhouse.execInContainer(
                "clickhouse-client",
                "--user",
                "eventflow",
                "--password",
                "test-password",
                "--query",
                "SELECT count() FROM eventflow.events FINAL WHERE event_id='" + id + "'");
        assertThat(analytics.getStdout().trim()).isEqualTo("1");
        event.put("source", "changed");
        assertThatThrownBy(
                () ->
                    ingestApi
                        .post()
                        .uri("/api/v1/events")
                        .body(event)
                        .retrieve()
                        .toBodilessEntity())
            .isInstanceOf(HttpClientErrorException.Conflict.class);

        UUID replayId = UUID.randomUUID();
        event.put("eventId", replayId.toString()).put("eventType", "recovery.created");
        ingestApi.post().uri("/api/v1/events").body(event).retrieve().toBodilessEntity();
        await()
            .atMost(Duration.ofSeconds(60))
            .ignoreExceptions()
            .untilAsserted(
                () ->
                    assertThat(
                            processApi
                                .get()
                                .uri("/api/v1/events/{id}", replayId)
                                .retrieve()
                                .body(JsonNode.class)
                                .path("status")
                                .asText())
                        .isEqualTo("FAILED"));
        var pipeline =
            mapper.readTree(
                "{\"name\":\"recovery\",\"version\":1,\"eventType\":\"recovery.created\",\"steps\":[{\"type\":\"route\",\"target\":\"POSTGRES\",\"destination\":\"events\"}]}");
        var created =
            processApi.post().uri("/api/v1/pipelines").body(pipeline).retrieve().toBodilessEntity();
        String pipelinePath = created.getHeaders().getLocation().toString();
        expectStatus(foreignProcess, "GET", pipelinePath, null, 404);
        expectStatus(
            foreignProcess,
            "POST",
            pipelinePath + "/disable?revision=0",
            java.util.Map.of("reason", "foreign change"),
            404);
        foreignProcess.post().uri("/api/v1/pipelines").body(pipeline).retrieve().toBodilessEntity();
        processApi
            .post()
            .uri(created.getHeaders().getLocation().toString() + "/validate?revision=0")
            .retrieve()
            .toBodilessEntity();
        processApi
            .post()
            .uri(created.getHeaders().getLocation().toString() + "/activate?revision=1")
            .body(java.util.Map.of("reason", "E2E recovery"))
            .retrieve()
            .toBodilessEntity();
        var processingDb =
            JdbcClient.create(
                new DriverManagerDataSource(
                    postgres.getJdbcUrl().replace("/postgres", "/eventflow_processing"),
                    postgres.getUsername(),
                    postgres.getPassword()));
        UUID dlq =
            processingDb
                .sql("SELECT id FROM processing_dead_letter WHERE event_id=:id AND status='OPEN'")
                .param("id", replayId)
                .query(UUID.class)
                .single();
        expectStatus(foreignProcess, "POST", "/api/v1/dlq/" + dlq + "/replay", null, 404);
        assertThat(
                processingDb
                    .sql("SELECT status FROM processing_dead_letter WHERE id=:id")
                    .param("id", dlq)
                    .query(String.class)
                    .single())
            .isEqualTo("OPEN");
        assertThat(
                processingDb
                    .sql(
                        "SELECT count(*) FROM audit_log WHERE tenant_id='demo' AND actor='test-producer'")
                    .query(Long.class)
                    .single())
            .isGreaterThanOrEqualTo(3);
        processApi.post().uri("/api/v1/dlq/{id}/replay", dlq).retrieve().toBodilessEntity();
        await()
            .atMost(Duration.ofSeconds(60))
            .ignoreExceptions()
            .untilAsserted(
                () ->
                    assertThat(
                            deliveryApi
                                .get()
                                .uri("/api/v1/events/{id}/deliveries", replayId)
                                .retrieve()
                                .body(JsonNode.class)
                                .get(0)
                                .path("status")
                                .asText())
                        .isEqualTo("SUCCEEDED"));
        assertThat(
                processingDb
                    .sql("SELECT status FROM processing_dead_letter WHERE id=:id")
                    .param("id", dlq)
                    .query(String.class)
                    .single())
            .isEqualTo("RESOLVED");
      }
    }
  }

  private GenericContainer<?> application(
      String name, int port, Network network, PostgreSQLContainer<?> postgres) {
    String service = name + "-service";
    return new GenericContainer<>("eclipse-temurin:21-jre")
        .withNetwork(network)
        .withExposedPorts(port)
        .withCopyFileToContainer(
            MountableFile.forHostPath(
                Path.of("..", service, "target", service + "-0.1.0-SNAPSHOT.jar").toAbsolutePath()),
            "/app.jar")
        .withEnv("JWT_ISSUER", "https://e2e.eventflow.test")
        .withEnv("JWT_JWK_SET_URI", "http://jwks/jwks.json")
        .withEnv("DATABASE_URL", "jdbc:postgresql://postgres:5432/eventflow_" + name)
        .withEnv("DATABASE_USERNAME", postgres.getUsername())
        .withEnv("DATABASE_PASSWORD", postgres.getPassword())
        .withEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
        .withEnv("REDIS_HOST", "redis")
        .withEnv("CLICKHOUSE_URL", "http://clickhouse:8123")
        .withEnv("CLICKHOUSE_USERNAME", "eventflow")
        .withEnv("CLICKHOUSE_PASSWORD", "test-password")
        .withEnv("MANAGEMENT_TRACING_ENABLED", "false")
        .withCommand("java", "-Xmx256m", "-jar", "/app.jar")
        .waitingFor(
            Wait.forHttp("/actuator/health/readiness")
                .forPort(port)
                .withStartupTimeout(Duration.ofMinutes(2)));
  }

  private RestClient api(GenericContainer<?> app, int port) throws Exception {
    return api(app, port, token("demo", "test-producer", "ADMIN"));
  }

  private RestClient api(GenericContainer<?> app, int port, String token) {
    var builder =
        RestClient.builder().baseUrl("http://" + app.getHost() + ":" + app.getMappedPort(port));
    if (token != null) {
      builder.defaultHeader("Authorization", "Bearer " + token);
    }
    return builder.build();
  }

  private String token(String tenant, String subject, String role) throws Exception {
    var claims =
        new com.nimbusds.jwt.JWTClaimsSet.Builder()
            .issuer("https://e2e.eventflow.test")
            .audience(java.util.List.of("eventflow-ingestion", "eventflow-operations"))
            .subject(subject)
            .claim("tenant_id", tenant)
            .claim("roles", java.util.List.of(role))
            .expirationTime(java.util.Date.from(Instant.now().plusSeconds(1800)))
            .build();
    var jwt =
        new com.nimbusds.jwt.SignedJWT(
            new com.nimbusds.jose.JWSHeader.Builder(com.nimbusds.jose.JWSAlgorithm.RS256)
                .keyID("e2e")
                .build(),
            claims);
    jwt.sign(new com.nimbusds.jose.crypto.RSASSASigner(signingKey));
    return jwt.serialize();
  }

  private void expectStatus(
      RestClient client, String method, String path, Object body, int expected) {
    var request = client.method(org.springframework.http.HttpMethod.valueOf(method)).uri(path);
    if (body != null) {
      request.body(body);
    }
    request.exchange(
        (sent, response) -> {
          assertThat(response.getStatusCode().value()).as(method + " " + path).isEqualTo(expected);
          return null;
        });
  }
}
