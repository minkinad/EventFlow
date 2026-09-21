package io.github.minkin.eventflow.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@SpringJUnitConfig(ApiSecurityTest.Config.class)
@WebAppConfiguration
class ApiSecurityTest {
  static final RSAKey KEY;
  static final HttpServer JWKS;

  static {
    try {
      KEY = new RSAKeyGenerator(2048).keyID("security-test").generate();
      JWKS = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      JWKS.createContext(
          "/jwks",
          exchange -> {
            byte[] body =
                new JWKSet(KEY.toPublicJWK())
                    .toString()
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (var output = exchange.getResponseBody()) {
              output.write(body);
            }
          });
      JWKS.start();
    } catch (Exception failure) {
      throw new ExceptionInInitializerError(failure);
    }
  }

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry values) {
    values.add("eventflow.security.issuer", () -> "https://issuer.test");
    values.add("eventflow.security.audience", () -> "eventflow-test");
    values.add(
        "eventflow.security.jwk-set-uri",
        () -> "http://127.0.0.1:" + JWKS.getAddress().getPort() + "/jwks");
  }

  @Configuration
  @EnableWebMvc
  @EnableWebSecurity
  @Import(ApiSecurityConfiguration.class)
  static class Config {
    @Bean
    Endpoints endpoints() {
      return new Endpoints();
    }
  }

  @RestController
  static class Endpoints {
    @GetMapping({"/api/v1/events/1", "/api/v1/pipelines/1", "/actuator/prometheus"})
    Caller read() {
      return Caller.current();
    }

    @PostMapping({"/api/v1/events", "/api/v1/pipelines", "/api/v1/dlq/1/replay"})
    Caller write() {
      return Caller.current();
    }

    @GetMapping("/actuator/health/readiness")
    String readiness() {
      return "UP";
    }
  }

  @Autowired WebApplicationContext context;
  MockMvc mvc;

  @BeforeEach
  void setup() {
    mvc = MockMvcBuilders.webAppContextSetup(context).apply(springSecurity()).build();
  }

  @AfterAll
  static void stop() {
    JWKS.stop(0);
  }

  private JWTClaimsSet.Builder claims(String role) {
    return new JWTClaimsSet.Builder()
        .issuer("https://issuer.test")
        .audience("eventflow-test")
        .subject("producer-1")
        .claim("tenant_id", "tenant-a")
        .claim("roles", List.of(role))
        .issueTime(new Date())
        .expirationTime(Date.from(Instant.now().plusSeconds(300)));
  }

  private String signed(JWTClaimsSet.Builder claims) throws Exception {
    var jwt =
        new SignedJWT(
            new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(KEY.getKeyID()).build(),
            claims.build());
    jwt.sign(new RSASSASigner(KEY));
    return "Bearer " + jwt.serialize();
  }

  @Test
  void anonymousHasOnlyMinimalHealthAndApiKeyIsNotAuthentication() throws Exception {
    mvc.perform(get("/actuator/health/readiness")).andExpect(status().isOk());
    mvc.perform(post("/api/v1/events").header("X-API-Key", "anything"))
        .andExpect(status().isUnauthorized())
        .andExpect(content().contentType("application/problem+json"))
        .andExpect(header().string("WWW-Authenticate", "Bearer"));
    mvc.perform(get("/actuator/prometheus")).andExpect(status().isUnauthorized());
    assertThatThrownBy(Caller::current)
        .isInstanceOf(org.springframework.security.access.AccessDeniedException.class);
    assertThat(Caller.auditActor()).isEqualTo("system");
  }

  @Test
  void signatureIssuerAudienceLifetimeAndTenantAreEnforced() throws Exception {
    var badClaims =
        List.of(
            claims("ADMIN").issuer("https://attacker.test"),
            claims("ADMIN").audience("other-api"),
            claims("ADMIN").audience((List<String>) null),
            claims("ADMIN").expirationTime(Date.from(Instant.now().minusSeconds(300))),
            claims("ADMIN").expirationTime(null),
            claims("ADMIN").claim("tenant_id", null),
            claims("ADMIN").claim("tenant_id", "../../other"),
            claims("ADMIN").subject(""),
            claims("ADMIN").notBeforeTime(Date.from(Instant.now().plusSeconds(300))));
    for (var claims : badClaims) {
      mvc.perform(post("/api/v1/events").header("Authorization", signed(claims)))
          .andExpect(status().isUnauthorized());
    }
    var forged =
        new SignedJWT(
            new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(KEY.getKeyID()).build(),
            claims("ADMIN").build());
    forged.sign(new RSASSASigner(new RSAKeyGenerator(2048).generate()));
    mvc.perform(post("/api/v1/events").header("Authorization", "Bearer " + forged.serialize()))
        .andExpect(status().isUnauthorized());
  }

  @Test
  void rolesCannotEscalateAcrossApiBoundaries() throws Exception {
    String producer = signed(claims("PRODUCER"));
    mvc.perform(post("/api/v1/events").header("Authorization", producer))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.tenant").value("tenant-a"))
        .andExpect(jsonPath("$.subject").value("producer-1"))
        .andExpect(jsonPath("$.operator").value(false));
    mvc.perform(post("/api/v1/pipelines").header("Authorization", producer))
        .andExpect(status().isForbidden());
    mvc.perform(post("/api/v1/dlq/1/replay").header("Authorization", producer))
        .andExpect(status().isForbidden());
    mvc.perform(
            post("/api/v1/pipelines").header("Authorization", signed(claims("PIPELINE_EDITOR"))))
        .andExpect(status().isOk());
    mvc.perform(post("/api/v1/events").header("Authorization", signed(claims("PIPELINE_EDITOR"))))
        .andExpect(status().isForbidden());
    mvc.perform(
            post("/api/v1/dlq/1/replay").header("Authorization", signed(claims("DLQ_OPERATOR"))))
        .andExpect(status().isOk());
    mvc.perform(get("/api/v1/events/1").header("Authorization", signed(claims("VIEWER"))))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.operator").value(true));
    mvc.perform(post("/api/v1/dlq/1/replay").header("Authorization", signed(claims("VIEWER"))))
        .andExpect(status().isForbidden());
    mvc.perform(post("/api/v1/events").header("Authorization", signed(claims("unknown"))))
        .andExpect(status().isForbidden());
    mvc.perform(
            post("/api/v1/events")
                .header("Authorization", signed(claims("ADMIN").claim("roles", "ADMIN"))))
        .andExpect(status().isForbidden());
    mvc.perform(post("/api/v1/events").header("Authorization", signed(claims("ADMIN"))))
        .andExpect(status().isOk());
    mvc.perform(get("/actuator/env").header("Authorization", signed(claims("ADMIN"))))
        .andExpect(status().isForbidden());
  }
}
