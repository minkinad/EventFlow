package io.github.minkin.eventflow.processing.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import com.fasterxml.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

class HttpEnrichmentGatewayTest {
  @Test
  void equalCustomerIdsResolveWithinTheDurableTenantScope() {
    var builder = RestClient.builder();
    var server = MockRestServiceServer.bindTo(builder).build();
    var gateway = new HttpEnrichmentGateway(builder, "https://enrichment.test");
    server
        .expect(
            requestTo("https://enrichment.test/api/v1/tenants/tenant-a/enrichments/customers/42"))
        .andRespond(withSuccess("{\"name\":\"Alice\"}", MediaType.APPLICATION_JSON));
    server
        .expect(
            requestTo("https://enrichment.test/api/v1/tenants/tenant-b/enrichments/customers/42"))
        .andRespond(withSuccess("{\"name\":\"Bob\"}", MediaType.APPLICATION_JSON));
    var payload =
        JsonMapper.builder()
            .build()
            .createObjectNode()
            .put("customerId", "42")
            .put("tenantId", "forged");
    assertThat(
            gateway
                .enrich("tenant-a", "customers", payload)
                .path("_enrichment")
                .path("name")
                .asText())
        .isEqualTo("Alice");
    assertThat(
            gateway
                .enrich("tenant-b", "customers", payload)
                .path("_enrichment")
                .path("name")
                .asText())
        .isEqualTo("Bob");
    assertThat(payload.has("_enrichment")).isFalse();
    server.verify();
  }
}
