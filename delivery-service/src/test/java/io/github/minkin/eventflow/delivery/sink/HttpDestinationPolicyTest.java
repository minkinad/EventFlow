package io.github.minkin.eventflow.delivery.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.junit.jupiter.api.Test;

class HttpDestinationPolicyTest {
  @Test
  void defaultsToDenyAndRequiresExactHttpsDestination() {
    assertThatThrownBy(
            () -> new HttpDestinationPolicy("").requireAllowed("https://example.com/events"))
        .isInstanceOf(IllegalArgumentException.class);
    var policy = new HttpDestinationPolicy("https://example.com/events");
    assertThat(policy.requireAllowed("https://example.com/events").getHost())
        .isEqualTo("example.com");
    for (String url :
        new String[] {
          "http://example.com/events",
          "file:///etc/passwd",
          "https://example.com/other",
          "https://user:pass@example.com/events",
          "https://example.com/events?secret=x"
        }) {
      assertThatThrownBy(() -> policy.requireAllowed(url))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  void rejectsPrivateMetadataMappedAndMixedDnsAnswers() throws Exception {
    for (String address :
        new String[] {
          "127.0.0.1",
          "0.0.0.0",
          "10.0.0.1",
          "172.16.0.1",
          "192.168.1.1",
          "169.254.169.254",
          "100.64.0.1",
          "224.0.0.1",
          "::1",
          "fc00::1",
          "fe80::1",
          "::ffff:127.0.0.1",
          "2001:db8::1"
        }) {
      InetAddress value = InetAddress.getByName(address);
      assertThatThrownBy(() -> HttpDestinationPolicy.requirePublic(new InetAddress[] {value}))
          .as(address)
          .isInstanceOf(UnknownHostException.class);
    }
    assertThatThrownBy(
            () ->
                HttpDestinationPolicy.requirePublic(
                    new InetAddress[] {
                      InetAddress.getByName("8.8.8.8"), InetAddress.getByName("127.0.0.1")
                    }))
        .isInstanceOf(UnknownHostException.class);
    assertThat(
            HttpDestinationPolicy.requirePublic(
                new InetAddress[] {InetAddress.getByName("8.8.8.8")}))
        .hasSize(1);
  }
}
