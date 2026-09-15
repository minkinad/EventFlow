package io.github.minkin.eventflow.delivery.sink;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hc.client5.http.DnsResolver;
import org.apache.hc.client5.http.SystemDefaultDnsResolver;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** The same resolver that checks addresses supplies the addresses used for the TCP connection. */
@Component
public class HttpDestinationPolicy implements DnsResolver {
  private final Set<URI> allowed;

  public HttpDestinationPolicy(
      @Value("${eventflow.http.allowed-destinations:}") String destinations) {
    allowed =
        Arrays.stream(destinations.split(","))
            .map(String::trim)
            .filter(value -> !value.isEmpty())
            .map(URI::create)
            .map(this::safeUri)
            .collect(Collectors.toUnmodifiableSet());
    if (allowed.size() > 100) {
      throw new IllegalArgumentException("At most 100 HTTP destinations may be configured");
    }
  }

  public URI requireAllowed(String destination) {
    URI uri = safeUri(URI.create(destination));
    if (!allowed.contains(uri)) {
      throw new IllegalArgumentException("HTTP destination is not allow-listed");
    }
    return uri;
  }

  private URI safeUri(URI uri) {
    if (!"https".equalsIgnoreCase(uri.getScheme())
        || uri.getHost() == null
        || uri.getUserInfo() != null
        || uri.getFragment() != null
        || uri.getQuery() != null) {
      throw new IllegalArgumentException(
          "HTTP destinations require HTTPS without credentials, query or fragment");
    }
    return uri.normalize();
  }

  @Override
  public InetAddress[] resolve(String host) throws UnknownHostException {
    return requirePublic(SystemDefaultDnsResolver.INSTANCE.resolve(host));
  }

  static InetAddress[] requirePublic(InetAddress[] addresses) throws UnknownHostException {
    if (addresses.length == 0) {
      throw new UnknownHostException("Destination resolved no addresses");
    }
    for (InetAddress address : addresses) {
      byte[] bytes = address.getAddress();
      boolean denied =
          address.isAnyLocalAddress()
              || address.isLoopbackAddress()
              || address.isLinkLocalAddress()
              || address.isSiteLocalAddress()
              || address.isMulticastAddress();
      int first = Byte.toUnsignedInt(bytes[0]);
      int second = Byte.toUnsignedInt(bytes[1]);
      if (bytes.length == 4) {
        denied |=
            first == 0
                || first >= 224
                || (first == 100 && second >= 64 && second <= 127)
                || (first == 192 && second == 0)
                || (first == 198 && (second == 18 || second == 19))
                || (first == 198 && second == 51)
                || (first == 203 && second == 0);
      } else {
        // Permit only global IPv6 unicast; exclude transition and documentation prefixes.
        denied |=
            (first & 0xe0) != 0x20
                || (first == 0x20 && second == 0x02)
                || (first == 0x20
                    && second == 0x01
                    && (bytes[2] == 0 || (bytes[2] == 0x0d && bytes[3] == (byte) 0xb8)));
      }
      if (denied) {
        throw new ForbiddenAddressException();
      }
    }
    return addresses;
  }

  @Override
  public String resolveCanonicalHostname(String host) {
    return host;
  }

  static class ForbiddenAddressException extends UnknownHostException {
    ForbiddenAddressException() {
      super("Destination address is not public unicast");
    }
  }
}
