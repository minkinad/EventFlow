package io.github.minkin.eventflow.ingestion.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CanonicalJsonTest {
    private final CanonicalJson canonicalJson = new CanonicalJson(new ObjectMapper());

    @Test
    void objectPropertyOrderDoesNotAffectHash() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        assertThat(canonicalJson.sha256(mapper.readTree("{\"b\":2,\"a\":1}")))
                .isEqualTo(canonicalJson.sha256(mapper.readTree("{\"a\":1,\"b\":2}")));
    }
}
