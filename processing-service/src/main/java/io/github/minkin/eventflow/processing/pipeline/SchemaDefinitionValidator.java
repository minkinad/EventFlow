package io.github.minkin.eventflow.processing.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import org.springframework.stereotype.Component;

import java.util.regex.Pattern;

@Component
public class SchemaDefinitionValidator {
    private static final Pattern NAME = Pattern.compile("^[a-z][a-z0-9._-]{1,119}$");
    private final JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);

    public void validate(String name, JsonNode definition) {
        if (!NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("Schema name must match " + NAME.pattern());
        }
        if (definition == null || !definition.isObject()) {
            throw new IllegalArgumentException("JSON Schema must be an object");
        }
        try {
            schemaFactory.getSchema(definition);
        } catch (RuntimeException exception) {
            throw new IllegalArgumentException("Invalid JSON Schema: " + exception.getMessage(), exception);
        }
    }
}
