INSERT INTO schema_definition(name, definition, active, updated_at)
VALUES (
    'order-v1',
    '{
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "type": "object",
      "required": ["orderId", "customerId", "total", "currency"],
      "properties": {
        "orderId": {"type": "string", "minLength": 1},
        "customerId": {"type": "string", "minLength": 1},
        "total": {"type": "number", "minimum": 0},
        "currency": {"type": "string", "pattern": "^[A-Z]{3}$"}
      },
      "additionalProperties": true
    }'::jsonb,
    true,
    now()
);

INSERT INTO pipeline_definition(id, name, version, event_type, definition, enabled, created_at)
VALUES (
    '10000000-0000-0000-0000-000000000001',
    'orders',
    1,
    'order.created',
    '{
      "name": "orders",
      "version": 1,
      "eventType": "order.created",
      "steps": [
        {"type": "validate", "schema": "order-v1"},
        {"type": "route", "target": "POSTGRES", "destination": "operational_events"},
        {"type": "route", "target": "CLICKHOUSE", "destination": "events"}
      ]
    }'::jsonb,
    true,
    now()
);
