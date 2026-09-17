import http from 'k6/http';
import { check } from 'k6';
import { randomUUID } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

// RATE is offered load, not a measured throughput claim. Local limiter defaults are lower.
export const options = {
  scenarios: {
    events: { executor: 'constant-arrival-rate', rate: Number(__ENV.RATE || 10),
      timeUnit: '1s', duration: __ENV.DURATION || '30s', preAllocatedVUs: 20, maxVUs: 200 },
  },
  thresholds: { http_req_failed: ['rate<0.01'], checks: ['rate>0.99'] },
};
export default function () {
  const event = {eventId: randomUUID(), eventType: 'order.created', source: 'load-test',
    schemaVersion: 1, occurredAt: new Date().toISOString(),
    payload: {orderId: randomUUID(), customerId: 'load', total: 42, currency: 'USD'}};
  const url = (__ENV.INGESTION_URL || 'http://localhost:8080') + '/api/v1/events';
  const params = {headers: {'Content-Type': 'application/json'}};
  const response = http.post(url, JSON.stringify(event), params);
  check(response, {'accepted': r => r.status === 202});
  if (__ENV.DUPLICATES === 'true') {
    const duplicate = http.post(url, JSON.stringify(event), params);
    check(duplicate, {'idempotent duplicate': r => r.status === 202 && r.json('duplicate') === true});
  }
}
