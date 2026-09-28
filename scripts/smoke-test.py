#!/usr/bin/env python3
"""Verify the local demo from REST acceptance through both durable destinations."""
import base64
import http.client
import json
import os
import time
import urllib.error
import urllib.request
import urllib.parse
import uuid
from datetime import datetime, timezone

INGESTION = os.getenv('INGESTION_URL', 'http://localhost:8080')
PROCESSING = os.getenv('PROCESSING_URL', 'http://localhost:8081')
DELIVERY = os.getenv('DELIVERY_URL', 'http://localhost:8082')
TOKEN = os.getenv('EVENTFLOW_TOKEN')
TOKEN_URL = os.getenv('TOKEN_URL', 'http://localhost:8180/realms/eventflow/protocol/openid-connect/token')

CLICKHOUSE = os.getenv('CLICKHOUSE_URL', 'http://localhost:8123')


def request(url, data=None, headers=None):
    body = json.dumps(data).encode() if data is not None else None
    headers = dict(headers or {'Content-Type': 'application/json'})
    if TOKEN:
        headers['Authorization'] = 'Bearer ' + TOKEN
    req = urllib.request.Request(url, body, headers)
    with urllib.request.urlopen(req, timeout=5) as response:
        raw = response.read(1024 * 1024)
        return response.status, json.loads(raw) if raw else None


def wait_until(label, check, timeout=120):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except (OSError, http.client.HTTPException, ValueError, KeyError) as error:
            last = str(error)
        time.sleep(1)
    raise RuntimeError(f'{label} did not complete in {timeout}s: {last}')


def main():
    global TOKEN
    if not TOKEN:
        form = urllib.parse.urlencode({'grant_type': 'client_credentials',
            'client_id': os.getenv('CLIENT_ID', 'demo-admin'),
            'client_secret': os.getenv('CLIENT_SECRET', 'demo-admin-local-only')}).encode()
        with urllib.request.urlopen(TOKEN_URL, form, timeout=10) as response:
            TOKEN = json.load(response)['access_token']
    for url in (INGESTION, PROCESSING, DELIVERY):
        wait_until(f'{url} readiness', lambda: request(url + '/actuator/health/readiness')[1]['status'] == 'UP')
    event_id = str(uuid.uuid4())
    event = {'eventId': event_id, 'eventType': 'order.created', 'source': 'smoke',
             'schemaVersion': 1, 'occurredAt': datetime.now(timezone.utc).isoformat(),
             'payload': {'orderId': event_id, 'customerId': 'smoke-customer', 'total': 42, 'currency': 'USD'}}
    status, accepted = request(INGESTION + '/api/v1/events', event)
    assert status == 202 and accepted['eventId'] == event_id and not accepted['duplicate']
    status, duplicate = request(INGESTION + '/api/v1/events', event)
    assert status == 202 and duplicate['duplicate'] and duplicate['acceptedAt'] == accepted['acceptedAt']
    wait_until('processing', lambda: request(PROCESSING + '/api/v1/events/' + event_id)[1]['status'] == 'SUCCEEDED')

    def delivered():
        jobs = request(DELIVERY + '/api/v1/events/' + event_id + '/deliveries')[1]
        return len(jobs) == 2 and all(job['status'] == 'SUCCEEDED' for job in jobs)

    wait_until('both destinations', delivered)
    user = os.getenv('CLICKHOUSE_USERNAME', 'eventflow')
    password = os.getenv('CLICKHOUSE_PASSWORD', 'eventflow')  # Local Compose only.
    authorization = base64.b64encode(f'{user}:{password}'.encode()).decode()
    query = f"SELECT count() AS count FROM eventflow.events FINAL WHERE event_id='{event_id}' FORMAT JSONEachRow"
    req = urllib.request.Request(CLICKHOUSE, query.encode(), {'Authorization': 'Basic ' + authorization})
    with urllib.request.urlopen(req, timeout=5) as response:
        assert int(json.loads(response.read())['count']) == 1
    print(f'SUCCESS eventId={event_id}: duplicate acceptance, processing, both deliveries, ClickHouse FINAL=1')


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        raise SystemExit(f'FAIL: {error}') from error
