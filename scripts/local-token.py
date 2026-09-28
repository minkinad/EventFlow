#!/usr/bin/env python3
"""Print a short-lived client-credentials token for the local Keycloak realm."""
import json
import os
import sys
import urllib.parse
import urllib.request

client = os.getenv('CLIENT_ID', 'demo-producer')
secret = os.getenv('CLIENT_SECRET', client + '-local-only')
url = os.getenv('TOKEN_URL', 'http://localhost:8180/realms/eventflow/protocol/openid-connect/token')
form = urllib.parse.urlencode({'grant_type': 'client_credentials', 'client_id': client,
                              'client_secret': secret}).encode()
try:
    with urllib.request.urlopen(url, form, timeout=10) as response:
        print(json.load(response)['access_token'])
except (OSError, ValueError, KeyError) as error:
    print(f'Cannot obtain local token: {error}', file=sys.stderr)
    sys.exit(1)
