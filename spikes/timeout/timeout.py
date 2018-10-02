#!/usr/bin/env python

import stripe
import sys

stripe.api_key = sys.argv[1]
# Can't use this until my [PR]() is merged
# stripe.max_network_retries = 5
client = stripe.http_client.RequestsClient(timeout=0.1)
# Use this until my [PR]() is merged
client._max_network_retries = lambda: 5
stripe.default_http_client = client
stripe.log = 'info'

while True:
    try:
        charges = stripe.Charge.list()
    except stripe.error.APIConnectionError as e:
        print(e.user_message)
        break
