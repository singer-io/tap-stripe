#!/usr/bin/env python

# TODO force a 429 and see how to handle it with the SDK

import stripe
improt sys

stripe.api_key = sys.argv[1]

# TODO untested
while True:
    stripe.Charges.list()
