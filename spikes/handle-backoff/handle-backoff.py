#!/usr/bin/env python

# TODO force a 429 and see how to handle it with the SDK

import stripe
import sys
from datetime import datetime

stripe.api_key = sys.argv[1]

script_start = datetime.now()

# Still nothing as of
# Getting Charges
# Request ran for 0.990934 seconds
# Charges last_response.code: 200
# Script running for 7449.92087 seconds

while True:
    print("Getting Charges")
    req_start = datetime.now()
    charges = stripe.Charge.list()
    print("Request ran for {} seconds".format(
        (datetime.now()-req_start).total_seconds()))
    print("Charges last_response.code: {}".format(charges.last_response.code))
    print("Script running for {} seconds".format(
        (datetime.now()-script_start).total_seconds()))
