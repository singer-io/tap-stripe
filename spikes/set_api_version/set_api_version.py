#!/usr/bin/env python

import stripe
import sys

account_id = sys.argv[1]
stripe.api_key = sys.argv[2]

account = stripe.Account.retrieve(account_id)

import ipdb; ipdb.set_trace()
1+1

# ipdb>  stripe.Charge.list(stripe_account=account_id).last_response.headers['Stripe-Version']
# '2014-11-20'

# ipdb> stripe.api_version = '2018-09-24'
# ipdb> stripe.Charge.list(stripe_account=account_id).last_response.headers['Stripe-Version']
# '2018-09-24'
