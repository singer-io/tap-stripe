#!/usr/bin/env python

import sys

stripe_secret_key = sys.argv[1]

import stripe

stripe.api_key = stripe_secret_key

account = stripe.Account.retrieve()

import ipdb; ipdb.set_trace()
1+1

# Retrieve a couple sample objects

[x for x in stripe.Charge.list().auto_paging_iter()]

# expanding 'data.customer'. Probably not directly useful as sourcerer
# doesn't do this (I don't think)
# ipdb> [x.customer.keys() for x in stripe.Charge.list(expand=['data.customer']).auto_paging_iter() if x.customer]
# [dict_keys(['invoice_prefix', 'default_source', 'sources', 'subscriptions', 'default_card', 'object', 'id', 'livemode', 'created', 'metadata', 'shipping', 'email', 'description', 'tax_info', 'currency', 'tax_info_verification', 'cards', 'delinquent', 'account_balance', 'discount']), dict_keys(['invoice_prefix', 'default_source', 'sources', 'subscriptions', 'default_card', 'object', 'id', 'livemode', 'created', 'metadata', 'shipping', 'email', 'description', 'tax_info', 'currency', 'tax_info_verification', 'cards', 'delinquent', 'account_balance', 'discount']), dict_keys(['invoice_prefix', 'default_source', 'sources', 'subscriptions', 'default_card', 'object', 'id', 'livemode', 'created', 'metadata', 'shipping', 'email', 'description', 'tax_info', 'currency', 'tax_info_verification', 'cards', 'delinquent', 'account_balance', 'discount']), dict_keys(['invoice_prefix', 'default_source', 'sources', 'subscriptions', 'default_card', 'object', 'id', 'livemode', 'created', 'metadata', 'shipping', 'email', 'description', 'tax_info', 'currency', 'tax_info_verification', 'cards', 'delinquent', 'account_balance', 'discount'])]
# ipdb> [x.customer for x in stripe.Charge.list().auto_paging_iter() if x.customer]
# ['cus_6ZXlps8Nz326Cf', 'cus_6ZXlps8Nz326Cf', 'cus_6ZXlps8Nz326Cf', 'cus_6ZXlps8Nz326Cf']

# This call was made with stripe.api_key != the account's api_key
# ipdb> stripe.api_key
# <REDACTED STITCH STRIPE TEST KEY>
# ipdb> stripe.Account.retrieve().id
# 'acct_14zvmQDcBSxinnbL' # The Stitch Dev Account
# ipdb> stripe.Account.retrieve("acct_15VQRNKi8yTvIJwI").id # account 1742 conn 27092
# 'acct_15VQRNKi8yTvIJwI'
# This one is the test account
# ipdb> len(stripe.Charge.list())
# 7
# This one is the 1742/27092 account
# ipdb> len(stripe.Charge.list(stripe_account="acct_15VQRNKi8yTvIJwI"))
# 2

# We can use gte filtering like so
# ipdb> sorted([x.created for x in stripe.Event.list(stripe_account="acct_15VQRNKi8yTvIJwI").data])
# [1424111008, 1424111009, 1424123398, 1424123398, 1424583077]
# ipdb> sorted([x.created for x in stripe.Event.list(stripe_account="acct_15VQRNKi8yTvIJwI", created={"gte": 1424123398}).data])
# [1424123398, 1424123398, 1424583077]

# Here's how we do object id pagination

# ipdb> [c.stripe_id for c in stripe.Charge.list()]
# ['ch_1DGvHKDcBSxinnbL8cqeaClE', 'ch_1CncCSDcBSxinnbLGAg0FgCS', 'ch_1AfIBEDcBSxinnbLXEPmc6Yc', 'ch_16OI9wDcBSxinnbLb7d17FuT', 'ch_16MOjADcBSxinnbLMmmwvRi0', 'ch_16Jo1HDcBSxinnbLDT7Mk8hO', 'ch_16JnhSDcBSxinnbLK5KrZm2F', 'ch_16AOSQDcBSxinnbLaqa4dHqZ']
# ipdb> [c.stripe_id for c in stripe.Charge.list(starting_after="ch_16MOjADcBSxinnbLMmmwvRi0")]
# ['ch_16Jo1HDcBSxinnbLDT7Mk8hO', 'ch_16JnhSDcBSxinnbLK5KrZm2F', 'ch_16AOSQDcBSxinnbLaqa4dHqZ']

# Checking how many accounts we can talk to
# ipdb> stripe.Account.retrieve().id
# 'acct_14zvmQDcBSxinnbL' # The Stitch Dev Account
# ipdb> stripe.api_key
# <REDACTED STITCH STRIPE TEST KEY>
# ipdb> stripe.api_key = <REDACTED STITCH STRIPE LIVE KEY>
# ipdb> stripe.Account.retrieve().id
# 'acct_14zvmQDcBSxinnbL' # The Stitch Dev Account again
