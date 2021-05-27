import logging
import random
import json
import backoff
from datetime import datetime as dt
from datetime import time, timedelta
from time import sleep

import stripe as stripe_client

from tap_tester import menagerie
from base import BaseTapTest


midnight = int(dt.combine(dt.today(), time.min).timestamp())
NOW = dt.utcnow()
metadata_value = {"test_value": "senorita_alice_{}@stitchdata.com".format(NOW)}

stripe_client.api_key = BaseTapTest.get_credentials()["client_secret"]
client = {
    'balance_transactions': stripe_client.BalanceTransaction,
    'charges': stripe_client.Charge,
    'coupons': stripe_client.Coupon,
    'customers': stripe_client.Customer,
    'disputes': stripe_client.Dispute,
    'events': stripe_client.Event,
    'invoice_items': stripe_client.InvoiceItem,
    'invoice_line_items': stripe_client.Invoice, # substream of invoices
    'invoices': stripe_client.Invoice,
    'payout_transactions' : None, # not a native stream to Stripe
    'payouts': stripe_client.Payout,
    'plans': stripe_client.Plan,
    'products': stripe_client.Product,
    'subscription_items': stripe_client.SubscriptionItem,
    'subscriptions': stripe_client.Subscription,
    'transfers': stripe_client.Transfer,
}

hidden_tracking = False
hidden_objects = {stream: set()
                  for stream in client.keys()}


##########################################################################
### Util Methods
##########################################################################
def get_catalogs(conn_id, streams: list = None):
    """Get catalog from menagerie"""

    # Select all streams and no fields within streams
    found_catalogs = menagerie.get_catalogs(conn_id)

    if streams:
        found_catalogs = [
            catalog
            for catalog in found_catalogs
            if catalog.get("tap_stream_id") in streams
        ]

    return found_catalogs


def activate_tracking():
    """
    Start tracking objects that are created in util_stripe but
    are not expeclitly called in the test
    """
    global hidden_tracking

    hidden_tracking = True


def get_hidden_objects(stream):
    """Return all objects which were created but not returned"""
    return hidden_objects[stream]


def get_schema(stream):
    with open(os.getcwd() + '/stripe_fields_from_schema.json', 'r') as json_schema:
        schema = json.load(json_schema)
    return schema


##########################################################################
### Helper Methods
##########################################################################
def create_payment_method(cust, meta_val):
    """
    This method is must be called  whenever a customer does not have
    an associated payment method.
     - creating a charge
     - creating a subscription
    """
    pay_method = stripe_client.PaymentMethod.create(
        type="card",
        card={
            "number": "4242424242424242",
            "exp_month": 2,
            "exp_year": dt.today().year + 2,
            "cvc": "666",
        },
        metadata=meta_val,
    )
    stripe_client.PaymentMethod.attach(
        "{}".format(pay_method['id']),
        customer="{}".format(cust['id']),
    )
    cust = stripe_client.Customer.modify(
        cust['id'],
        metadata={"test_value": "senor_bob_{}@stitchdata.com".format(NOW)},
        invoice_settings={
            "default_payment_method": pay_method['id']
        },
    )
    add_to_hidden("customers", cust['id']) # TRACK UPDATE
    return cust, pay_method


def get_a_record(stream):
    """Get a random record for a given stream. If there are none, create one."""
    records = list_all_object(stream)
    index = rand_len(records)
    if index:
        return records[index]

    obj = create_object(stream)
    add_to_hidden(stream, obj['id'])
    return obj


def add_to_hidden(stream, oid):
    """pass in any objects which are created but not returned"""
    global hidden_tracking

    if hidden_tracking:
        hidden_objects[stream].add(oid)


def rand_len(resp):
    """return a random int between 0 and length, or just 0 if length == 0"""
    return random.randint(0, len(resp) -1) if len(resp) > 0 else None

def stripe_obj_to_dict(stripe_obj):
    stripe_json = json.dumps(stripe_obj, sort_keys=True, indent=2)
    dict_obj = json.loads(stripe_json)
    return dict_obj

def list_all_object(stream, max_limit: int = 100, get_invoice_lines: bool = False):
    """Retrieve all records for an object"""
    if stream in client:

        if stream == "subscriptions":
            stripe_obj = client[stream].list(limit=max_limit, created={"gte": midnight})
            dict_obj = stripe_obj_to_dict(stripe_obj)

            if dict_obj.get('data'):
                for obj in dict_obj['data']:

                    if obj['items']:
                        subscription_item_ids = [item['id'] for item in obj['items']['data']]
                        obj['items'] = subscription_item_ids

                return dict_obj['data']

        elif stream == "subscription_items":
            all_subscriptions= list_all_object("subscriptions")
            all_subscription_ids = {subscription['id'] for subscription in all_subscriptions}

            objects = []
            for subscription_id in all_subscription_ids:
                stripe_object = client[stream].list(subscription=subscription_id)['data']
                stripe_dict = stripe_obj_to_dict(stripe_object)
                if isinstance(stripe_dict, list):
                    objects += stripe_dict
                else:
                    # objects.append(stripe_dict)
                    raise NotImplementedError("Didn't accuont for a non-list object")

            return objects

        elif stream == "invoices":
            stripe_obj = client[stream].list(limit=max_limit, created={"gte": midnight})
            dict_obj = stripe_obj_to_dict(stripe_obj)

            if dict_obj.get('data') and not get_invoice_lines:
                for obj in dict_obj['data']:

                    if obj['lines']:
                        line_ids = []
                        for line in obj['lines']['data']:
                            # NB | Sometimes there is a 'unique_line_item_id' and sometimes a 'unique_id'
                            #      both of which differ from 'id', so it's unclear what we should be referencing.
                            #      The following logic matches the current behavior.
                            identifier = 'unique_line_item_id' if line.get('unique_line_item_id') else 'id'
                            line_id = line[identifier]
                            line_ids.append(line_id)

                        obj['lines'] = line_ids

            return dict_obj['data']

        elif stream == "invoice_line_items":
            all_invoices = list_all_object("invoices", get_invoice_lines=True)
            objects = []
            for invoice in all_invoices:
                invoice_dict = stripe_obj_to_dict(invoice)
                invoice_line_dict = invoice_dict['lines']['data']

                if isinstance(invoice_line_dict, list):
                    for item in invoice_line_dict:
                        item.update({'invoice': invoice['id']})

                    return invoice_line_dict

                else:
                    raise AssertionError(f"invoice['lines']['data'] is not a list {invoice_line_dict}")

            return objects

        elif stream == "customers":
            stripe_obj = client[stream].list(limit=max_limit, created={"gte": midnight})
            dict_obj = stripe_obj_to_dict(stripe_obj)

            if dict_obj.get('data'):
                for obj in dict_obj['data']:

                    if obj['sources']:
                        sources = obj['sources']['data']
                        obj['sources'] = sources
                    if obj['subscriptions']:
                        subscription_ids = [subscription['id'] for subscription in obj['subscriptions']['data']]
                        obj['subscriptions'] = subscription_ids

                return dict_obj['data']

            if not isinstance(dict_obj, list):
                return [dict_obj]
            return dict_obj


        stripe_obj = client[stream].list(limit=max_limit, created={"gte": midnight})
        dict_obj = stripe_obj_to_dict(stripe_obj)
        if dict_obj.get('data'):
            if not isinstance(dict_obj['data'], list):
                return [dict_obj['data']]
            return dict_obj['data']

        if not isinstance(dict_obj, list):
            return [dict_obj]
        return dict_obj

    return None


##########################################################################
### Create Methods
##########################################################################
def standard_create(stream):
    """Return create without any logic built in"""
    if stream == 'coupons':
        return client[stream].create(
            currency='usd',
            duration="repeating",
            duration_in_months=3,
            metadata=metadata_value,
            name='Coupon Name',
            percent_off=66.6,
            max_redemptions=1000000
        )
    elif stream == 'customers':
        return client[stream].create(
            address={'city': 'Philadelphia', 'country': 'US', 'line1': 'APT 2R.',
                'line1': '666 Street Rd.', 'postal_code': '11111', 'state': 'PA'},
            description="Description {}".format(NOW),
            email="senor_bob_{}@stitchdata.com".format(NOW),
            metadata=metadata_value,
            name="Roberto Alicia",
            # pyment_method=, see source explanation
            phone="9999999999",
            shipping={'name': 'Shipping name', 'phone': "9999999999",
                      'address': {'city': 'Philadelphia', 'country': 'US', 'line1': 'APT 2R.',
                                  'line1': '666 Street Rd.', 'postal_code': '11111', 'state': 'PA'}},
            balance=50000,
            coupon=get_a_record('coupons')['id'],
            # invoice_prefix='ABC',  # this can conflict with existing customers' invoices
            invoice_settings={"footer": "default footer"},
            next_invoice_sequence=1,
            preferred_locales=["English"],
            # source=,  # we are already attaching sources for customers so we should be fine without data
            tax_exempt="none",
            tax_id_data=[],
        )
    elif stream == 'payouts':
        return client[stream].create(
            amount=random.randint(0, 10000),
            currency="usd",
            description="Comfortable cotton t-shirt {}".format(NOW),
            statement_descriptor="desc",
            metadata=metadata_value,
            method='standard',
        )
    elif stream == 'plans':
        return client[stream].create(
            active=True,
            amount=random.randint(0, 10000),
            currency="usd",
            interval="year",
            metadata=metadata_value,
            nickname="nickname {}".format(NOW),
            product="prod_FITVlwf65MGiqn", # we used to use one service, product, now we create a new one
            # product={  # NOTE this is introducing an unaccounted object for product object
            #     "name": "Name{}".format(NOW),
            #     "active": True,
            #     "metadata": metadata_value,
            #     "statement_descriptor": "desc",
            #     "unit_label": "label",
            # },
            amount_decimal=None,
            billing_scheme='per_unit',
            interval_count=1,
            tiers=None,
            tier_mode=None,
            transform_usage={
                'divide_by': 2,
                'round': 'up',
            },
            trial_period_days=30,
            usage_type='licensed',
        )
    elif stream == 'products':
        return client[stream].create(
            active=True,
            description="Comfortable cotton t-shirt",
            metadata=metadata_value,
            name="Product Name {}".format(NOW),
            attributes=["size", "gender"],
            caption="This is a caption",
            package_dimensions={"height": 92670.16, "length": 9158.65, "weight": 582.73, "width": 96656496.18},
            shippable=True,
            url='fakeurl.stitch',
        )

    return None

@backoff.on_exception(backoff.expo,
                      (stripe_client.error.InvalidRequestError),
                      max_tries=2,
                      factor=2,
                      jitter=None)
def create_object(stream):
    """Logic for creating a record for a given  object stream"""
    global NOW
    NOW = dt.utcnow()  # update NOW time to maintain uniqueness across records

    if stream in client:
        global metadata_value
        metadata_value = {"test_value": "senorita_alice_{}@stitchdata.com".format(NOW)}

        if stream in {"disputes", "transfers"}:
            return None

        elif stream in {'products', 'coupons', 'plans', 'payouts'}:
            return standard_create(stream)

        elif stream == 'customers':
            customer = standard_create(stream)
            customer_dict = stripe_obj_to_dict(customer)
            if customer_dict['subscriptions']:
                subscription_ids = [subscription['id'] for subscription in customer_dict['subscriptions']['data']]
                customer_dict['subscriptions'] = subscription_ids
            return customer_dict

        elif stream == 'invoice_line_items':
            # An invoice_line_item is implicity generated by the creation of an invoice
            invoice = create_object('invoices')
            invoice_line = stripe_obj_to_dict(invoice['lines']['data'][0])
            invoice_line.update({'invoice': invoice['id']})
            return invoice_line

        cust = get_a_record('customers')

        if stream == 'invoice_items':
            return client[stream].create(
                amount=random.randint(0, 10000),
                currency="usd",
                customer=cust['id'],
                description="Comfortable cotton t-shirt {}".format(NOW),
                metadata=metadata_value,
                discountable=True,
                subscription_item=None,
                tax_rates=[],  # TODO enter the child attributes
            )
        elif stream == 'invoices':
            # Invoices requires the customer has an item associated with them
            item = client["{}_items".format(stream[:-1])].create(
                amount=random.randint(0, 10000),
                currency="usd",
                customer=cust['id'],
                description="Comfortable cotton t-shirt {}".format(NOW),
                metadata=metadata_value,
                discountable=True,
                subscription_item=None,
                tax_rates=[],  # TODO enter the child attributes
            )
            add_to_hidden('invoice_items', item['id'])
            return client[stream].create(
                customer=cust['id'],
                auto_advance=False,
                collection_method='charge_automatically',
                description="Comfortable cotton t-shirt {}".format(NOW),
                metadata=metadata_value,
                # custom_fields={
                #     'name': 'CustomName',
                #     'value': 'CustomValue',
                # },
                footer='footer',
                statement_descriptor='desc',
                default_source=cust['default_source'],
                default_tax_rates=[],
            )

        plan = get_a_record('plans')

        if stream == 'subscriptions':
            # Subscriptions require a customer to have a payment method
            cust, pay_method = create_payment_method(cust, metadata_value)
            backdate = NOW - timedelta(days=1)
            return client[stream].create(
                customer=cust['id'],
                items=[
                    {"plan": plan['id'],
                     "metadata": metadata_value},
                ],
                cancel_at_period_end=False,
                default_payment_method=pay_method['id'],
                backdate_start_date=backdate,
                metadata=metadata_value,
                billing_cycle_anchor=NOW + timedelta(days=30),
                billing_thresholds={'amount_gte':100000},
                collection_method='charge_automatically',
                coupon=get_a_record('coupons')['id'],
                default_source=cust['default_source'],
                default_tax_rates=None,  # TODO tax rates
                off_session=True,
                payment_behavior='allow_incomplete',
                pending_invoice_item_interval={
                    'interval':'month',
                    'interval_count':11,
                },
                proration_behavior='none',
                trial_from_plan=False,
            )

        subscription = get_a_record('subscriptions')
        plan = create_object('plans') # we cannot overwrite an existing plan for subscription_items
        add_to_hidden('plans', plan['id'])

        if stream == "subscription_items":
            add_to_hidden('subscriptions', subscription['id'])
            return client[stream].create(
                subscription=subscription['id'],
                plan=plan['id'],
                metadata=metadata_value,
                proration_behavior='none',
                quantity=2,
                billing_thresholds={'usage_gte':100000000},
                payment_behavior='allow_incomplete',
                # proration_date= not set ^
                tax_rates=[],  # TODO tax rates
            )

        if stream == 'charges':
            # Create a Source, attach to new customer, then charge them.
            src = stripe_client.Source.create(
                type='card',
                card={
                    "number": "4242424242424242",
                    "exp_month": 2,
                    "exp_year": dt.today().year + 2,
                    "cvc": "666",
                },
                currency='usd',
                owner={'email': "senor_bob@stitchdata.com"},
                metadata=metadata_value
            )
            cust = stripe_client.Customer.create(
                email="senor_bob@stitchdata.com",
                source=src['id'],
                metadata=metadata_value,
            )
            add_to_hidden('customers', cust['id'])
            return client[stream].create(
                amount=50,
                currency="usd",
                customer=cust['id'],
                description="An arbitrary string attached to object.",
                metadata=metadata_value,
                receipt_email="senor_bob@stitchdata.com",
                shipping={'name': 'Shipping name', 'phone': "9999999999",
                          'address': {'city': 'Philadelphia', 'country': 'US', 'line1': 'APT 2R.',
                                      'line1': '666 Street Rd.', 'postal_code': '11111', 'state': 'PA'}
                },
                source=src['id'],
                statement_descriptor_suffix="suffix",
                statement_descriptor="desc",
                # application_fee_amount=,  # CONNECT only
                capture=True,
                # on_behalf_of=,  # CONNECT only
                # transfer_data=,  # CONNECT only
                # transfer_group=,  # CONNECT only
            )

    return None


##########################################################################
### Upate and Delete
##########################################################################
def update_object(stream, oid):
    """
    Update a specific record for a given object.

    The update will always change the test_value under the metadata field
    which is found in all object streams.
    """
    global NOW
    NOW = dt.utcnow()

    if stream in client:
        if stream == "balance_transactions":
            # bt = list_all_object(stream)['data'][oid]

            # if bt['source'][:1] == "po":
            #     return update_object("payouts", bt['source'])
            # elif bt['source'][:1] == "ch":
            #     return update_object("charges", bt['source'])

            return None

        return client[stream].modify(
            oid, metadata={"test_value": "senor_bob_{}@stitchdata.com".format(NOW)},
        )

    return None


def delete_object(stream, oid):
    """Delete a specific record for a given object"""
    if stream in client:
        if stream in {"payouts","charges"}:
            return None

            # return client[stream].cancel(oid)
            # TODO ^ Can't be canceled when it's pending

            try:
                delete = client[stream].delete(oid)
                logging.info("DELETE SUCCESSFUL of record {} in stream {}".format(oid, stream))
                return delete
            except:
                logging.info("DELETE FAILED of record {} in stream {}".format(oid, stream))

    return None
