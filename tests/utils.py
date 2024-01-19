import json
import backoff
import random
from datetime import datetime as dt
from datetime import time, timedelta
from time import sleep

import stripe as stripe_client

from tap_tester import menagerie
from tap_tester import LOGGER
from base import BaseTapTest

from utils_invoices import create_invoice_items, create_invoices
# # uncomment line below for debug logging
# stripe_client.log = 'info'

midnight = int(dt.combine(dt.today(), time.min).timestamp())
NOW = dt.utcnow()
metadata_value = {"test_value": "senorita_alice_{}@stitchdata.com".format(NOW)}

stripe_client.api_version = '2022-11-15'
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
    'payment_intents': stripe_client.PaymentIntent,
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
        LOGGER.info("Acquiring all %s records", stream)

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

                    # only returns line items associated with the single newest invoice object that
                    # has at least 1 line item
                    return invoice_line_dict

                else:
                    raise AssertionError(f"invoice['lines']['data'] is not a list {invoice_line_dict}")

            return objects

        elif stream == "charges":
            stripe_obj = client[stream].list(limit=max_limit, created={"gte": midnight},
                                             expand=['data.refunds']) # retrieve fields by passing expand paramater in SDK object
            dict_obj = stripe_obj_to_dict(stripe_obj)
            if dict_obj.get('data'):
                for obj in dict_obj['data']:
                    if obj.get('refunds'):
                        refunds = obj['refunds']['data'] 
                        obj['refunds'] = refunds

                return dict_obj['data']

        elif stream == "customers":
            stripe_obj = client[stream].list(limit=max_limit, created={"gte": midnight},
                                             expand=['data.sources', 'data.subscriptions', 'data.tax_ids']) # retrieve fields by passing expand paramater in SDK object
            dict_obj = stripe_obj_to_dict(stripe_obj)

            if dict_obj.get('data'):
                for obj in dict_obj['data']:

                    if obj.get('sources'):
                        sources = obj['sources']['data']
                        obj['sources'] = sources
                    if obj.get('subscriptions'):
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

            if stream in ["payment_intents", "payouts", "products", "coupons", "plans", "invoice_items", "disputes", "transfers"]:
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
    elif stream == 'payment_intents':
        # Sometimes due to insufficient balance, stripe throws an error while creating records for
        # other streams like charges or payouts. Create a payment intent using card, source,
        # or payment_mehtod with type = card and card number = 4000 0000 0000 0077 to allow the
        # payment to bypass pending and go straight to avaialble balance in test data with `confirm`
        # param as true. Without `confirm` param stripe does not add balance.
        # Reference for failure: https://app.circleci.com/pipelines/github/singer-io/tap-stripe/1278/workflows/e1bc336d-a468-4b6d-b8a2-bc2dde0768f6/jobs/4239

        currency="usd"
        customer="cus_LAXuu6qTrq8LSf"
        # sources have been deprecated, switched to defualt_source = card instead
        card_id = stripe_client.Customer.retrieve(customer)['default_source']
        card_object = stripe_client.Customer.retrieve_source(customer, card_id)
        card_exp_month = card_object['exp_month']
        card_exp_year = card_object['exp_year']

        # keep card from ever expiring in the future
        if NOW.year >= card_exp_year and NOW.month >= card_exp_month:
            stripe_client.Customer.modify_source(
                customer,
                card_id,
                exp_year = dt.today().year + 2
            )

        client[stream].create(
            amount=random.randint(100, 10000),
            currency=currency,
            customer=customer,
            confirm=True
        )

        # Creating record for payment_intents without `confirm` param. Because confirmed payment_intents can't be updated later on and
        # we require to update record for event_updates test case.
        return client[stream].create(
            amount=random.randint(100, 10000),
            currency=currency,
            customer=customer,
            statement_descriptor="stitchdata"
        )
    elif stream == 'customers':
        return client[stream].create(
            address={'city': 'Philadelphia', 'country': 'US', 'line1': 'APT 2R.',
                'line1': '666 Street Rd.', 'postal_code': '11111', 'state': 'PA'},
            description="Description {}".format(NOW),
            email="stitchdata.test@gmail.com", # In the latest API version, it is mandatory to provide a valid email address
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
        # stream order is random so we may need this payment_intent to keep the stripe account
        # balance from getting too low to create payout objects

        current_balances = stripe_client.Balance.retrieve()['available']
        # if available balance goes below $100 usd add another $100.
        for balance in current_balances:
            if balance.get('currency') == 'usd' and balance.get('amount') <= 10000:
                # added balance bypasses pending if card 0077 is used
                stripe_client.PaymentIntent.create(
                    amount=10000,
                    currency="usd",
                    customer="cus_LAXuu6qTrq8LSf",
                    confirm=True,
                )

        return client[stream].create(
            amount=random.randint(1, 10),
            currency="usd",
            description="Comfortable cotton t-shirt {}".format(NOW),
            statement_descriptor="desc",
            metadata=metadata_value,
            method='standard',
        )
    elif stream == 'plans':
        return client[stream].create(
            active=True,
            amount=random.randint(1, 10000),
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
            type='good' # In the latest API version, it is mandatory to provide the value of the `type` field in the body.
        )

    return None

@backoff.on_exception(backoff.expo,
                      (stripe_client.error.InvalidRequestError,
                          stripe_client.error.APIError,
                          stripe_client.error.RateLimitError),
                      max_tries=2,
                      factor=2,
                      jitter=None)
def create_object(stream):
    """Logic for creating a record for a given  object stream"""
    global NOW
    NOW = dt.utcnow()  # update NOW time to maintain uniqueness across record

    LOGGER.info("Creating a %s record", stream)

    if stream in client:
        global metadata_value
        metadata_value = {"test_value": "senorita_alice_{}@stitchdata.com".format(NOW)}

        if stream in {'products', 'coupons', 'plans', 'payouts', 'payment_intents'}:
            return standard_create(stream)

        elif stream == 'customers':
            customer = standard_create(stream)
            customer_dict = stripe_obj_to_dict(customer)
            if customer_dict.get('subscriptions'):
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
                amount=random.randint(1, 10000),
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
            # Creating invoice record using olderversion because it generates invoice.lines data 
            # at the time of record creation itself
            customer_id = cust['id']
            customer_default_source = cust['default_source']

            item = create_invoice_items(customer_id, metadata_value, now_value = NOW)

            add_to_hidden('invoice_items', item['id'])

            invoices_response = create_invoices(customer_id, customer_default_source, metadata_value, now_value = NOW)
            return invoices_response

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
        # To generate the data for the `disputes` stream, we need to provide wrong card numbers
        #  in the `charges` API. Hence bifurcated this data creation into two.
        # Refer documentation: https://stripe.com/docs/testing#disputes
        if stream == 'charges' or stream == 'disputes':
            if stream == 'disputes':
                card_number = str(random.choice(["4000000000001976", "4000000000002685", "4000000000000259"]))
            else:
                card_number = "4242424242424242"
            # Create a Source, attach to new customer, then charge them.
            src = stripe_client.Source.create(
                type='card',
                card={
                    "number": card_number,
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
            return client['charges'].create(
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

        if stream == 'transfers':
            return client[stream].create(
                amount=1,
                currency="usd",
                destination="acct_1DOR67LsKC35uacf",
                transfer_group="ORDER_95"
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

    LOGGER.info("Updating %s object %s", stream, oid)

    if stream in client:
        if stream == "balance_transactions":
            # bt = list_all_object(stream)['data'][oid]

            # if bt['source'][:1] == "po":
            #     return update_object("payouts", bt['source'])
            # elif bt['source'][:1] == "ch":
            #     return update_object("charges", bt['source'])

            return None
        if stream == "payment_intents":
            raise NotImplementedError("Use update_payment_intent instead.")
        return client[stream].modify(
            oid, metadata={"test_value": "senor_bob_{}@stitchdata.com".format(NOW)},
        )

    return None

def update_payment_intent(stream, existing_objects=[]):
    """
    Update a payment_intent object.

    NB: The payment_intents object cannot generate `updated` events on the metadata field.
        Instead, updateing the payment_method will always require you to confirm the PaymentIntent.
        Reference: https://stripe.com/docs/api/payment_intents/update

        Additionally, we have observed a race condition in which a recently created  payment may
        have been confirmed either by an `autoconfirm` charge or by actions in another test. To
        reduce the risk of altering creates and updates on other streams, we are choosing to iterate
        through all exisitng objects and retry if a given object is already confirmed.
    """
    if not existing_objects:
        existing_objects = list_all_object(stream)
    for existing_obj in existing_objects:
        try:
            LOGGER.info("Updating %s object %s", stream, existing_obj["id"])
            updated_object = client[stream].confirm(
                existing_obj['id'], payment_method="pm_card_visa",
            )
        except stripe_client.error.InvalidRequestError as err:
            LOGGER.info("Update failed for %s object %s", stream, existing_obj["id"])
            is_final_iteration = existing_objects.index(existing_obj) == len(existing_objects) - 1
            is_previously_confirmed = 'previously confirmed' in err.error['message']

            # throw error if no objects were able to be updated
            if is_final_iteration and is_previously_confirmed:
                raise RuntimeError(f"The test client has exhausted the available {stream} objects to update.") from err

            # throw error if it is unrelated to the known race condition
            elif not is_previously_confirmed:
                raise err

            # otherwise try the next object
            LOGGER.info("Will attempt to update a new  %s object", stream)
            continue

        return updated_object
    return None


def delete_object(stream, oid):
    """Delete a specific record for a given object"""
    LOGGER.info("Deleting %s object %s", stream, oid)

    if stream in client:
        if stream in {"payouts","charges"}:
            return None

            # return client[stream].cancel(oid)
            # TODO ^ Can't be canceled when it's pending

            try:
                delete = client[stream].delete(oid)
                LOGGER.info("DELETE SUCCESSFUL of record {} in stream {}".format(oid, stream))
                return delete
            except:
                LOGGER.warn("DELETE FAILED of record {} in stream {}".format(oid, stream))

    return None
