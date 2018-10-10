#!/usr/bin/env python3
import os
import json
import logging

import stripe
import singer
from singer import utils, Transformer
from singer.transform import unix_seconds_to_datetime
from singer import metadata

REQUIRED_CONFIG_KEYS = [
    "account_id",
    "client_secret"
]
STREAM_SDK_OBJECTS = {
    'charges': stripe.Charge,
    'events': stripe.Event,
    'customers': stripe.Customer,
    'plans': stripe.Plan,
    'invoices': stripe.Invoice,
    'invoice_items': stripe.InvoiceItem,
    'transfers': stripe.Transfer,
    'coupons': stripe.Coupon,
    'subscriptions': stripe.Subscription,
#    'subscription_items': stripe.SubscriptionItem
}

EVENT_RESOURCE_TO_STREAM = {
    'charge': 'charges',
    'customer': 'customers',
    'plan': 'plans',
    'invoice': 'invoices',
    'invoiceitem': 'invoice_items',
    'transfer': 'transfers',
    'coupon': 'coupons',
    'subscription': 'subscriptions',
#    'subscriptionitem': 'subscription_items'
}

SUB_STREAMS = {
    'subscriptions': ['subscription_items']
}

LOGGER = singer.get_logger()

class Context():
    config = {}
    state = {}
    catalog = {}
    tap_start = None
    stream_map = {}

    @classmethod
    def get_catalog_entry(cls, stream_name):
        if not cls.stream_map:
            cls.stream_map = {s["tap_stream_id"]: s for s in cls.catalog['streams']}
        return cls.stream_map.get(stream_name)

    @classmethod
    def get_schema(cls, stream_name):
        stream = [s for s in cls.catalog["streams"] if s["tap_stream_id"] == stream_name][0]
        return stream["schema"]

    @classmethod
    def is_selected(cls, stream_name):
        stream = cls.get_catalog_entry(stream_name)
        stream_metadata = metadata.to_map(stream['metadata'])
        return metadata.get(stream_metadata, (), 'selected')


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas

def get_discovery_metadata(schema, key_property, replication_method, replication_key):
    mdata = metadata.new()
    mdata = metadata.write(mdata, (), 'table-key-properties', [key_property])
    mdata = metadata.write(mdata, (), 'forced-replication-method', replication_method)

    if replication_key:
        mdata = metadata.write(mdata, (), 'valid-replication-keys', [replication_key])

    for field_name in schema['properties'].keys():
        if field_name == key_property:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return metadata.to_list(mdata)


def discover():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():
        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata': get_discovery_metadata(schema, 'id', 'INCREMENTAL', 'id'),
            # Events may have a different key property than this. Change
            # if it's appropriate.
            'key_properties': ['id']
        }
        streams.append(catalog_entry)

    return {'streams': streams}

def sync():
    new_counts = {}
    updated_counts = {}

    # Write all schemas and init count to 0
    for catalog_entry in Context.catalog['streams']:
        if Context.is_selected(catalog_entry["tap_stream_id"]):
            singer.write_schema(catalog_entry['tap_stream_id'],
                                catalog_entry['schema'],
                                'id')

            new_counts[catalog_entry['tap_stream_id']] = 0
            updated_counts[catalog_entry['tap_stream_id']] = 0


    # Loop over streams in catalog
    for catalog_entry in Context.catalog['streams']:
        stream_id = catalog_entry['tap_stream_id']
        stream_schema = catalog_entry['schema']

        # Sync records for stream
        if Context.is_selected(stream_id):
            extraction_time = singer.utils.now()
            for stream_obj in STREAM_SDK_OBJECTS[stream_id].list(
                    # If we want to increase the page size we can do
                    # `limit=N` as a second parameter here.
                    stripe_account=Context.config.get('account_id'),
                    # None passed to starting_after appears to retrieve
                    # all of them so this should always be safe.
                    starting_after=singer.get_bookmark(Context.state, stream_id, 'id')
            ).auto_paging_iter():

                with Transformer(singer.UNIX_SECONDS_INTEGER_DATETIME_PARSING) as transformer:
                    rec = transformer.transform(stream_obj.to_dict_recursive(),
                                                stream_schema,
                                                {})

                    singer.write_record(stream_id,
                                        rec,
                                        time_extracted=extraction_time)

                    new_counts[stream_id] += 1

                    singer.write_bookmark(Context.state,
                                          stream_id,
                                          'id',
                                          stream_obj.id)
            singer.write_state(Context.state)

    # Get updates via events endpoint
    # look at 'events update' bookmark and pull events after that
    extraction_time = singer.utils.now()
    for events_obj in STREAM_SDK_OBJECTS['events'].list(
            # If we want to increase the page size we can do
            # `limit=N` as a second parameter here.
            stripe_account=Context.config.get('account_id'),
            # None passed to starting_after appears to retrieve
            # all of them so this should always be safe.
            starting_after=singer.get_bookmark(Context.state, 'events', 'updates_id')
    ).auto_paging_iter():
        event_resource_obj = events_obj.data.object
        event_resource_name = EVENT_RESOURCE_TO_STREAM.get(event_resource_obj.object)
        event_resource_stream = Context.get_catalog_entry(event_resource_name)

        # if we got an event for a selected stream, sync the updates for that stream
        if event_resource_stream and Context.is_selected(event_resource_name):
            with Transformer(singer.UNIX_SECONDS_INTEGER_DATETIME_PARSING) as transformer:
                rec = transformer.transform(event_resource_obj.to_dict_recursive(),
                                            event_resource_stream["schema"],
                                            {})
                # we've observed event_resources with ids (e.g. invoice.upcoming events)
                if rec.get('id'):
                    singer.write_record(event_resource_name,
                                        rec,
                                        time_extracted=extraction_time)

                    updated_counts[event_resource_name] += 1
                    singer.write_bookmark(Context.state,
                                                  'events',
                                                  'updates_id',
                                                  events_obj.id)
                else:
                    LOGGER.warning('Caught %s event for %s without an id (event id %s)!',
                                   events_obj.type,
                                   event_resource_name,
                                   events_obj.id)
    singer.write_state(Context.state)

    # log count of new and updated items
    LOGGER.info('------------------')
    for stream_name, stream_count in new_counts.items():
        LOGGER.info('%s: %d new, %d updates',
                    stream_name,
                    stream_count,
                    updated_counts[stream_name])
    LOGGER.info('------------------')


@utils.handle_top_exception(LOGGER)
def main():

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)



        # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:
        Context.tap_start = utils.now()
        if args.catalog:
            Context.catalog = args.catalog.to_dict()
        else:
            Context.catalog = discover()


        # Set the API key we'll be using
        # https://github.com/stripe/stripe-python/tree/a9a8d754b73ad47bdece6ac4b4850822fa19db4e#usage
        stripe.api_key = args.config.get('client_secret')
        # Allow ourselves to retry retriable network errors 5 times
        # https://github.com/stripe/stripe-python/tree/a9a8d754b73ad47bdece6ac4b4850822fa19db4e#configuring-automatic-retries
        stripe.max_network_retries = 5
        # Configure client-side network timeout of 1 second
        # https://github.com/stripe/stripe-python/tree/a9a8d754b73ad47bdece6ac4b4850822fa19db4e#configuring-a-client
        client = stripe.http_client.RequestsClient(timeout=1)
        stripe.default_http_client = client
        # Set stripe logging to INFO level
        # https://github.com/stripe/stripe-python/tree/a9a8d754b73ad47bdece6ac4b4850822fa19db4e#logging
        logging.getLogger('stripe').setLevel(logging.INFO)
        # Verify connectivity
        account = stripe.Account.retrieve(args.config.get('account_id'))
        msg = "Successfully connected to Stripe Account with display name" \
                      + " `%s`"
        LOGGER.info(msg, account.display_name)

        Context.config = args.config
        Context.state = args.state
        sync()

if __name__ == "__main__":
    main()
