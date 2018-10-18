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
    'subscription_items': stripe.SubscriptionItem,
    'balance_transactions': stripe.BalanceTransaction
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
    # Cannot find evidence of these streams having events associated:
    # subscription_items - appears on subscriptions events
    # balance_transactions - seems to be immutable
}

SUB_STREAMS = {
    'subscriptions': 'subscription_items'
}

LOGGER = singer.get_logger()

class Context():
    config = {}
    state = {}
    catalog = {}
    tap_start = None
    stream_map = {}
    new_counts = {}
    updated_counts = {}

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

    @classmethod
    def is_sub_stream(cls, stream_name):
        for sub_stream_id in SUB_STREAMS.values():
            if stream_name == sub_stream_id:
                return True
        return False

    @classmethod
    def print_counts(cls):
        LOGGER.info('------------------')
        for stream_name, stream_count in Context.new_counts.items():
            LOGGER.info('%s: %d new, %d updates',
                        stream_name,
                        stream_count,
                        Context.updated_counts[stream_name])
        LOGGER.info('------------------')


def configure_stripe_client():
    stripe.set_app_info(Context.config.get('user_agent', 'Singer.io Tap'),
                        url="https://github.com/singer-io/tap-stripe")
    # Set the API key we'll be using
    # https://github.com/stripe/stripe-python/tree/a9a8d754b73ad47bdece6ac4b4850822fa19db4e#usage
    stripe.api_key = Context.config.get('client_secret')
    # Allow ourselves to retry retriable network errors 5 times
    # https://github.com/stripe/stripe-python/tree/a9a8d754b73ad47bdece6ac4b4850822fa19db4e#configuring-automatic-retries
    stripe.max_network_retries = 5
    # Configure client-side network timeout of 1 second
    # https://github.com/stripe/stripe-python/tree/a9a8d754b73ad47bdece6ac4b4850822fa19db4e#configuring-a-client
    client = stripe.http_client.RequestsClient(timeout=5)
    stripe.default_http_client = client
    # Set stripe logging to INFO level
    # https://github.com/stripe/stripe-python/tree/a9a8d754b73ad47bdece6ac4b4850822fa19db4e#logging
    logging.getLogger('stripe').setLevel(logging.INFO)
    # Verify connectivity
    account = stripe.Account.retrieve(Context.config.get('account_id'))
    msg = "Successfully connected to Stripe Account with display name" \
          + " `%s`"
    LOGGER.info(msg, account.display_name)

class DependencyException(Exception):
    pass

def validate_dependencies():
    errs = []
    msg_tmpl = ("Unable to extract {0} data. "
                "To receive {0} data, you also need to select {1}.")

    for catalog_entry in Context.catalog['streams']:
        stream_id = catalog_entry['tap_stream_id']
        sub_stream_id = SUB_STREAMS.get(stream_id)
        if sub_stream_id:
            if Context.is_selected(sub_stream_id) and not Context.is_selected(stream_id):
                #throw error here
                errs.append(msg_tmpl.format(sub_stream_id, stream_id))

    if errs:
        raise DependencyException(" ".join(errs))


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

def sync_stream(stream_name):
    catalog_entry = Context.get_catalog_entry(stream_name)
    stream_schema = catalog_entry['schema']
    stream_metadata = metadata.to_map(catalog_entry['metadata'])
    extraction_time = singer.utils.now()
    stream_bookmark = singer.get_bookmark(Context.state, stream_name, 'id')
    bookmark = stream_bookmark
    # if this stream has a sub_stream, compare the bookmark
    sub_stream_name = SUB_STREAMS.get(stream_name)
    if sub_stream_name:
        sub_stream_bookmark = singer.get_bookmark(Context.state, sub_stream_name, 'id')
        # if there is a sub stream, set bookmark to sub stream's bookmark
        # since we know it must be earlier than the stream's bookmark
        if sub_stream_bookmark != stream_bookmark:
            bookmark = sub_stream_bookmark
    with Transformer(singer.UNIX_SECONDS_INTEGER_DATETIME_PARSING) as transformer:
        for stream_obj in STREAM_SDK_OBJECTS[stream_name].list(
                # If we want to increase the page size we can do
                # `limit=N` as a second parameter here.
                stripe_account=Context.config.get('account_id'),
                # None passed to starting_after appears to retrieve
                # all of them so this should always be safe.
                starting_after=bookmark
        ).auto_paging_iter():
            if sub_stream_name:
                sub_stream_bookmark = singer.get_bookmark(Context.state, sub_stream_name, 'id')
            should_sync_sub_stream = sub_stream_name and Context.is_selected(sub_stream_name)

            # If there is no sub stream, or there is and it isn't selected,
            # or the sub stream is up to date (bookmarks are equal),
            # the stream should be sync'd
            should_sync_stream = not sub_stream_name \
            or not Context.is_selected(sub_stream_name) \
                or stream_bookmark == sub_stream_bookmark

            # if the bookmark equals the stream bookmark, sync stream records
            if should_sync_stream:

                rec = transformer.transform(stream_obj.to_dict_recursive(),
                                            stream_schema,
                                            stream_metadata)

                singer.write_record(stream_name,
                                    rec,
                                    time_extracted=extraction_time)

                Context.new_counts[stream_name] += 1

                stream_bookmark = stream_obj.id

                singer.write_bookmark(Context.state,
                                      stream_name,
                                      'id',
                                      stream_obj.id)

            # sync sub streams
            if should_sync_sub_stream:
                sync_sub_stream(sub_stream_name, stream_obj.id)

            # write state after every 100 records
            if (Context.new_counts[stream_name] % 100) == 0:
                singer.write_state(Context.state)

    singer.write_state(Context.state)


def sync_sub_stream(sub_stream_name, parent_id):
    sub_stream_catalog_entry = Context.get_catalog_entry(sub_stream_name)
    sub_stream_schema = sub_stream_catalog_entry['schema']
    sub_stream_metadata = metadata.to_map(sub_stream_catalog_entry['metadata'])
    extraction_time = singer.utils.now()
    with Transformer(singer.UNIX_SECONDS_INTEGER_DATETIME_PARSING) as transformer:
        for sub_stream_obj in STREAM_SDK_OBJECTS[sub_stream_name].list(
                # If we want to increase the page size we can do
                # `limit=N` as a second parameter here.
                stripe_account=Context.config.get('account_id'),
                subscription=parent_id
        ).auto_paging_iter():

            rec = transformer.transform(sub_stream_obj.to_dict_recursive(),
                                        sub_stream_schema,
                                        sub_stream_metadata)

            singer.write_record(sub_stream_name,
                                rec,
                                time_extracted=extraction_time)
            Context.new_counts[sub_stream_name] += 1

            sub_stream_bookmark = parent_id

            singer.write_bookmark(Context.state,
                                  sub_stream_name,
                                  'id',
                                  sub_stream_bookmark)

def sync_event_updates():
    '''
    Get updates via events endpoint

    look at 'events update' bookmark and pull events after that
    '''
    extraction_time = singer.utils.now()
    bookmark_value = singer.get_bookmark(Context.state, 'events', 'created') or 0
    max_created = bookmark_value

    for events_obj in STREAM_SDK_OBJECTS['events'].list(
            # If we want to increase the page size we can do
            # `limit=N` as a second parameter here.
            stripe_account=Context.config.get('account_id'),
            # None passed to starting_after appears to retrieve
            # all of them so this should always be safe.
            **{"created[gt]": max_created}
    ).auto_paging_iter():
        event_resource_obj = events_obj.data.object
        event_resource_name = EVENT_RESOURCE_TO_STREAM.get(event_resource_obj.object)
        event_resource_stream = Context.get_catalog_entry(event_resource_name)

        # if we got an event for a selected stream, sync the updates for that stream
        if event_resource_stream and Context.is_selected(event_resource_name):
            with Transformer(singer.UNIX_SECONDS_INTEGER_DATETIME_PARSING) as transformer:
                event_resource_metadata = metadata.to_map(event_resource_stream['metadata'])
                rec = transformer.transform(event_resource_obj.to_dict_recursive(),
                                            event_resource_stream['schema'],
                                            event_resource_metadata)

                if events_obj.created > bookmark_value:
                    if rec.get('id') is not None:
                        singer.write_record(event_resource_name,
                                            rec,
                                            time_extracted=extraction_time)
                        Context.updated_counts[event_resource_name] += 1

                    if events_obj.created > max_created:
                        max_created = events_obj.created
                        singer.write_bookmark(Context.state,
                                              'events',
                                              'created',
                                              max_created)

    singer.write_state(Context.state)

def any_streams_selected():
    return any(s for s in STREAM_SDK_OBJECTS if Context.is_selected(s))

def sync():
    # Write all schemas and init count to 0
    for catalog_entry in Context.catalog['streams']:
        if Context.is_selected(catalog_entry["tap_stream_id"]):
            singer.write_schema(catalog_entry['tap_stream_id'],
                                catalog_entry['schema'],
                                'id')

            Context.new_counts[catalog_entry['tap_stream_id']] = 0
            Context.updated_counts[catalog_entry['tap_stream_id']] = 0

    # Loop over streams in catalog
    for catalog_entry in Context.catalog['streams']:
        stream_name = catalog_entry['tap_stream_id']
        # Sync records for stream
        if Context.is_selected(stream_name) and not Context.is_sub_stream(stream_name):
            sync_stream(stream_name)

    # Get event updates
    if any_streams_selected():
        sync_event_updates()

    # Print counts
    Context.print_counts()

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

        Context.config = args.config
        Context.state = args.state
        configure_stripe_client()
        validate_dependencies()
        sync()

if __name__ == "__main__":
    main()
