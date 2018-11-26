#!/usr/bin/env python3
import os
import json
import logging
import re

import stripe
import stripe.error
import singer
from singer import utils, Transformer, metrics
from singer import metadata

REQUIRED_CONFIG_KEYS = [
    "start_date",
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
    'invoice_line_items': stripe.InvoiceLineItem,
    'transfers': stripe.Transfer,
    'coupons': stripe.Coupon,
    'subscriptions': stripe.Subscription,
    'subscription_items': stripe.SubscriptionItem,
    'balance_transactions': stripe.BalanceTransaction,
    'payouts': stripe.Payout
}

STREAM_REPLICATION_KEY = {
    'charges': 'created',
    'events': 'created',
    'customers': 'created',
    'plans': 'created',
    'invoices': 'date',
    'invoice_items': 'date',
    'transfers': 'created',
    'coupons': 'created',
    'subscriptions': 'created',
    'subscription_items': 'created',
    'balance_transactions': 'created',
    'payouts': 'created',
    # invoice_line_items is bookmarked based on parent invoices,
    # no replication key value on the object itself
    #'invoice_line_items': 'date'
}

STREAM_TO_TYPE_FILTER = {
    'charges': {'type': 'charge.*', 'object': 'charge'},
    'customers': {'type': 'customer.*', 'object': 'customer'},
    'plans': {'type': 'plan.*', 'object': 'plan'},
    'invoices': {'type': 'invoice.*', 'object': 'invoice'},
    'invoice_items': {'type': 'invoiceitem.*', 'object': 'invoiceitem'},
    'coupons': {'type': 'coupon.*', 'object': 'coupon'},
    'subscriptions': {'type': 'customer.subscription.*', 'object': 'subscription'},
    'payouts': {'type': 'payout.*', 'object': 'transfer'},
    'transfers': {'type': 'transfer.*', 'object': 'transfer'},
    # Cannot find evidence of these streams having events associated:
    # subscription_items - appears on subscriptions events
    # balance_transactions - seems to be immutable
    # payouts - these are called transfers with an event type of payout.*
}

SUB_STREAMS = {
    'subscriptions': 'subscription_items',
    'invoices': 'invoice_line_items'
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
        # Separate loops for formatting.
        for stream_name, stream_count in Context.new_counts.items():
            with metrics.record_counter(stream_name) as counter:
                updates_count = Context.updated_counts[stream_name]
                total_replicated = stream_count + updates_count
                counter.increment(total_replicated)

        LOGGER.info('------------------')
        for stream_name, stream_count in Context.new_counts.items():
            LOGGER.info('%s: %d new, %d updates',
                        stream_name,
                        stream_count,
                        Context.updated_counts[stream_name])
        LOGGER.info('------------------')

def apply_request_timer_to_client(client):
    """ Instruments the Stripe SDK client object with a request timer. """
    _original_request = client.request
    def wrapped_request(*args, **kwargs):
        url = args[1]
        match = re.match(r'http[s]?://api\.stripe\.com/v1/(\w+)\??', url)
        stream_name = match.groups()[0]
        with metrics.http_request_timer(stream_name):
            return _original_request(*args, **kwargs)
    client.request = wrapped_request

def configure_stripe_client():
    stripe.set_app_info(Context.config.get('user_agent', 'Singer.io Tap'),
                        url="https://github.com/singer-io/tap-stripe")
    # Set the API key we'll be using
    # https://github.com/stripe/stripe-python/tree/a9a8d754b73ad47bdece6ac4b4850822fa19db4e#usage
    stripe.api_key = Context.config.get('client_secret')
    # Override the Stripe API Version for consistent access
    stripe.api_version = '2018-09-24'
    # Allow ourselves to retry retriable network errors 5 times
    # https://github.com/stripe/stripe-python/tree/a9a8d754b73ad47bdece6ac4b4850822fa19db4e#configuring-automatic-retries
    stripe.max_network_retries = 15
    # Configure client-side network timeout of 1 second
    # https://github.com/stripe/stripe-python/tree/a9a8d754b73ad47bdece6ac4b4850822fa19db4e#configuring-a-client
    client = stripe.http_client.RequestsClient(timeout=15)
    apply_request_timer_to_client(client)
    stripe.default_http_client = client
    # Set stripe logging to INFO level
    # https://github.com/stripe/stripe-python/tree/a9a8d754b73ad47bdece6ac4b4850822fa19db4e#logging
    logging.getLogger('stripe').setLevel(logging.INFO)
    # Verify connectivity
    account = stripe.Account.retrieve(Context.config.get('account_id'))
    msg = "Successfully connected to Stripe Account with display name" \
          + " `%s`"
    LOGGER.info(msg, account.display_name)

def unwrap_data_objects(rec):
    """
    Looks for levels in the record that look like:

    {
        "url": ...,
        "object": ...,
        "data": {...}|[...]|...,
        ...
    }

    and recursively de-nests any that match by bringing the "data"
    value up to its parent's level.
    """
    # Return early if we got here with a list of strings, no denesting required
    if not isinstance(rec, dict):
        return rec

    for k, v in rec.items(): #pylint: disable=invalid-name
        if k == "data" and 'object' in rec and rec['object'] == 'list':
            if isinstance(v, dict):
                return unwrap_data_objects(v)
            if isinstance(v, list):
                return [unwrap_data_objects(o) for o in v]
            return v
        if isinstance(v, dict):
            rec[k] = unwrap_data_objects(v)
        if isinstance(v, list):
            rec[k] = [unwrap_data_objects(o) for o in rec[k]]
    return rec

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
                # throw error here
                errs.append(msg_tmpl.format(sub_stream_id, stream_id))

    if errs:
        raise DependencyException(" ".join(errs))


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_shared_schema_refs():
    shared_schemas_path = get_abs_path('schemas/shared')

    shared_file_names = [f for f in os.listdir(shared_schemas_path)
                         if os.path.isfile(os.path.join(shared_schemas_path, f))]

    shared_schema_refs = {}
    for shared_file in shared_file_names:
        with open(os.path.join(shared_schemas_path, shared_file)) as data_file:
            shared_schema_refs['shared/' + shared_file] = json.load(data_file)

    return shared_schema_refs

# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    schema_path = get_abs_path('schemas')
    files = [f for f in os.listdir(schema_path) if os.path.isfile(os.path.join(schema_path, f))]
    for filename in files:
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = {'path': filename, 'schema': json.load(file)}

    return schemas

def get_discovery_metadata(schema, key_property, replication_method, replication_key):
    mdata = metadata.new()
    mdata = metadata.write(mdata, (), 'table-key-properties', [key_property])
    mdata = metadata.write(mdata, (), 'forced-replication-method', replication_method)

    if replication_key:
        mdata = metadata.write(mdata, (), 'valid-replication-keys', [replication_key])

    for field_name in schema['properties'].keys():
        if field_name in [key_property, replication_key, "updated"]:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return metadata.to_list(mdata)


def discover():
    raw_schemas = load_schemas()
    streams = []

    for stream_name in STREAM_SDK_OBJECTS:
        schema = raw_schemas[stream_name]['schema']
        refs = load_shared_schema_refs()
        # create and add catalog entry
        catalog_entry = {
            'stream': stream_name,
            'tap_stream_id': stream_name,
            'schema': singer.resolve_schema_references(schema, refs),
            'metadata': get_discovery_metadata(schema,
                                               'id',
                                               'INCREMENTAL',
                                               STREAM_REPLICATION_KEY.get(stream_name)),
            # Events may have a different key property than this. Change
            # if it's appropriate.
            'key_properties': ['id']
        }
        streams.append(catalog_entry)

    return {'streams': streams}

def reduce_foreign_keys(rec, stream_name):
    if stream_name == 'customers':
        rec['subscriptions'] = [s['id'] for s in rec.get('subscriptions', [])] or None
    elif stream_name == 'subscriptions':
        rec['items'] = [i['id'] for i in rec.get('items', [])] or None
    elif stream_name == 'invoices':
        rec['lines'] = [l['id'] for l in rec.get('lines', [])] or None
    return rec

def sync_stream(stream_name):
    """
    Sync each stream, looking for newly created records. Updates are captured by events stream.
    """
    LOGGER.info("Started syncing stream %s", stream_name)

    stream_metadata = metadata.to_map(Context.get_catalog_entry(stream_name)['metadata'])
    extraction_time = singer.utils.now()
    replication_key = metadata.get(stream_metadata, (), 'valid-replication-keys')[0]
    # Invoice Items bookmarks on `date`, but queries on `created`
    filter_key = 'created' if stream_name == 'invoice_items' else replication_key
    stream_bookmark = singer.get_bookmark(Context.state, stream_name, replication_key)
    bookmark = stream_bookmark or \
               int(utils.strptime_to_utc(Context.config["start_date"]).timestamp())
    max_bookmark = bookmark
    # if this stream has a sub_stream, compare the bookmark
    sub_stream_name = SUB_STREAMS.get(stream_name)

    if sub_stream_name:
        sub_stream_bookmark = singer.get_bookmark(Context.state, sub_stream_name, replication_key)
        # if there is a sub stream, set bookmark to sub stream's bookmark
        # since we know it must be earlier than the stream's bookmark
        if sub_stream_bookmark and sub_stream_bookmark != stream_bookmark:
            bookmark = sub_stream_bookmark
    else:
        sub_stream_bookmark = None

    with Transformer(singer.UNIX_SECONDS_INTEGER_DATETIME_PARSING) as transformer:
        for stream_obj in STREAM_SDK_OBJECTS[stream_name].list(
                limit=100,
                stripe_account=Context.config.get('account_id'),
                # None passed to starting_after appears to retrieve
                # all of them so this should always be safe.
                **{filter_key + "[gte]": bookmark}
        ).auto_paging_iter():

            if sub_stream_name:
                sub_stream_bookmark = singer.get_bookmark(Context.state,
                                                          sub_stream_name,
                                                          replication_key)

            # If there is no sub stream, or there is and it isn't selected,
            # or the sub stream is up to date (bookmarks are equal),
            # the stream should be sync'd
            # Note: The parent stream is already checked if selected before we
            #       call this function
            should_sync_stream = not sub_stream_name \
                                 or not Context.is_selected(sub_stream_name) \
                                 or stream_bookmark == sub_stream_bookmark

            # if the bookmark equals the stream bookmark, sync stream records
            if should_sync_stream:
                rec = unwrap_data_objects(stream_obj.to_dict_recursive())
                rec = reduce_foreign_keys(rec, stream_name)
                rec["updated"] = rec[replication_key]
                rec = transformer.transform(rec,
                                            Context.get_catalog_entry(stream_name)['schema'],
                                            stream_metadata)

                singer.write_record(stream_name,
                                    rec,
                                    time_extracted=extraction_time)

                Context.new_counts[stream_name] += 1

                stream_bookmark = stream_obj.get(replication_key)

                if stream_bookmark > max_bookmark:
                    max_bookmark = stream_bookmark
                    singer.write_bookmark(Context.state,
                                          stream_name,
                                          replication_key,
                                          max_bookmark)

                # sync sub streams
                if sub_stream_name and Context.is_selected(sub_stream_name):
                    sync_sub_stream(sub_stream_name, stream_obj, replication_key)

            # write state after every 100 records
            if (Context.new_counts[stream_name] % 100) == 0:
                singer.write_state(Context.state)

    singer.write_state(Context.state)


def sync_sub_stream(sub_stream_name,
                    parent_obj,
                    parent_replication_key,
                    save_bookmarks=True,
                    updates=False):
    """
    Given a parent object, retrieve its values for the specified substream.
    """
    extraction_time = singer.utils.now()

    if sub_stream_name == "invoice_line_items":
        object_list = parent_obj.lines
    elif sub_stream_name == "subscription_items":
        # parent_obj.items is a function that returns a dict iterator, so use the attribute
        object_list = parent_obj.get("items")
    else:
        raise Exception("Attempted to sync substream that is not implemented: {}"
                        .format(sub_stream_name))

    with Transformer(singer.UNIX_SECONDS_INTEGER_DATETIME_PARSING) as transformer:
        iterator = object_list.auto_paging_iter() if object_list is not None else []
        for sub_stream_obj in iterator:
            obj_ad_dict = sub_stream_obj.to_dict_recursive()

            if sub_stream_name == "invoice_line_items":
                # Synthetic addition of a key to the record we sync
                obj_ad_dict["invoice"] = parent_obj.id

            rec = transformer.transform(unwrap_data_objects(obj_ad_dict),
                                        Context.get_catalog_entry(sub_stream_name)['schema'],
                                        metadata.to_map(
                                            Context.get_catalog_entry(sub_stream_name)['metadata']
                                        ))

            singer.write_record(sub_stream_name,
                                rec,
                                time_extracted=extraction_time)
            if updates:
                Context.updated_counts[sub_stream_name] += 1
            else:
                Context.new_counts[sub_stream_name] += 1

            sub_stream_bookmark = parent_obj.get(parent_replication_key)

            if save_bookmarks:
                singer.write_bookmark(Context.state,
                                      sub_stream_name,
                                      parent_replication_key,
                                      sub_stream_bookmark)


def should_sync_event(events_obj, object_type, id_to_created_map):
    """Checks to ensure the event's underlying object has an id and that the id_to_created_map
    contains an entry for that id. Returns true the first time an id should be added to the map
    and when we're looking at an event that is created later than one we've seen before."""
    event_resource_dict = events_obj.data.object.to_dict_recursive()
    event_resource_id = event_resource_dict.get('id')
    current_max_created = id_to_created_map.get(event_resource_id)
    event_created = events_obj.created

    # The event's object had no id so throw it out!
    if not event_resource_id or event_resource_dict.get('object') != object_type:
        return False

    # If the event is the most recent one we've seen, we should sync it
    should_sync = not current_max_created or event_created >= current_max_created
    if should_sync:
        id_to_created_map[event_resource_id] = events_obj.created
    return should_sync


def sync_event_updates(stream_name):
    '''
    Get updates via events endpoint

    look at 'events update' bookmark and pull events after that
    '''
    LOGGER.info("Started syncing event based updates")

    bookmark_value = singer.get_bookmark(Context.state,
                                         stream_name + '_events',
                                         'updates_created') or \
                     int(utils.strptime_to_utc(Context.config["start_date"]).timestamp())
    max_created = bookmark_value
    date_window_start = max_created
    date_window_end = max_created + 604800 # Number of seconds in a week

    stop_paging = False


    # Create a map to hold relate event object ids to timestamps
    updated_object_timestamps = {}

    while not stop_paging:
        extraction_time = singer.utils.now()

        response = STREAM_SDK_OBJECTS['events'].list(**{
            "limit": 100,
            "type": STREAM_TO_TYPE_FILTER[stream_name]['type'],
            "stripe_account" : Context.config.get('account_id'),
            # None passed to starting_after appears to retrieve
            # all of them so this should always be safe.
            "created[gte]": date_window_start,
            "created[lt]": date_window_end,
        })

        # If no results, and we are not up to current time
        if not len(response) and date_window_end > extraction_time.timestamp(): # pylint: disable=len-as-condition
            stop_paging = True

        for events_obj in response.auto_paging_iter():
            event_resource_obj = events_obj.data.object
            sub_stream_name = SUB_STREAMS.get(stream_name)

            # Check whether we should sync the event based on its created time
            if not should_sync_event(events_obj,
                                     STREAM_TO_TYPE_FILTER[stream_name]['object'],
                                     updated_object_timestamps):
                continue

            # Syncing an event as its the first time we've seen it or its the most recent version
            with Transformer(singer.UNIX_SECONDS_INTEGER_DATETIME_PARSING) as transformer:
                event_resource_metadata = metadata.to_map(
                    Context.get_catalog_entry(stream_name)['metadata']
                )
                rec = unwrap_data_objects(event_resource_obj.to_dict_recursive())
                rec = reduce_foreign_keys(rec, stream_name)
                rec["updated"] = events_obj.created
                rec = transformer.transform(
                    rec,
                    Context.get_catalog_entry(stream_name)['schema'],
                    event_resource_metadata
                )

                if events_obj.created >= bookmark_value:
                    if rec.get('id') is not None:
                        singer.write_record(stream_name,
                                            rec,
                                            time_extracted=extraction_time)
                        Context.updated_counts[stream_name] += 1

                        if sub_stream_name and Context.is_selected(sub_stream_name):
                            if event_resource_obj:
                                sync_sub_stream(sub_stream_name,
                                                event_resource_obj,
                                                STREAM_REPLICATION_KEY[stream_name],
                                                save_bookmarks=False,
                                                updates=True)
            if events_obj.created > max_created:
                max_created = events_obj.created

        date_window_start = date_window_end
        date_window_end = date_window_end + 604800
        singer.write_bookmark(Context.state,
                              stream_name + '_events',
                              'updates_created',
                              max_created)
        singer.write_state(Context.state)

    singer.write_state(Context.state)

def sync():
    # Write all schemas and init count to 0
    for catalog_entry in Context.catalog['streams']:
        stream_name = catalog_entry["tap_stream_id"]
        if Context.is_selected(stream_name):
            singer.write_schema(stream_name, catalog_entry['schema'], 'id')

            Context.new_counts[stream_name] = 0
            Context.updated_counts[stream_name] = 0

    # Loop over streams in catalog
    for catalog_entry in Context.catalog['streams']:
        stream_name = catalog_entry['tap_stream_id']
        # Sync records for stream
        if Context.is_selected(stream_name) and not Context.is_sub_stream(stream_name):
            sync_stream(stream_name)
            # This prevents us from retrieving 'events.events'
            if STREAM_TO_TYPE_FILTER.get(stream_name):
                sync_event_updates(stream_name)

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
        try:
            sync()
        finally:
            # Print counts
            Context.print_counts()



if __name__ == "__main__":
    main()
