#!/usr/bin/env python3
import os
import json
import logging

import stripe
import singer
from singer import utils, Transformer
from singer.transform import unix_seconds_to_datetime

REQUIRED_CONFIG_KEYS = [
    "account_id",
    "client_secret"
]
STREAM_SDK_OBJECTS = {
    'charges': stripe.Charge,
    'events': stripe.Event,
    'customers': stripe.Customer
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
        return cls.stream_map[stream_name]

    @classmethod
    def get_schema(cls, stream_name):
        stream = [s for s in cls.catalog["streams"] if s["tap_stream_id"] == stream_name][0]
        return stream["schema"]


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

def discover():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():
        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata': [],
            # Events may have a different key property than this. Change
            # if it's appropriate.
            'key_properties': ['id']
        }
        streams.append(catalog_entry)

    return {'streams': streams}



class Stream():
    name = None
    key_properties = 'id'
    schema = None

    def __init__(self, name, schema):
        self.name = name
        self.schema = schema

    def sync(self):
        count = 0
        for stream_obj in STREAM_SDK_OBJECTS[self.name].list(
                # If we want to increase the page size we can do
                # `limit=N` as a second parameter here.
                stripe_account=Context.config.get('account_id'),
                # None passed to starting_after appears to retrieve
                # all of them so this should always be safe.
                starting_after=singer.get_bookmark(Context.state, self.name, 'id')
            ).auto_paging_iter():
            count += 1
            yield (self.name, stream_obj)

            singer.write_bookmark(Context.state,
                                  self.name,
                                  self.key_properties,
                                  stream_obj.id)
            singer.write_state(Context.state)
        LOGGER.info("Sync'd %d records for %s", count, self.name)

class Events():
    name = None
    key_properties = 'id'
    schema = None

    def __init__(self, name, schema):
        self.name = name
        self.schema = schema

    def sync(self):
        count = 0
        for event_obj in STREAM_SDK_OBJECTS[self.name].list(
                # If we want to increase the page size we can do
                # `limit=N` as a second parameter here.
                stripe_account=Context.config.get('account_id'),
                # None passed to starting_after appears to retrieve
                # all of them so this should always be safe.
                starting_after=singer.get_bookmark(Context.state, self.name, 'id')
            ).auto_paging_iter():

            event_obj_dict  = event_obj.to_dict_recursive()
            yield (self.name, event_obj_dict)
            count += 1

            event_resource = event_obj.data.object
            event_resource_name = event_resource.object + 's'
            if STREAMS.get(event_resource_name):
                event_resource_dict = event_resource.to_dict_recursive()
                yield (event_resource_name, event_resource_dict)

            singer.write_bookmark(Context.state,
                                  self.name,
                                  self.key_properties,
                                  event_obj.id)
            singer.write_state(Context.state)
        LOGGER.info("Sync'd %d records for %s", count, self.name)

STREAMS = {
    "charges": Stream,
    "events": Events,
    "customers": Stream
}


def sync():
    # Loop over streams in catalog
    for catalog_entry in Context.catalog['streams']:
        stream_id = catalog_entry['tap_stream_id']
        stream_schema = catalog_entry['schema']
        stream = STREAMS[stream_id](stream_id, stream_schema)

        singer.write_schema(stream.name,
                            stream.schema,
                            stream.key_properties)

        for (tap_stream_id, rec) in stream.sync():
            with Transformer(singer.UNIX_SECONDS_INTEGER_DATETIME_PARSING) as transformer:
                extraction_time = singer.utils.now()
                record_stream = Context.get_catalog_entry(tap_stream_id)
                record_schema = record_stream['schema']

                rec = transformer.transform(rec, record_schema, {})
                #LOGGER.info('-------------')
                #LOGGER.info('Stream Name: ' + tap_stream_id)
                #LOGGER.info('Created: ' + rec['created'])
                #if tap_stream_id == 'events':
                #    LOGGER.info('Type: ' + rec['type'])
                singer.write_record(tap_stream_id,
                                    rec,
                                    time_extracted=extraction_time)


@utils.handle_top_exception(LOGGER)
def main():

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

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

    catalog = discover()


    Context.config = args.config
    Context.state = args.state
    Context.catalog = catalog


    sync()

if __name__ == "__main__":
    main()
