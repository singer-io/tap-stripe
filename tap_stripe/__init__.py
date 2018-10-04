#!/usr/bin/env python3
import os
import json
import stripe
import singer
from singer import utils

REQUIRED_CONFIG_KEYS = [
    "account_id",
    "client_secret",
    "start_date",
]
STREAM_IDS = [
    'charges',
]
STREAM_SDK_OBJECTS = {
    'charges': stripe.Charge,
}
LOGGER = singer.get_logger()

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
            'key_properties': ['id']
        }
        streams.append(catalog_entry)

    return {'streams': streams}

def sync(config, catalog):

    # Loop over streams in catalog
    for stream in catalog['streams']:
        stream_id = stream['tap_stream_id']
        stream_schema = stream['schema']
        stream_key_properties = stream['key_properties']
        if stream_id in STREAM_IDS:
            LOGGER.info('Syncing stream: %s', stream_id)
            singer.write_schema(stream_id,
                                stream_schema,
                                stream_key_properties)
            for obj in STREAM_SDK_OBJECTS[stream_id].list(
                    stripe_account=config.get(
                        'account_id')).auto_paging_iter():
                singer.write_record(stream_id, obj)

@utils.handle_top_exception(LOGGER)
def main():

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    stripe.api_key = args.config.get('client_secret')
    account = stripe.Account.retrieve(args.config.get('account_id'))
    msg = "Successfully connected to Stripe Account with display name" \
        + " `%s`"
    LOGGER.info(msg, account.display_name)

    catalog = discover()

    sync(args.config, catalog)

if __name__ == "__main__":
    main()
