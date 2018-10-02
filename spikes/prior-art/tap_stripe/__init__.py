#!/usr/bin/env python3
import os
import json
import singer
from singer import utils
from singer import metrics
from singer import bookmarks
from singer import metadata
from singer import (transform,
                    Transformer)
import stripe
import json

REQUIRED_CONFIG_KEYS = ["start_date", "access_token"]
LOGGER = singer.get_logger()
STREAM_ENDPOINTS = {
    'customers': stripe.Customer,
    'charges': stripe.Charge,
    'invoices': stripe.Invoice,
    'subscriptions': stripe.Subscription,
    'plans': stripe.Plan
}

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    schemas_path = get_abs_path('schemas')
    files = [f for f in os.listdir(schemas_path) if os.path.isfile(os.path.join(schemas_path, f))]

    for filename in files:
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas


def load_shared_schema_refs():
    shared_schemas_path = get_abs_path('schemas/shared')

    shared_file_names = [f for f in os.listdir(shared_schemas_path)
                         if os.path.isfile(os.path.join(shared_schemas_path, f))]

    shared_schema_refs = {}
    for shared_file in shared_file_names:
        with open(os.path.join(shared_schemas_path, shared_file)) as data_file:
            shared_schema_refs[shared_file] = json.load(data_file)

    return shared_schema_refs


def generate_metadata(schema):
    mdata = metadata.new()

    mdata = metadata.write(mdata, (), 'table-key-properties', ['id'])
    for field_name, props in schema['properties'].items():
        mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')

    return metadata.to_list(mdata)


def discover():
    raw_schemas = load_schemas()
    streams = []

    refs = load_shared_schema_refs()

    for schema_name, schema in raw_schemas.items():
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': singer.resolve_schema_references(schema, refs=refs),
            'metadata' : generate_metadata(schema),
            'key_properties': ['id']
        }
        streams.append(catalog_entry)

    return {'streams': streams}


def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected' first (legacy)
    and then checks metadata (current), looking for an empty breadcrumb
    and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog.streams:
        stream_metadata = stream.metadata
        if stream.is_selected():
            selected_streams.append(stream.tap_stream_id)
        else:
            for entry in stream_metadata:
                # stream metadata will have empty breadcrumb
                if not entry['breadcrumb'] and entry['metadata'].get('selected',None):
                    selected_streams.append(stream.tap_stream_id)

    return selected_streams


def replace_data_array(obj):
    for key, value in obj.items():
        if isinstance(value, dict) and value.get('object') == 'list':
            result = []
            for nested in value.auto_paging_iter():
                replace_data_array(nested)
                result.append(nested)
            obj[key] = result


def sync_stream(stream, schema, **params):
    singer.write_schema(stream, schema.to_dict(), ['id'])
    endpoint = STREAM_ENDPOINTS[stream]
    starting_after = None
    has_more = True
    while has_more:
        LOGGER.info('Loading data for stream {} after {}'.format(stream, starting_after))
        result = endpoint.list(
            starting_after=starting_after,
            limit=100,
            **params
        )
        has_more = result['has_more']
        if has_more:
            starting_after = result.data[-1].id
        for obj in result.data:
            replace_data_array(obj)

            with Transformer(singer.UNIX_SECONDS_INTEGER_DATETIME_PARSING) as transformer:
                obj = transformer.transform(obj, schema.to_dict())
                singer.write_record(stream, obj)




def sync(config, state, catalog):

    selected_stream_ids = get_selected_streams(catalog)

    # Loop over streams in catalog
    for stream in catalog.streams:
        stream_id = stream.tap_stream_id
        stream_schema = stream.schema
        if stream_id in selected_stream_ids:
            LOGGER.info('Syncing stream: ' + stream_id)
            if stream_id == 'subscriptions':
                sync_stream(stream_id, stream_schema, status='all')
            else:
                sync_stream(stream_id, stream_schema)

    return

@utils.handle_top_exception(LOGGER)
def main():

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    stripe.api_key = args.config['access_token']
    stripe.default_http_client = stripe.http_client.RequestsClient()

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:

        # 'properties' is the legacy name of the catalog
        if args.properties:
            catalog = args.properties
        # 'catalog' is the current name
        elif args.catalog:
            catalog = args.catalog
        else:
            catalog =  discover()

        sync(args.config, args.state, catalog)

if __name__ == "__main__":
    main()
