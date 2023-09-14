"""
Setup expectations for test sub classes
Run discovery for as a prerequisite for most tests
"""
import os
import json
import decimal
from datetime import datetime as dt
from datetime import timezone as tz
from dateutil import parser

from tap_tester import connections, menagerie, runner, LOGGER
from tap_tester.base_case import BaseCase
from tap_tester.jira_client import JiraClient as jira_client
from tap_tester.jira_client import CONFIGURATION_ENVIRONMENT as jira_config

JIRA_CLIENT = jira_client({ **jira_config })


class BaseTapTest(BaseCase):
    """
    Setup expectations for test sub classes
    Run discovery for as a prerequisite for most tests
    """

    AUTOMATIC_FIELDS = "automatic"
    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    API_LIMIT = 100
    INCREMENTAL = "INCREMENTAL"
    FULL = "FULL_TABLE"
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    TS_COMPARISON_FORMAT = "%Y-%m-%dT%H:%M:%S.000000Z"

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-stripe"

    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.stripe"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""

        return_value = {
            'start_date': dt.strftime(dt.today(), self.START_DATE_FORMAT),
            'account_id': os.getenv('TAP_STRIPE_ACCOUNT_ID')
        }

        if original:
            return return_value

        # Start Date test needs the new connections start date to be prior to the default
        assert self.start_date < return_value["start_date"]

        # Assign start date to be the default
        return_value["start_date"] = self.start_date
        return return_value

    @staticmethod
    def get_credentials():
        """Authentication information for the test account"""
        return {
            'client_secret': os.getenv('TAP_STRIPE_CLIENT_SECRET')
        }

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""

        default = {
            self.AUTOMATIC_FIELDS: {"updated"},
            self.REPLICATION_KEYS: {"created"},
            self.PRIMARY_KEYS: {"id"},
            self.REPLICATION_METHOD: self.INCREMENTAL,
        }

        return {
            'charges': default,
            'events': default,
            'customers': default,
            'plans': default,
            'payment_intents': default,
            'invoices': default,
            'invoice_items': {
                self.AUTOMATIC_FIELDS: {"updated"},
                self.REPLICATION_KEYS: {"date"},
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'invoice_line_items': {
                self.PRIMARY_KEYS: {"id", "invoice"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: None,
                self.AUTOMATIC_FIELDS: None
            },
            'transfers': default,
            'coupons': default,
            'subscriptions': default,
            'subscription_items': {
                self.AUTOMATIC_FIELDS: None,
                self.REPLICATION_KEYS: {"created"},
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'balance_transactions': default,
            'payouts': default,
            'payout_transactions' : {
                self.AUTOMATIC_FIELDS: {"id"},
                self.REPLICATION_KEYS: {"id"},
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL
            },
            'disputes': default,
            'products': default,
        }

    def expected_streams(self):
        """A set of expected stream names"""
        return set(self.expected_metadata().keys())

    def child_streams(self):
        """
        Return a set of streams that are child streams
        based on having foreign key metadata
        """
        return {stream for stream in self.expected_metadata()
                if stream in ["invoice_line_items", "subscription_items"]}

    def expected_primary_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of primary key fields
        """
        return {table: properties.get(self.PRIMARY_KEYS) or set()
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of replication key fields
        """
        return {table: properties.get(self.REPLICATION_KEYS, None)
                for table, properties
                in self.expected_metadata().items()}

    def expected_foreign_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of foreign key fields
        """
        return {}

    def expected_automatic_fields(self):
        non_key_automatic_fields = {table: properties.get(self.AUTOMATIC_FIELDS) or set()
                                    for table, properties
                                    in self.expected_metadata().items()}
        return {table: ((non_key_automatic_fields.get(table) or set()) |
                        (self.expected_primary_keys().get(table) or set()) |
                        (self.expected_replication_keys().get(table) or set()) |
                        (self.expected_foreign_keys().get(table) or set()))
                for table in self.expected_metadata()}

    def expected_replication_method(self):
        """return a dictionary with key of table name nd value of replication method"""
        return {table: properties.get(self.REPLICATION_METHOD, None)
                for table, properties
                in self.expected_metadata().items()}

    def expected_incremental_streams(self):
        return set(stream for stream, rep_meth in
                   self.expected_replication_method().items()
                   if rep_meth == self.INCREMENTAL)

    def expected_full_table_streams(self):
        return set(stream for stream, rep_meth in
                   self.expected_replication_method().items()
                   if rep_meth == self.FULL)

    def setUp(self):
        """Verify that you have set the prerequisites to run the tap (creds, etc.)"""
        env_keys = {'TAP_STRIPE_CLIENT_SECRET'}
        missing_envs = [x for x in env_keys if os.getenv(x) is None]
        if missing_envs:
            raise Exception("Set environment variables: {}".format(missing_envs))

    #########################
    #   Helper Methods      #
    #########################

    @staticmethod
    def local_to_utc(date: dt):
        """Convert a datetime with timezone information to utc"""
        utc = dt(date.year, date.month, date.day, date.hour, date.minute,
                 date.second, date.microsecond, tz.utc)

        if date.tzinfo and hasattr(date.tzinfo, "_offset"):
            utc += date.tzinfo._offset

        return utc

    def max_bookmarks_by_stream(self, sync_records):
        """
        Return the maximum value for the replication key for the events stream
        which is the bookmark expected value for updated records.

        Comparisons are based on the class of the bookmark value. Dates will be
        string compared which works for ISO date-time strings.
        """
        max_bookmarks = {}

        for stream, batch in sync_records.items():
            upsert_messages = [m for m in batch.get('messages') if m['action'] == 'upsert']

            stream_bookmark_key = self.expected_replication_keys().get(stream) or set()
            assert not stream_bookmark_key or len(stream_bookmark_key) == 1  # There shouldn't be a compound replication key
            if not stream_bookmark_key:
                continue
            stream_bookmark_key = stream_bookmark_key.pop()

            bk_values = [message["data"].get(stream_bookmark_key) for message in upsert_messages]
            max_bookmarks[stream] = {stream_bookmark_key: None}
            for bk_value in bk_values:
                if bk_value is None:
                    continue

                if max_bookmarks[stream][stream_bookmark_key] is None:
                    max_bookmarks[stream][stream_bookmark_key] = bk_value

                if bk_value > max_bookmarks[stream][stream_bookmark_key]:
                    max_bookmarks[stream][stream_bookmark_key] = bk_value
        return max_bookmarks


    def max_events_bookmarks_by_stream(self, sync_records):
        """
        Return the maximum value for the replication key for the events stream
        which is the bookmark expected value for updated records.

        Comparisons are based on the class of the bookmark value. Dates will be
        string compared which works for ISO date-time strings.
        """
        max_bookmarks = {}

        events = {stream: batch for stream, batch in sync_records.items() if stream == "events"}
        for s in sync_records.keys():

            if s != 'events':
                type_name = s[:-1]
                if '_' in type_name:
                    type_name = type_name.replace('_', '')

                if not events:
                    return None
                upsert_messages = [m for m in events.get('events').get('messages')
                                   if m['action'] == 'upsert' and type_name in m['data']['type'] ]

                stream_bookmark_key = 'updated'
                bk_values = [message["data"].get(stream_bookmark_key) for message in upsert_messages]
                current = s + "_events"
                max_bookmarks[current] = {stream_bookmark_key: None}
                for bk_value in bk_values:
                    if bk_value is None:
                        continue

                    if max_bookmarks[current][stream_bookmark_key] is None:
                        max_bookmarks[current][stream_bookmark_key] = bk_value

                    if bk_value > max_bookmarks[current][stream_bookmark_key]:
                        max_bookmarks[current][stream_bookmark_key] = bk_value

        return max_bookmarks

    def min_bookmarks_by_stream(self, sync_records):
        """
        Return the minimum value for the replication key for each stream
        """
        min_bookmarks = {}
        for stream, batch in sync_records.items():
            upsert_messages = [m for m in batch.get('messages') if m['action'] == 'upsert']

            stream_bookmark_key = self.expected_replication_keys().get(stream) or set()
            assert not stream_bookmark_key or len(stream_bookmark_key) == 1  # There shouldn't be a compound replication key
            if not stream_bookmark_key:
                continue
            stream_bookmark_key = stream_bookmark_key.pop()

            bk_values = [message["data"].get(stream_bookmark_key) for message in upsert_messages]
            min_bookmarks[stream] = {stream_bookmark_key: None}
            for bk_value in bk_values:
                if bk_value is None:
                    continue

                if min_bookmarks[stream][stream_bookmark_key] is None:
                    min_bookmarks[stream][stream_bookmark_key] = bk_value

                if bk_value < min_bookmarks[stream][stream_bookmark_key]:
                    min_bookmarks[stream][stream_bookmark_key] = bk_value
        LOGGER.info(min_bookmarks)
        return min_bookmarks

    def split_records_into_created_and_updated(self, records):
        created = {}
        updated = {}
        current_state = menagerie.get_state(self.conn_id)

        for stream, batch in records.items():
            # Get key from state since rep key found in stripe docs & base.py may not match state
            if current_state.get('bookmarks', {stream: None}).get(stream) and stream != 'invoices':
                bookmark_state_items = list(current_state['bookmarks'][stream].items())
                assert len(bookmark_state_items) <= 1, f"Unexpected compound bookmark_key " \
                    f"detected: {bookmark_state_items}"
                bookmark_key, bookmark_value = bookmark_state_items[0]
                assert bookmark_key is not None
            else:
                if stream == 'invoices':
                    LOGGER.info("Replicaiton key in state is 'date'.  Key in stripe documentation "
                                "and base.py is 'created'. Not all invoice records have a 'date' "
                                f"key defined so over-ride state for stream: {stream} to allow the "
                                "split method to work as intended. See JIRA BUG TDL-21614")
                else:
                    # This may not work for streams where the replication key and state key are different
                    LOGGER.warn("Failed to get replication key from state, using expected replication "
                                "key from base.py instead. If key in base does not match key in the "
                                f"tap then the split method may fail for this stream: {stream}")

                bookmark_key = self.expected_replication_keys().get(stream, set())
                assert len(bookmark_key) <= 1
                bookmark_key = bookmark_key.pop() if bookmark_key else None

            if stream not in created:
                created[stream] = {'messages': [],
                                   'schema': batch['schema'],
                                   'key_names' : batch.get('key_names'),
                                   'table_version': batch.get('table_version')}
            # add the records which are created in the created dictionary
            created[stream]['messages'] += [m for m in batch['messages']
                                                if m['data'].get("updated") == m['data'].get(bookmark_key)]

            if stream not in updated:
                updated[stream] = {'messages': [],
                                   'schema': batch['schema'],
                                   'key_names' : batch.get('key_names'),
                                   'table_version': batch.get('table_version')}
            # add the records which are updated in the updated dictionary
            updated[stream]['messages'] += [m for m in batch['messages']
                                                if m['data'].get("updated") != m['data'].get(bookmark_key)]
        return created, updated

    def select_all_streams_and_fields(self, conn_id, catalogs, select_all_fields: bool = True, exclude_streams=None):
        """Select all streams and all fields within streams"""

        for catalog in catalogs:
            if exclude_streams and catalog.get('stream_name') in exclude_streams:
                continue
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {})
                # remove properties that are automatic
                for prop in self.expected_automatic_fields().get(catalog['stream_name'], []):
                    if prop in non_selected_properties:
                        del non_selected_properties[prop]
                non_selected_properties = non_selected_properties.keys()
            additional_md = []

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, additional_md=additional_md,
                non_selected_fields=non_selected_properties
            )

    @staticmethod
    def get_selected_fields_from_metadata(metadata):
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field['breadcrumb']) > 1
            inclusion_automatic_or_selected = (field['metadata']['inclusion'] == 'automatic'
                                               or field['metadata']['selected'] is True)
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field['breadcrumb'][1])
        return selected_fields

    def records_data_type_conversions(self, records):

        converted_records = []
        for record in records:
            converted_record = dict(record)

            # Known keys with data types that must be converted to compare with
            # jsonified records emitted by the tap
            timestamp_to_datetime_keys = [
                'created', 'updated', 'start', 'end', 'start_date', 'end_date', 'paid_at',
                'next_payment_attempt', 'finalized_at', 'current_period_start', 'current_period_end',
                'billing_cycle_anchor', 'arrival_date', 'period_end', 'period_start', 'date',
                'webhooks_delivered_at',
            ]
            int_or_float_to_decimal_keys = [
                'percent_off', 'percent_off_precise', 'height', 'length', 'weight', 'width'
            ]

            object_keys = [
                'charges', 'coupon', 'discount', 'package_dimensions', 'period', 'plan', 'price',
                'refunds', 'source', 'sources', 'status_transitions'
                # Convert epoch timestamp value of 'price.created' to standard datetime format.
                # This field is available specific for invoice_line_items stream
            ]

            # timestamp to datetime
            for key in timestamp_to_datetime_keys:
                if record.get(key, False):
                    converted_record[key] = dt.strftime(
                        dt.fromtimestamp(record[key]), self.TS_COMPARISON_FORMAT
                    )

            # int/float to decimal
            for key in int_or_float_to_decimal_keys:
                if record.get(key, False):
                    str_value = str(record.get(key))  # does float -> float or int -> float
                    converted_record[key] = decimal.Decimal(str_value)

            # object field requires recursive check of subfields
            for key in object_keys:
                if record.get(key, False):
                    field_object = record[key]

                    if isinstance(field_object, dict):
                        converted_record[key] = self.records_data_type_conversions([field_object])[0]

                    elif isinstance(field_object, list):
                        converted_record[key] = self.records_data_type_conversions(field_object)

            converted_records.append(converted_record)

        return converted_records

    def run_and_verify_check_mode(self, conn_id):
        """
        Run the tap in check mode and verify it succeeds.
        This should be ran prior to field selection and initial sync.

        Return the connection id and found catalogs from menagerie.
        """
        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))
        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        diff = self.expected_streams().symmetric_difference(found_catalog_names)
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))
        LOGGER.info("discovered schemas are OK")

        return found_catalogs

    def run_and_verify_sync(self, conn_id, clear_state=False):
        """
        Clear the connections state in menagerie and Run a Sync.
        Verify the exit code following the sync.

        Return the connection id and record count by stream
        """
        if clear_state:
            #clear state
            menagerie.set_state(conn_id, {})

        # run sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # read target output
        record_count_by_stream = runner.examine_target_output_file(self, conn_id,
                                                                   self.expected_streams(),
                                                                   self.expected_primary_keys())

        return record_count_by_stream

    def perform_and_verify_table_and_field_selection(self, conn_id, found_catalogs, streams_to_select, select_all_fields=True):
        """
        Perform table and field selection based off of the streams to select set and field selection parameters.
        Verfify this results in the expected streams selected and all or no fields selected for those streams.
        """
        # Select all available fields or select no fields from all testable streams
        exclude_streams = self.expected_streams().difference(streams_to_select)
        self.select_all_streams_and_fields(
            conn_id=conn_id, catalogs=found_catalogs, select_all_fields=select_all_fields, exclude_streams=exclude_streams
        )

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection worked
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])
            # Verify all testable streams are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            LOGGER.info("Validating selection on %s: %s", cat['stream_name'], selected)
            if cat['stream_name'] not in streams_to_select:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                for field, field_props in catalog_entry.get('annotated-schema').get('properties').items():
                    field_selected = field_props.get('selected')
                    LOGGER.info("\tValidating selection on %s.%s: %s", cat['stream_name'], field, field_selected)
                    self.assertTrue(field_selected, msg="Field not selected.")
            else:
                # Verify only automatic fields are selected
                expected_automatic_fields = self.expected_automatic_fields().get(cat['tap_stream_id'])
                selected_fields = self.get_selected_fields_from_metadata(catalog_entry['metadata'])
                self.assertEqual(expected_automatic_fields, selected_fields)

    def expected_schema_keys(self, stream):
        props = self._load_schemas(stream).get(stream).get('properties')
        if not props:
            props = self._load_schemas(stream, shared=True).get(stream).get('properties')

        assert props, "schema not configured proprerly"

        return props.keys()

    @staticmethod
    def _get_abs_path(path):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

    def _load_schemas(self, stream, shared: bool = False):
        schemas = {}

        file_name = "shared/" + stream[:-1] + ".json" if shared else stream + ".json"
        path = self._get_abs_path("schemas") + "/" + file_name
        final_path = path.replace('tests', self.tap_name().replace('-', '_'))

        with open(final_path) as file:
                          schemas[stream] = json.load(file)

        return schemas

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_date = self.get_properties().get('start_date')
        self.maxDiff=None

    def dt_to_ts(self, dtime):
        return parser.parse(dtime).timestamp()

    def skipUntilDone(jira_ticket):
        def wrap(test_method):
            # statusCategory keys https://jira.talendforge.org/rest/api/2/statuscategory/
            is_done = JIRA_CLIENT.get_status_category(jira_ticket) == "done"
            return BaseCase.skipUnless(is_done, jira_ticket)(test_method)

        return wrap
