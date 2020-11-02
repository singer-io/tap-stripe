"""
Setup expectations for test sub classes
Run discovery for as a prerequisite for most tests
"""
import unittest
import os
from datetime import datetime as dt
from datetime import timezone as tz
from singer import utils

from tap_tester import connections, menagerie, runner


class BaseTapTest(unittest.TestCase):
    """
    Setup expectations for test sub classes
    Run discovery for as a prerequisite for most tests
    """

    AUTOMATIC_FIELDS = "automatic"
    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    API_LIMIT = 100 # "max-row-limit"
    INCREMENTAL = "INCREMENTAL"
    FULL = "FULL_TABLE"
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"

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
            'account_id': os.getenv('TAP_STRIPE_ACCOUNT_ID'),
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

        date_rep_key = default.copy()
        date_rep_key.update({self.REPLICATION_KEYS: {"date"}})

        child_stream_default = dict(default)
        child_stream_default.update({self.AUTOMATIC_FIELDS: None})

        # invoice_line_items have a composite pk
        null_replication_key_child = child_stream_default.copy()
        null_replication_key_child.update({self.REPLICATION_KEYS: None})
        null_replication_key_child.update({self.PRIMARY_KEYS: {"id", "invoice"}})

        payout_transactions_metadata = {
            self.AUTOMATIC_FIELDS: {"id"},
            self.REPLICATION_KEYS: {"id"},
            self.PRIMARY_KEYS: {"id"},
            self.REPLICATION_METHOD: self.INCREMENTAL
        }

        return {
            'charges': default,
            'events': default,
            'customers': default,
            'plans': default,
            'invoices': date_rep_key,
            'invoice_items': date_rep_key,
            'invoice_line_items': null_replication_key_child,
            'transfers': default,
            'coupons': default,
            'subscriptions': default,
            'subscription_items': child_stream_default,
            'balance_transactions': default,
            'payouts': default,
            'payout_transactions' : payout_transactions_metadata,
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

    def setUp(self):
        """Verify that you have set the prerequisites to run the tap (creds, etc.)"""
        env_keys = {'TAP_STRIPE_CLIENT_SECRET'}
        missing_envs = [x for x in env_keys if os.getenv(x) is None]
        if missing_envs:
            raise Exception("Set environment variables: {}".format(missing_envs))

    #########################
    #   Helper Methods      #
    #########################

    def create_connection(self, original_properties: bool = True):
        """Create a new connection with the test name"""
        # Create the connection
        conn_id = connections.ensure_connection(self, original_properties)

        # Run a check job using orchestrator (discovery)
        check_job_name = runner.run_check_mode(self, conn_id)

        # Assert that the check job succeeded
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)
        return conn_id

    def run_sync(self, conn_id):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced
        and values of records synced for each stream
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())
        return sync_record_count

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
        print(min_bookmarks)
        return min_bookmarks

    def split_records_into_created_and_updated(self, records):
        created = {}
        updated = {}
        for stream, batch in records.items():
            bookmark_key = self.expected_replication_keys().get(stream, set())
            bookmark_key = bookmark_key.pop() if bookmark_key else None
            if stream not in created:
                created[stream] = {'messages': [],
                                   'schema': batch['schema'],
                                   'key_names' : batch.get('key_names'),
                                   'table_version': batch.get('table_version')}
            created[stream]['messages'] += [m for m in batch['messages']
                                                if m['data'].get("updated") == m['data'].get(bookmark_key)]
            if stream not in updated:
                updated[stream] = {'messages': [],
                                   'schema': batch['schema'],
                                   'key_names' : batch.get('key_names'),
                                   'table_version': batch.get('table_version')}
            updated[stream]['messages'] += [m for m in batch['messages']
                                                if m['data'].get("updated") != m['data'].get(bookmark_key)]
        return created, updated

    @staticmethod
    def select_all_streams_and_fields(conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {}).keys()

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, non_selected_fields=non_selected_properties)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_date = self.get_properties().get('start_date')

        # self.start_date = dt.strftime(dt.today(), "%Y-%m-%dT00:00:00Z")
