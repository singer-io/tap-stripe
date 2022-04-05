"""
Test tap sets a lookback window and evaluates start time properly
"""
import os
from datetime import datetime as dt
from dateutil.parser import parse

from tap_tester import menagerie, runner, connections
from base import BaseTapTest


class LookbackWindow(BaseTapTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream"""

    @staticmethod
    def name():
        return "tt_stripe_lookback_window"

    def parse_bookmark_to_date(self, value):
        if value:
            if isinstance(value, str):
                return self.local_to_utc(parse(value))
            if isinstance(value, int):
                return self.local_to_utc(dt.utcfromtimestamp(value))
        return value

    def get_properties(self):  # pylint: disable=arguments-differ
        return_value = {
            'start_date': dt.strftime(dt.today(), self.START_DATE_FORMAT),
            'lookback_window': 2*24*60*60,
            'account_id': os.getenv('TAP_STRIPE_ACCOUNT_ID'),
            'client_secret': os.getenv('TAP_STRIPE_CLIENT_SECRET')
        }
        return return_value

    def expected_sync_streams(self):
        return {
            'events',
            'balance_transactions'
        }

    def test_run(self):
        self.lookback_window = 2*24*60*60
        # Instantiate connection with default start
        conn_id = connections.ensure_connection(self)

        # run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select all testable streams and all fields within streams
        streams_to_select = self.expected_sync_streams()
        our_catalogs = [catalog for catalog in found_catalogs
                        if catalog.get('tap_stream_id') in
                        streams_to_select]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)

        bookmark = int(dt.today().timestamp()) - 1*24*60*60
        new_state = {'bookmarks': dict()}
        state = {'events': {'created': bookmark}, 'balance_transactions': {'created': bookmark + 60}}
        for stream in self.expected_sync_streams():
            new_state['bookmarks'][stream] = state[stream]
        menagerie.set_state(conn_id, new_state)

        # Run a sync job using orchestrator
        now_time = int(dt.today().timestamp())
        sync_record_count = self.run_and_verify_sync(conn_id)

        # Get the set of records from the sync
        sync_records = runner.get_records_from_target_output()

        for stream in self.expected_sync_streams():
            sync_data = [record.get("data") for record
                                    in sync_records.get(stream, {}).get("messages", [])]
            # check for the record if it is between evaluated start date and bookmark
            is_between = False
            for record in sync_data:
                date_value = record["updated"]
                self.assertGreaterEqual(date_value, dt.strftime(dt.fromtimestamp(now_time - self.lookback_window), self.TS_COMPARISON_FORMAT),
                                        msg="A sync record has a replication-key that is less than the today - lookback.")
                if dt.strftime(dt.fromtimestamp(now_time - self.lookback_window), self.TS_COMPARISON_FORMAT) <= date_value < dt.strftime(dt.fromtimestamp(new_state['bookmarks'][stream]['created']), self.TS_COMPARISON_FORMAT):
                    is_between = True

            self.assertTrue(is_between, msg='No record found between evaluated start time and bookmark')