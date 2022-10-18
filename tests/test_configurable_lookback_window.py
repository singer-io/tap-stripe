"""Test tap configurable properties. Specifically the lookback_window"""
import re
import os
from datetime import datetime as dt
from datetime import timedelta

from tap_tester import menagerie, connections, runner, LOGGER

from base import BaseTapTest


class ConversionWindowBaseTest(BaseTapTest):
    """
    Test tap's sync mode can execute with valid lookback_window values set.
    Validate setting the lookback_window configurable property.
    Test Cases:
    Verify connection can be created, and tap can discover and sync with a lookback window
    when passed in config else takes default value.
    """
    lookback_window = '600' # default value

    @staticmethod
    def name():
        return "tt_stripe_lookback_window_600"

    def get_properties(self):
        """Configurable properties, with a switch to override the 'start_date' property"""
        return_value = {
            'start_date':dt.strftime(dt.utcnow(), self.START_DATE_FORMAT),
            'lookback_window': self.lookback_window,
            'account_id': os.getenv('TAP_STRIPE_ACCOUNT_ID'),
            'client_secret': os.getenv('TAP_STRIPE_CLIENT_SECRET')
        }
        return return_value

    def run_test(self):
        """
        Testing that basic sync functions without Critical Errors when
        a valid lookback_window is set.
        """
        LOGGER.info("Configurable Properties Test (lookback_window)")

        conn_id = connections.ensure_connection(self)
        self.conn_id = conn_id

        streams_to_test = {'balance_transactions'}

        # Run a discovery job
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Perform table and field selection...
        core_catalogs = [catalog for catalog in found_catalogs
                         if catalog['stream_name'] in streams_to_test]

        # select all fields for core streams and...
        self.select_all_streams_and_fields(conn_id, core_catalogs, select_all_fields=True)

        # set state to ensure conversion window is used
        today_datetime = int(dt.utcnow().timestamp())
        
        initial_state = {'currently_syncing': None, 'bookmarks': {'balance_transactions': {"created": today_datetime}}}
        menagerie.set_state(conn_id, initial_state)

        # Run a sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify the tap and target do not throw a critical error
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)


class LookbackWindowTestConfig(ConversionWindowBaseTest):

    lookback_window = '300'

    @staticmethod
    def name():
        return "tt_stripe_lookback_window_300"

    def test_run(self):
        self.run_test()
