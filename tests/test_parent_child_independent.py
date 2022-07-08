from tap_tester import runner, connections
from datetime import datetime as dt
from datetime import timedelta
from base import BaseTapTest

class ParentChildIndependentTest(BaseTapTest):

    def name(self):
        return "tt_stripe_parent_child_test"

    def test_child_streams(self):
        """
            Test case to verify that tap is working fine if only first level child streams are selected
        """
        # select child streams only and run the test
        child_streams = {"invoice_line_items", "subscription_items"}
        start_date_1 = dt.strftime(dt.today(), self.START_DATE_FORMAT)
        self.run_test(child_streams, start_date_1)
        child_streams = {"payout_transactions"}
        start_date_1 = dt.strftime(dt.today() - timedelta(days=4), self.START_DATE_FORMAT)
        self.run_test(child_streams, start_date_1)

    def run_test(self, streams, start_date):
        """
            Testing that tap is working fine if only child streams are selected
            - Verify that if only child streams are selected then only child stream are replicated.
        """
        self.start_date = start_date
        # instantiate connection
        conn_id = connections.ensure_connection(self, original_properties=False)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        self.perform_and_verify_table_and_field_selection(conn_id, found_catalogs, streams_to_select=streams)

        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(streams, synced_stream_names)