from tap_tester import runner, connections
from datetime import datetime as dt
from datetime import timedelta
from base import BaseTapTest

class ParentChildIndependentTest(BaseTapTest):

    @staticmethod
    def name():
        return "tt_stripe_parent_child_test"

    def test_child_streams(self):
        """
            Test case to verify that tap is working fine if only first level child streams are selected
        """
        four_days_ago = dt.strftime(dt.today() - timedelta(days=4), self.START_DATE_FORMAT)
        # select child streams only and run the test
        child_streams = {"invoice_line_items", "subscription_items"}
        self.run_test(child_streams)
        # Separated the payout_transactions stream as there is a lag from the Stripe side to reflect
        # the automatic payout transactions data, hence we want to change the start_date for that stream
        child_streams = {"payout_transactions"}
        start_date = four_days_ago
        self.run_test(child_streams, start_date, False)

    def run_test(self, streams, start_date=None, default_start_date=True):
        """
            Testing that tap is working fine if only child streams are selected
            - Verify that if only child streams are selected then only child stream are replicated.
        """

        if not default_start_date:
            self.start_date = start_date
        # instantiate connection
        conn_id = connections.ensure_connection(self, original_properties=default_start_date)
        self.conn_id = conn_id

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
