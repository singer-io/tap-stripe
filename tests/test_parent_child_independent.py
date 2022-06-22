from tap_tester import runner, connections
from base import BaseTapTest

class ParentChildIndependentTest(BaseTapTest):

    def name(self):
        return "tt_stripe_parent_child_test"

    def test_child_streams(self):
        """
            Test case to verify that tap is working fine if only first level child streams are selected
        """
        # select child streams only and run the test
        child_streams = {"invoice_line_items", "subscription_items", "payout_transactions"}
        self.run_test(child_streams)

    def run_test(self, streams):
        """
            Testing that tap is working fine if only child streams are selected
            - Verify that if only child streams are selected then only child stream are replicated.
        """
        # instantiate connection
        conn_id = connections.ensure_connection(self)

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