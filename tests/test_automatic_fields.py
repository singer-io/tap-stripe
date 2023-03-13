"""
Test that with no fields selected for a stream automatic fields are still replicated
"""

from tap_tester import runner, menagerie, connections

from base import BaseTapTest
from utils import create_object


class MinimumSelectionTest(BaseTapTest):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    @staticmethod
    def name():
        return "tt_stripe_auto_fields"

    def test_run(self):
        """
        Verify that for each stream you can get multiple pages of data
        when no fields are selected and only the automatic fields are replicated.

        PREREQUISITE
        For EACH stream add enough data that you surpass the limit of a single
        fetch of data.  For instance if you have a limit of 250 records ensure
        that 251 (or more) records have been posted for that stream.
        """
        conn_id = connections.ensure_connection(self)
        self.conn_id = conn_id
        streams_to_create = {
            # "balance_transactions",  # should be created implicity with a create in the payouts or charges streams
            "charges",
            "coupons",
            "customers",
            "invoice_items",
            "invoice_line_items", # this is created implicity by invoices, it just creates another invoice
            "invoices", # this will create an invoice_item
            "payouts",
            "plans",
            "payment_intents",
            "products",
            "subscription_items",
            "subscriptions", # this will create a new plan and payment method
            "transfers",
         }
        untested_streams = {
            "payout_transactions",
            "disputes"
        }
        new_objects = {
            stream: create_object(stream)
            for stream in streams_to_create.difference()
        }


        # Select all streams and no fields within streams
        # IF THERE ARE NO AUTOMATIC FIELDS FOR A STREAM
        # WE WILL NEED TO UPDATE THE BELOW TO SELECT ONE
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        self.select_all_streams_and_fields(conn_id, found_catalogs, select_all_fields=False)

        # Run a sync job using orchestrator
        record_count_by_stream = self.run_and_verify_sync(conn_id)

        actual_fields_by_stream = runner.examine_target_output_for_fields()

        for stream in self.expected_streams().difference(untested_streams):
            with self.subTest(stream=stream):

                # verify that you get some records for each stream
                # SKIP THIS ASSERTION FOR STREAMS WHERE YOU CANNOT GET
                # MORE THAN 1 PAGE OF DATA IN THE TEST ACCOUNT
                self.assertGreater(
                    record_count_by_stream.get(stream, -1), 0,
                    msg="The number of records is not over the stream max limit")

                # verify that only the automatic fields are sent to the target
                actual = actual_fields_by_stream.get(stream) or set()
                expected = self.expected_automatic_fields().get(stream, set())
                self.assertEqual(
                    actual, expected,
                    msg=("The fields sent to the target are not the automatic fields. Expected: {}, Actual: {}"
                         .format(actual, expected))
                )
