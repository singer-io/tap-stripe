"""
Test tap gets all creates for streams (as long as we can create an object)
"""

from tap_tester import menagerie, runner, connections, LOGGER
from base import BaseTapTest
from utils import create_object, delete_object


class CreateObjectTest(BaseTapTest):
    """Test tap gets all creates for streams (as long as we can create an object)"""

    @staticmethod
    def name():
        return "tt_stripe_create_objects"

    def test_run(self):
        """
        Verify that the sync only sent records to the target for selected streams
        Create a new object for each stream
        Verify that the second sync includes at least one create for each stream
        Verify that the created record was picked up on the second sync
        """
        conn_id = connections.ensure_connection(self)
        self.conn_id = conn_id

        streams_to_create = {
            "balance_transactions",  # should be created implicity with a create in the payouts or charges streams
            "charges",
            "coupons",
            "customers",
            "invoice_items",
            "invoice_line_items", # this is created implicity by invoices, it just creates another invoice TODO get this outa here
            "invoices", # this will create an invoice_item
            "payouts",
            "plans",
            "payment_intents",
            "products",
            "subscription_items",
            "subscriptions", # this will create a new plan and payment method
         }

        missing_streams_to_create = {
            "disputes",  # can be created by simulating a dispute transaction with a specific card number
            # no way to create directly, see: https://stripe.com/docs/testing#disputes
            "payout_transactions",  # BUG_9703 | https://jira.talendforge.org/browse/TDL-9703
            # depends on payouts and transactions
            "transfers",
            # needs an account that we can transfer to, not sure
            # how to set up a test account we can use to create a transfer
         }

        our_catalogs = self.run_and_verify_check_mode(conn_id)

        self.select_all_streams_and_fields(
            conn_id, our_catalogs, select_all_fields=True
        )

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)

        # verify that the sync sent records to the target for selected streams (catalogs)
        self.assertTrue(streams_to_create.issubset(set(first_sync_record_count.keys())))

        # Get the set of records from a first sync
        first_sync_records = runner.get_records_from_target_output()

        first_sync_created, _ = self.split_records_into_created_and_updated(
            first_sync_records
        )

        new_objects = {
            stream: create_object(stream)
            for stream in streams_to_create.difference({"balance_transactions"})
        }

        # Run a second sync job using orchestrator
        second_sync_record_count = self.run_and_verify_sync(conn_id)

        # Get the set of records from a second sync
        second_sync_records = runner.get_records_from_target_output()

        second_sync_created, _ = self.split_records_into_created_and_updated(
            second_sync_records
        )

        # # THIS MAKES AN ASSUMPTION THAT CHILD STREAMS DO NOT NEED TESTING.
        # # ADJUST IF NECESSARY
        for stream in streams_to_create.difference(self.child_streams()):
            with self.subTest(stream=stream):

                second_sync_created_objects = second_sync_created.get(stream, {}).get(
                    "messages", []
                )

                # verify that you get at least one new record on the second sync
                self.assertGreaterEqual(
                    len(second_sync_created_objects),
                    1,
                    msg="second sync didn't have created objects",
                )

                if stream == "balance_transactions":
                    sources = [record.get("data", {}).get("source")
                               for record in second_sync_created_objects]

                    self.assertTrue(new_objects['payouts']['id'] in sources)
                    self.assertTrue(new_objects['charges']['id'] in sources)

                    continue

                # TODO START DEBUG
                # remove debug after BUG https://jira.talendforge.org/browse/TDL-21614 is resolved
                if stream == 'invoices':
                    null_date_invoices = []
                    masking_invoices = []
                    for rec in second_sync_records[stream]['messages']:
                        # detect old failures by comparing record dates using both replication keys
                        # it is believed that the created invoices never have 'date' and should
                        # always fail verification due to the old split logic
                        if not rec['data'].get('date'):
                            if rec['data'].get('created') == rec['data'].get('updated'):
                                null_date_invoices += [rec['data']['id']]
                        # date key was found for records in the else clause.  It is believed that
                        # these are all updated records. Check to see if failure would be masked
                        # by the split logic
                        else:
                            if rec['data'].get('date') == rec['data'].get('updated'):
                                masking_invoices += [rec['data']['id']]
                    LOGGER.info(f"null_date_invoices: {null_date_invoices}, "
                                f"masking_invoices: {masking_invoices}, "
                                f"new_id: {new_objects[stream]['id']}")
                    self.assertTrue(new_objects[stream]['id'] in null_date_invoices)
                    if new_objects[stream]['id'] not in masking_invoices:
                        LOGGER.warn(f"########## Previous error scenario detected (un-masked failure) ##########")
                # TODO END DEBUG

                # verify the new object is in the list of created objects
                # from the second sync
                self.assertTrue(
                    any(
                        new_objects[stream]["id"] == record.get("data", {}).get("id")
                        for record in second_sync_created_objects
                    )
                )

                if stream in streams_to_create:
                    delete_object(stream, new_objects[stream]["id"])
