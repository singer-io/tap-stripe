from utils import stripe_obj_to_dict, client, midnight
from tap_tester import runner, connections
from base import BaseTapTest

def get_payouts():
    # list of all data to return
    data = []
    # api call of 1st page
    stripe_obj = client["payouts"].list(limit=100, created={"gte": midnight})
    dict_obj = stripe_obj_to_dict(stripe_obj)
    # add data
    data += dict_obj['data']

    # loop over rest of the pages and collect data
    while dict_obj.get("has_more"):
        stripe_obj = client["payouts"].list(limit=100, created={"gte": midnight}, starting_after=dict_obj.get('data')[-1].get('id'))
        dict_obj = stripe_obj_to_dict(stripe_obj)
        data += dict_obj['data']

    # send data
    return data

class AutomaticPayoutTransactionTest(BaseTapTest):
    """
        Test case to verify that we only collect payout_transactions for payouts containing "automatic" field as "True"
    """

    @staticmethod
    def name():
        return "tt_stripe_automatic_payout_transactions"

    @classmethod
    def setUpClass(cls):
        # get all the payouts
        payouts = get_payouts()

        # create lists of payout ids containing automatic field as "True" and "False"
        cls.payouts_with_automatic_true = []
        cls.payouts_with_automatic_false = []
        for record in payouts:
            if "automatic" in record and record.get("automatic"):
                cls.payouts_with_automatic_true.append(record.get("id"))
            else:
                cls.payouts_with_automatic_false.append(record.get("id"))

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        expected_streams = {"payouts", "payout_transactions"}

        # Select payouts and payout_transactions streams
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get("tap_stream_id") in expected_streams]

        # field selection
        self.select_all_streams_and_fields(conn_id, our_catalogs)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)

        # Get the set of records from a first sync
        first_sync_records = runner.get_records_from_target_output()

        # set stream as "payout_transactions"
        stream = "payout_transactions"
        with self.subTest(stream=stream):
            # verify that there is more than 1 record
            self.assertGreater(first_sync_record_count.get(stream, -1), 0,
                               msg="Data isn't set up to be able to test full sync")

            # get records
            records = [message.get("data") for message in first_sync_records.get(stream).get("messages") if message["action"] == "upsert"]

            # collect payout ids for all the payout transaction records
            payout_transaction_payout_ids = set()
            for record in records:
                payout_transaction_payout_ids.add(record.get("payout_id"))

            # verify that data exists for payouts with "automatic" field as "True" and "False"
            self.assertTrue(self.payouts_with_automatic_true is not None)
            self.assertTrue(self.payouts_with_automatic_false is not None)

            # loop over all the payout ids from the payout transactions
            for id in payout_transaction_payout_ids:
                # verify that we collect payout transaction record for payout containing "automatic": True
                self.assertTrue(id in self.payouts_with_automatic_true)
                # verify that we do not collect payout transaction record for payout containing "automatic": False
                self.assertTrue(id not in self.payouts_with_automatic_false)
