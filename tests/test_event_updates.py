"""
Test tap gets all updates for streams with updates published to the events stream
"""
import json
from time import sleep
from random import random

import requests
from tap_tester import menagerie, runner
from tap_tester.scenario import SCENARIOS
from base import BaseTapTest
from util_stripe import \
    get_catalogs, update_object, create_object, delete_object


class EventUpdatesTest(BaseTapTest):
    """
    Test tap gets all updates for streams with updates published to the events stream
    """

    def name(self):
        return "tap_tester_tap_stripe_event_updates_test"

    def do_test(self, conn_id):
        """
        Verify that the sync only sent records to the target for selected streams
        Update metadata[test] with a random number for each stream with event updates
        Verify that the second sync includes at least one update for each stream
        Verify that the second sync includes less records than the first sync
        Verify that the updated metadata was picked up on the second sync

        PREREQUISITE
        For EACH stream that gets updates through events stream, there's at least 1 row
            of data
        """
        event_update_streams = {
            # "balance_transactions"  # Cannot be directly updated
            "charges",
            "coupons",
            "customers",
            # "disputes",  # Cannot create directly with api
            "invoice_items",
            # "invoice_line_items",  # Can't be updated via api
            "invoices",
            # "payout_transactions",  # See bug in create_test
            "payouts",
            "plans",
            "products",
            # "subscription_items", # BUG https://stitchdata.atlassian.net/browse/SUP-1214
            "subscriptions",
            # "transfers",  # Cannot be updated directly via api
        }

        our_catalogs = get_catalogs(conn_id, event_update_streams)

        self.select_all_streams_and_fields(
            conn_id, our_catalogs, select_all_fields=True
        )

        # Ensure each stream under test has data to start 
        new_objects = {
            stream: create_object(stream)
            for stream in event_update_streams
        }

        # Some streams will be updated implicitly
        streams_to_update =event_update_streams.difference({
            "invoice_line_items",
            "subscription_items",
        })

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_sync(conn_id)

        # verify that the sync only sent records to the target for selected streams (catalogs)
        self.assertEqual(set(first_sync_record_count.keys()), event_update_streams)

        # Get the set of records from a first sync
        first_sync_records = runner.get_records_from_target_output()

        first_sync_created, _ = self.split_records_into_created_and_updated(
            first_sync_records
        )

        updated = {}  # holds id for updated objects in each stream
        for stream in streams_to_update:

            # There needs to be some test data for each stream, otherwise this will break
            self.assertGreater(len(first_sync_created[stream]["messages"]), 0,
                               msg='We did not get any new records from '
                               'the first sync for {}'.format(stream))
            record = first_sync_created[stream]["messages"][0]["data"]

            # We need to make sure the data actually changes, otherwise no event update
            # will get created
            update_object(stream, record["id"])
            updated[stream] = record["id"]

        # Run a second sync job using orchestrator
        second_sync_record_count = self.run_sync(conn_id)

        # Get the set of records from a second sync
        second_sync_records = runner.get_records_from_target_output()

        _, second_sync_updated = self.split_records_into_created_and_updated(
            second_sync_records
        )

        # # THIS MAKES AN ASSUMPTION THAT CHILD STREAMS DO NOT NEED TESTING.
        # # ADJUST IF NECESSARY
        for stream in event_update_streams.difference(self.child_streams()):
            with self.subTest(stream=stream):
                # verify that there is more than 1 record of data - setup necessary
                self.assertGreater(
                    first_sync_record_count.get(stream, 0),
                    1,
                    msg="Data isn't set up to be able to test event updates",
                )

                # verify that you get at least one updated record on the second sync
                self.assertGreaterEqual(
                    len(second_sync_updated.get(stream, {}).get("messages", [])),
                    1,
                    msg="second syc didn't have updates",
                )

                # verify that you get less data the 2nd time around since only updates
                # should be picked up
                self.assertLess(
                    second_sync_record_count.get(stream, 0),
                    first_sync_record_count.get(stream, 0),
                    msg="second syc had the same or more records",
                )

                # verify all the updated records in the 2nd sync are different from
                # the first run
                first_data = next(
                    record["data"]
                    for record in first_sync_created.get(stream, {}).get("messages", [])
                    if record.get("data", {}).get("id") == updated[stream]
                )

                second_data = next(
                    record["data"]
                    for record in second_sync_updated.get(stream, {}).get(
                        "messages", []
                    )
                    if record.get("data", {}).get("id") == updated[stream]
                )

                # verify the updated timestamp is greater in the second sync
                self.assertGreater(
                    second_data["updated"],
                    first_data["updated"],
                    "updated timestamp for second sync is not greater than first sync",
                )

                # verify the metadata[test] value actually changed
                self.assertNotEqual(
                    second_data["metadata"].get("test_value", 0),
                    first_data["metadata"].get("test_value", 0),
                    "the test metadata should be different",
                )

                if stream in new_objects:
                    delete_object(stream, new_objects[stream]["id"])
                

SCENARIOS.add(EventUpdatesTest)
