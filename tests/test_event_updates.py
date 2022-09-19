"""
Test tap gets all updates for streams with updates published to the events stream
"""
import json
from datetime import datetime, timedelta
from time import sleep
from random import random

import requests
from tap_tester import menagerie, runner, connections, LOGGER
from base import BaseTapTest
from utils import \
    get_catalogs, update_object, update_payment_intent, create_object, delete_object


class TestEventUpdatesSyncStart(BaseTapTest):
    """
    Test for event records of streams, Even if start date is set before 30 days,
    no record before 30 days will be received.
    """

    @staticmethod
    def name():
        return "tt_stripe_event_sync_start"

    def test_run(self):
        """
        Verify that each record is from last 30 days.
        """

        # Setting start_date to 32 days before today
        self.start_date = datetime.strftime(datetime.today() - timedelta(days=32), self.START_DATE_FORMAT)
        conn_id = connections.ensure_connection(self, original_properties=False)

        # AS it takes more than hour to sync all the event_updates streams,
        # we are taking given two streams for sync 
        event_update_streams = {"subscriptions", "customers"}

        found_catalogs = self.run_and_verify_check_mode(conn_id)
        our_catalogs = [catalog for catalog in found_catalogs
                        if catalog.get('tap_stream_id') in
                        event_update_streams]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)

        # Getting a date before 30 days of current date-time
        events_start_date = datetime.strftime(datetime.now() - timedelta(days=30), self.START_DATE_FORMAT)

        # Run a sync job using orchestrator
        self.run_and_verify_sync(conn_id)

        # Get the set of records from the sync
        synced_records = runner.get_records_from_target_output()
        
        for stream in event_update_streams:
            with self.subTest(stream=stream):

                # Get event-based records based on the newly added field `updated_by_event_type`
                events_records_data = [message['data'] for message in synced_records.get(stream).get('messages')
                                    if message['action'] == 'upsert' and 
                                    message.get('data').get('updated_by_event_type', None)]

                for record in events_records_data:
                    self.assertGreaterEqual(record.get('updated'), events_start_date)


class EventUpdatesTest(BaseTapTest):
    """
    Test tap gets all updates for streams with updates published to the events stream
    """

    @staticmethod
    def name():
        return "tt_stripe_event_updates"

    def test_run(self):
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
        conn_id = connections.ensure_connection(self)

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
            # "subscription_items", # BUG_9916 | https://jira.talendforge.org/browse/TDL-9916
            "subscriptions",
            # "transfers",  # Cannot be updated directly via api
        }

        found_catalogs = self.run_and_verify_check_mode(conn_id)
        our_catalogs = [catalog for catalog in found_catalogs
                        if catalog.get('tap_stream_id') in
                        event_update_streams]
        self.select_all_streams_and_fields(
            conn_id, our_catalogs, select_all_fields=True
        )

        # Ensure each stream under test has data to start
        new_objects = {
            stream: create_object(stream)
            for stream in event_update_streams
        }

        # Some streams will be updated implicitly
        streams_to_update = event_update_streams.difference({
            "invoice_line_items",
            "subscription_items",
        })

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)

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
        second_sync_record_count = self.run_and_verify_sync(conn_id)

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
