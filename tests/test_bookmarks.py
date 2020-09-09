"""
Test tap sets a bookmark and respects it for the next sync of a stream
"""
import json
import logging
from pathlib import Path
from random import random
from time import sleep, perf_counter
from datetime import datetime as dt
from dateutil.parser import parse

from tap_tester import menagerie, runner
from base import BaseTapTest
from utils import \
    create_object, update_object, delete_object, get_hidden_objects, activate_tracking


class BookmarkTest(BaseTapTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream"""

    @staticmethod
    def name():
        return "tap_tester_tap_stripe_bookmark_test"

    def parse_bookmark_to_date(self, value):
        if value:
            if isinstance(value, str):
                return self.local_to_utc(parse(value))
            if isinstance(value, int):
                return self.local_to_utc(dt.utcfromtimestamp(value))
        return value

    # TODO address all the import warnings
    # def do_test(self, conn_id):
    #     import warnings

    #     def fxn():
    #         warnings.warn("deprecated", DeprecationWarning)
    #     with warnings.catch_warnings():
    #         warnings.simplefilter("ignore")
    #         fxn()

    @classmethod
    def setUpClass(cls):
        logging.info("Start Setup")
        # Create data prior to first sync
        cls.streams_to_create = {
            "customers",
            "charges",
            "coupons",
            "invoice_items",
            "invoice_line_items",
            "invoices",
            "payouts",
            "plans",
            "products",
            "subscription_items",
            "subscriptions",
        }
        cls.new_objects = {stream: [] for stream in cls.streams_to_create}

    @classmethod
    def tearDownClass(cls):
        logging.info("Start Teardown")
        for stream in cls.streams_to_create:
            for record in cls.new_objects[stream]:
                delete_object(stream, record["id"])

    def test_run(self):
        """
        Verify that for each stream you can do a sync which records bookmarks.
        That the bookmark is the maximum value sent to the target for the replication key.
        That a second sync respects the bookmark
            All data of the second sync is >= the bookmark from the first sync
            The number of records in the 2nd sync is less then the first (This assumes that
                new data added to the stream is done at a rate slow enough that you haven't
                doubled the amount of data from the start date to the first sync between
                the first sync and second sync run in this test)

        Verify that only data for incremental streams is sent to the target

        PREREQUISITE
        For EACH stream that is incrementally replicated there are multiple rows of data with
            different values for the replication key
        """
        conn_id = self.create_connection()

        expected_records = {stream: [] for stream in self.streams_to_create}

        # Create 3 records for each stream total
        for _ in range(2):
            for stream in self.streams_to_create:
                self.new_objects[stream].append(create_object(stream))
        # Turn on tracking of hidden object creates in util_stripe
        # so that we can properly assert on 2nd sync records and expectations
        activate_tracking()
        # Create the 3rd record now that we are recording all created objects
        for stream in self.streams_to_create:
            self.new_objects[stream].append(create_object(stream))
            expected_records[stream].append({"id": self.new_objects[stream][-1]['id']})

        # Select all streams and no fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        incremental_streams = {key for key, value in self.expected_replication_method().items()
                               if value == self.INCREMENTAL}

        # IF THERE ARE STREAMS THAT SHOULD NOT BE TESTED
        # REPLACE THE EMPTY SET BELOW WITH THOSE STREAMS
        untested_streams = self.child_streams().union({
            'transfers',
            'payout_transactions',  # BUG see create test
            'balance_transactions',  # join stream, can't be updated
            'disputes',
        })
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in incremental_streams.difference(
                            untested_streams)]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)

        # Run a sync job using orchestrator and track the sync's time window
        first_sync_start = self.local_to_utc(dt.utcnow())
        first_sync_record_count = self.run_sync(conn_id)
        first_sync_end = self.local_to_utc(dt.utcnow())

        # verify that the sync only sent records to the target for selected streams (catalogs)
        self.assertEqual(set(first_sync_record_count.keys()),
                         incremental_streams.difference(untested_streams))

        first_sync_state = menagerie.get_state(conn_id)

        # Get data about actual rows synced
        first_sync_records = runner.get_records_from_target_output()
        first_max_bookmarks = self.max_bookmarks_by_stream(first_sync_records)

        first_max_events = self.max_events_bookmarks_by_stream(first_sync_records)
        first_min_bookmarks = self.min_bookmarks_by_stream(first_sync_records)

        # Update one record from each stream prior to 2nd sync
        first_sync_created, _ = self.split_records_into_created_and_updated(first_sync_records)
        updated_objects = {stream: [] for stream in self.streams_to_create}  # TODO Delete if never used

        for stream in self.streams_to_create:
            # There needs to be some test data for each stream, otherwise this will break
            record = expected_records[stream][0]
            # record = first_sync_created[stream]["messages"][0]["data"]
            updated_objects[stream].append(update_object(stream, record["id"]))
            expected_records[stream].append({"id": updated_objects[stream][-1]['id']})

        # Ensure different times between udpates and inserts
        sleep(2)

        # Insert (create) one record for each stream prior to 2nd sync
        for stream in self.streams_to_create:
            self.new_objects[stream].append(create_object(stream))
            expected_records[stream].append({"id": self.new_objects[stream][-1]['id']})

        # Run a second sync job using orchestrator
        second_sync_start = self.local_to_utc(dt.utcnow())
        second_sync_record_count = self.run_sync(conn_id)
        second_sync_end = self.local_to_utc(dt.utcnow())

        second_sync_state = menagerie.get_state(conn_id)

        # Get data about rows synced
        second_sync_records = runner.get_records_from_target_output()
        second_sync_created, second_sync_updated = self.split_records_into_created_and_updated(second_sync_records)
        second_min_bookmarks = self.min_bookmarks_by_stream(second_sync_created)
        second_max_bookmarks = self.max_bookmarks_by_stream(second_sync_records)
        second_max_events = self.max_events_bookmarks_by_stream(second_sync_records)
        self.assertTrue(second_max_events)

        # Define bookmark keys
        events_bookmark_key = "updates_created"  # secondary bookmark from the events stream
        target_events_bookmark_key = "updated"  # the bookmark key for events

        # Adjust expectations to account for invoices created by the invoice_line_items calls
        for e in expected_records['invoice_line_items']:
            expected_records['invoices'].append(e)

        # THIS MAKES AN ASSUMPTION THAT CHILD STREAMS DO NOT HAVE BOOKMARKS.
        # ADJUST IF NECESSARY
        for stream in incremental_streams.difference(untested_streams):
            with self.subTest(stream=stream):

                # Get bookmark values from state and target data.
                # Recall that there are two bookmarks for every object stream: an events-based
                # bookmark for updates, and a standard repliction key-based bookmark for creates.
                stream_bookmark_key = self.expected_replication_keys().get(stream, set())
                assert len(stream_bookmark_key) == 1  # There shouldn't be a compound replication key
                stream_bookmark_key = stream_bookmark_key.pop()

                state_value = first_sync_state.get("bookmarks", {}).get(
                    stream + "", {}).get(stream_bookmark_key)
                target_value = first_max_bookmarks.get(
                    stream + "", {}).get(stream_bookmark_key)
                target_min_value = first_min_bookmarks.get(
                    stream + "", {}).get(stream_bookmark_key)
                # the events stream should get an empty set as events_events does not exist
                state_value_events = first_sync_state.get("bookmarks", {}).get(
                    stream + "_events", {}).get(events_bookmark_key)
                target_events_value = first_max_events.get(
                    stream + "_events", {}).get(target_events_bookmark_key)

                try:
                    # attempt to parse the bookmark as a date
                    state_value = self.parse_bookmark_to_date(state_value)
                    state_value_events = self.parse_bookmark_to_date(state_value_events)
                    target_value = self.parse_bookmark_to_date(target_value)
                    target_events_value = self.parse_bookmark_to_date(target_events_value)
                    target_min_value = self.parse_bookmark_to_date(target_min_value)

                except (OverflowError, ValueError, TypeError):
                    print("bookmarks cannot be converted to dates, comparing values directly")

                ##########################################################################
                ### 1st Sync Tests
                ##########################################################################

                if stream != 'payout_transactions':
                    # verify that there is data with different bookmark values - setup necessary
                    self.assertGreaterEqual(target_value, target_min_value,
                                            msg="Data isn't set up to be able to test bookmarks")

                # TODO run test with our time = current_time + 1 hour and see if the events stream is able to cover for any potential
                # issues with our time window discrepancy capturing updates/creates

                # verify that 'created' state is saved for the first sync in the appropriate
                # time window, between (inclusive) the target value (max bookmark) for this stream and
                # the first sync end time
                self.assertGreaterEqual(state_value, target_value,
                                        msg="The bookmark value isn't correct based on target data")
                self.assertLessEqual(state_value, first_sync_end,
                                     msg="The bookmark value isn't correct based on "
                                     "the start sync")

                if state_value_events != set() and stream != 'events':
                    if stream not in {'invoices', 'customers'}:  # BUG https://stitchdata.atlassian.net/browse/SUP-1320
                        # verify that 'updates' state matches the target (max bookmark)
                        self.assertEqual(state_value_events, target_events_value,
                                         msg="The bookmark value isn't correct "
                                         "based on target data")

                    # NOTE: This assertion is no longer valid. Some streams will create records for other streams
                    # verify the last record has the max bookmark in the target (this should never fail)
                    # last_created = self.parse_bookmark_to_date(self.new_objects[stream][2]['created'])
                    # self.assertEqual(target_value, last_created,
                    #                  msg="The last created record for the first sync should "
                    #                  "be sthe max bookmark in the target and is not.")

                else: # We need to ensure no assertions are missed for object streams
                    self.assertEqual(stream, 'events',
                                     msg="An object stream is missing it's object_events bookmark")

                ##########################################################################
                ### 2nd Sync Tests
                ##########################################################################

                prev_target_value = target_value
                prev_target_events_value = target_events_value
                target_value = second_max_bookmarks.get(stream + "", {}).get(stream_bookmark_key)
                target_min_value = second_min_bookmarks.get(
                    stream + "", {}).get(stream_bookmark_key)
                final_state_value = second_sync_state.get("bookmarks", {}).get(
                    stream + "", {}).get(stream_bookmark_key)
                final_state_value_events = second_sync_state.get("bookmarks", {}).get(
                    stream + "_events", {}).get(events_bookmark_key)
                target_events_value = second_max_events.get(
                    stream + "_events", {}).get(target_events_bookmark_key)

                try:
                    final_state_value = self.parse_bookmark_to_date(final_state_value)
                    final_state_value_events = self.parse_bookmark_to_date(final_state_value_events)
                    target_value = self.parse_bookmark_to_date(target_value)
                    target_events_value = self.parse_bookmark_to_date(target_events_value)
                    target_min_value = self.parse_bookmark_to_date(target_min_value)

                except (OverflowError, ValueError, TypeError):
                    print("bookmarks cannot be converted to dates, comparing values directly")

                # verify that the 2nd sync gets more than zero records
                self.assertGreater(second_sync_record_count.get(stream, 0), 0,
                                   msg="second syc didn't have any records")

                # verify that you get less data the 2nd time around
                self.assertGreater(
                    first_sync_record_count.get(stream, 0),
                    second_sync_record_count.get(stream, 0),
                    msg="second syc didn't have less records, bookmark usage not verified")

                # verify 'created' state is at or after the target (max bookmark) and before
                # or equal to the second sync end
                self.assertGreaterEqual(final_state_value, target_value,
                                        msg="The bookmark value isn't correct based on target data")
                self.assertLessEqual(final_state_value, second_sync_end,
                                     msg="The bookmark value isn't correct based on "
                                     "the end sync")

                if final_state_value_events != set() and stream != 'events':
                    if stream not in {'invoices', 'customers'}:  # BUG https://stitchdata.atlassian.net/browse/SUP-1320
                        # verify that 'updates' state matches the target (max bookmark)
                        self.assertEqual(final_state_value_events, target_events_value,
                                         msg="The bookmark value isn't correct based on target data")

                else: # We need to ensure no assertions are missed for object streams
                    self.assertEqual(stream, 'events',
                                     msg="An object stream is missing it's object_events bookmark")

                # verify that the second sync state is greater than or equal to the minimum target value
                self.assertGreaterEqual(target_value, target_min_value)

                # Verify that the minimum bookmark in the second sync is greater than or equal to state value from the first sync.
                self.assertGreaterEqual(target_min_value, prev_target_value,
                                 msg="Minimum bookmark in 2nd sync does not match 1st sync state value")

                if stream != 'events':

                    # verify that 2nd sync captures the expected records and nothing else
                    expected = expected_records[stream]
                    expected_set = set()
                    for record in expected:  # creating a set of expected ids for current stream
                        for key, val in record.items():
                            if 'id' == key: expected_set.add(val)

                    actual = [item["data"] for item in
                              second_sync_records.get(stream, {'messages': []}).get('messages')]
                    actual_set = set()
                    for record in actual:
                        for key, val in record.items():  # creating set of actual ids
                            if 'id' == key: actual_set.add(val)

                    if not expected_set.issubset(actual_set): # output with print in case next assertion fails
                        print(stream + "\nE: " + str(expected_set) + "\nA:" + str(actual_set))
                    self.assertTrue(expected_set.issubset(actual_set),
                                    msg="We expected records to be sent to the target and they were not")

                    # To create an object, some streams require the creation of objects from different streams
                    # As a result we create more than what is obvious in the test to capture all streams
                    # The 'hidden_creates' set tracks those extra records, in order to verify we aren't
                    # getting more records than expected
                    hidden_creates = get_hidden_objects(stream)

                    if stream in {
                            'plans',  # BUG https://stitchdata.atlassian.net/browse/SUP-1303
                            'subscriptions',  # BUG https://stitchdata.atlassian.net/browse/SUP-1304
                            'invoice_items', # BUG https://stitchdata.atlassian.net/browse/SUP-1316
                            'customers'}: # BUG https://stitchdata.atlassian.net/browse/SUP-1322
                        continue

                    # if stream == 'invoice_items': # UNCOMMENT WHEN SUP-1316 is addressed
                    #     # verify everything we created was sent
                    #     # NOTE: we annot account for all hidden records created in the bg for this stream
                    #     self.assertEqual(expected_set.union(hidden_creates) - actual_set, set(),
                    #                     msg="Failed to capture all records that were explicitly created.")
                    #     continue

                    self.assertEqual(actual_set.difference(expected_set), hidden_creates,
                                     msg="Some extra records sent to the target that were not in expected")
