"""
Test tap sets a bookmark and respects it for the next sync of a stream
"""
import base
import math
import json
from pathlib import Path
from random import random
from time import sleep, perf_counter
from datetime import datetime as dt
from dateutil.parser import parse

from tap_tester import menagerie, runner, connections, LOGGER
from base import BaseTapTest
from utils import create_object, update_object, delete_object, \
    get_hidden_objects, activate_tracking, stripe_obj_to_dict, update_payment_intent


class BookmarkTest(BaseTapTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream"""

    @staticmethod
    def name():
        return "tt_stripe_bookmarks"

    def parse_bookmark_to_date(self, value):
        if value:
            if isinstance(value, str):
                return self.local_to_utc(parse(value))
            if isinstance(value, int):
                return self.local_to_utc(dt.utcfromtimestamp(value))
        return value

    @classmethod
    def setUpClass(cls):
        LOGGER.info("Start Setup")
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
            "payment_intents",
            "products",
            "subscription_items",
            "subscriptions",
        }
        cls.new_objects = {stream: [] for stream in cls.streams_to_create}

    @classmethod
    def tearDownClass(cls):
        LOGGER.info("Start Teardown")
        for stream in cls.streams_to_create:
            for record in cls.new_objects[stream]:
                delete_object(stream, record["id"])

    def test_run(self):
        """
        Verify for each stream that you can do a sync which records bookmarks.
        Verify that the bookmark is the max value sent to the target for the `date` PK field
        Verify that the 2nd sync respects the bookmark
        Verify that all data of the 2nd sync is >= the bookmark from the first sync
        Verify that the number of records in the 2nd sync is less then the first
        Verify inclusivivity of bookmarks

        PREREQUISITE
        For EACH stream that is incrementally replicated there are multiple rows of data with
            different values for the replication key
        """
        untested_streams = {
            'transfers',
            'payout_transactions',  # BUG see create test
            'balance_transactions',  # join stream, can't be updated
            'disputes',
        }
        cannot_update_streams = {
            'invoice_line_items',  # updates not available via api
        }

        # Ensure tested streams have existing records
        expected_records_first_sync = {stream: [] for stream in self.streams_to_create}
        for _ in range(2): # create 3 records for each stream but only expect the 3rd
            for stream in self.streams_to_create:
                self.new_objects[stream].append(create_object(stream))
        # Why are we only expecting the last one?
        for stream in self.streams_to_create:
            new_object = create_object(stream)
            self.new_objects[stream].append(stripe_obj_to_dict(new_object))
            expected_records_first_sync[stream].append({"id": self.new_objects[stream][-1]['id']})

        self.START_DATE = self.get_properties().get('start_date')

        # Instantiate connection with default start
        conn_id = connections.ensure_connection(self)
        self.conn_id = conn_id

        # run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select all testable streams and all fields within streams
        streams_to_select = self.expected_incremental_streams().difference(untested_streams)
        our_catalogs = [catalog for catalog in found_catalogs
                        if catalog.get('tap_stream_id') in
                        streams_to_select]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)

        # Run a sync job using orchestrator
        first_sync_start = self.local_to_utc(dt.utcnow())
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_end = self.local_to_utc(dt.utcnow())

        # verify that the sync only sent records to the target for selected streams (catalogs)
        self.assertEqual(
            streams_to_select, set(first_sync_record_count.keys()),
            msg="Expected only testable streams to be replicated: {}".format(first_sync_record_count)
        )

        first_sync_state = menagerie.get_state(conn_id)

        # Get the set of records from a first sync
        first_sync_records = runner.get_records_from_target_output()

        # Add data before next sync via insert and update, and set expectations
        created_records = {x: [] for x in self.expected_streams()}
        updated_records = {x: [] for x in self.expected_streams()}
        expected_records_second_sync = {x: [] for x in self.expected_streams()}


        # Update one record from each stream prior to 2nd sync
        first_sync_created, _ = self.split_records_into_created_and_updated(first_sync_records)

        for stream in self.streams_to_create.difference(cannot_update_streams):
            # There needs to be some test data for each stream, otherwise this will break
            # TODO - first sync expected records is only the last record which would be synced no matter what
            #   we should actually do the first or second created record which should not be synced unless updated
            record = expected_records_first_sync[stream][0]
            if stream == 'payment_intents':
                # updating the PaymentIntent object may require multiple attempts
                updated_record = update_payment_intent(stream)
            else:
                updated_record = update_object(stream, record["id"])
            updated_records[stream].append(updated_record)
            expected_records_second_sync[stream].append({"id": updated_record['id']})

        # Ensure different times between udpates and inserts
        # NB: using sleep is a testing anti-pattern, please only do this if absolutely necessary
        LOGGER.info("Beginning sleep for 2 seconds")
        sleep(2)
        LOGGER.info("sleep has completed")

        # Insert (create) one record for each stream prior to 2nd sync
        for stream in self.streams_to_create:
            created_record = create_object(stream)
            self.new_objects[stream].append(created_record)
            created_records[stream].append(created_record)
            expected_records_second_sync[stream].append({"id": created_record['id']})

        # ensure validity of expected_records_second_sync
        for stream in self.streams_to_create:
            if stream in self.expected_incremental_streams():
                if stream in cannot_update_streams:
                    # Some streams will have only 1 record from the Insert
                    self.assertEqual(1, len(expected_records_second_sync.get(stream)),
                                     msg="Expectations are invalid for incremental stream {}".format(stream)
                    )
                    continue
                # Most streams will have 2 records from the Update and Insert
                self.assertEqual(2, len(expected_records_second_sync.get(stream)),
                                 msg="Expectations are invalid for incremental stream {}".format(stream)
                )
            elif stream in self.expected_full_table_streams():
                self.assertEqual(
                    len(expected_records_second_sync.get(stream)),
                    len(expected_records_first_sync.get(stream)) + len(created_records[stream]),
                    msg="Expectations are invalid for full table stream {}".format(stream)
                )
            else:
                raise NotImplementedError("The replication method has no tests")

            # created_records[stream] = self.records_data_type_conversions(created_records.get(stream))
            # updated_records[stream] = self.records_data_type_conversions(updated_records.get(stream))


        # Run a second sync job using orchestrator
        second_sync_start = self.local_to_utc(dt.utcnow())
        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_end = self.local_to_utc(dt.utcnow())

        second_sync_state = menagerie.get_state(conn_id)

        # Get the set of records from a second sync
        second_sync_records = runner.get_records_from_target_output()
        second_sync_created, second_sync_updated = self.split_records_into_created_and_updated(
            second_sync_records)

        # Loop first_sync_records and compare against second_sync_records
        for stream in self.streams_to_create.difference(untested_streams):
            with self.subTest(stream=stream):
                # TODO - We should assert the bookmark value is correct, i.e. it is the value of
                #        the latest record to come back from the sync. Add assetions.
                second_sync_data = [record.get("data") for record
                                    in second_sync_records.get(stream, {}).get("messages", [])]
                second_sync_created_data = [record.get("data") for record
                                            in second_sync_created.get(stream, {}).get("messages", [])]
                second_sync_updated_data = [record.get("data") for record
                                            in second_sync_updated.get(stream, {}).get("messages", [])]

                tap_replication_keys = self.expected_replication_keys()
                tap_primary_keys = self.expected_primary_keys()

                # TESTING INCREMENTAL STREAMS
                if stream in self.expected_incremental_streams():

                    stream_replication_keys = tap_replication_keys.get(stream)

                    # Verify both syncs write / keep the same bookmark keys
                    self.assertEqual(set(first_sync_state.get('bookmarks', {}).keys()),
                                     set(second_sync_state.get('bookmarks', {}).keys()))

                    # verify that there is more than 1 record of data - setup necessary
                    self.assertGreater(first_sync_record_count.get(stream, 0), 1,
                                       msg="Data isn't set up to be able to test full sync")

                    # verify that you get less data on the 2nd sync
                    self.assertGreater(
                        first_sync_record_count.get(stream, 0),
                        second_sync_record_count.get(stream, 0),
                        msg="first sync didn't have more records, bookmark usage not verified")

                    if stream in self.streams_to_create.difference(cannot_update_streams):
                        for replication_key in stream_replication_keys:
                            updates_replication_key = "updates_created"
                            updates_stream = stream + "_events"

                            sync_1_created_bookmark = list(first_sync_state.get('bookmarks', {stream: {}}).get(stream).values())
                            assert len(sync_1_created_bookmark) == 1, sync_1_created_bookmark
                            sync_1_value = sync_1_created_bookmark[0]
                            sync_1_updated_value = first_sync_state.get('bookmarks', {updates_stream: {updates_replication_key: -1}}).get(
                                    updates_stream, {updates_replication_key: -1}).get(updates_replication_key)
                            sync_2_created_bookmark = list(second_sync_state.get('bookmarks', {stream: {}}).get(stream).values())
                            assert len(sync_2_created_bookmark) == 1, sync_2_created_bookmark
                            sync_2_value = sync_2_created_bookmark[0]
                            sync_2_updated_value = second_sync_state.get('bookmarks', {updates_stream: {updates_replication_key: -1}}).get(
                                    updates_stream, {updates_replication_key: -1}).get(updates_replication_key)

                            # Verify second sync's bookmarks move past the first sync's for update events
                            self.assertGreater(sync_2_updated_value, sync_1_updated_value)

                            # Verify second sync's bookmarks move past the first sync's for create data
                            self.assertGreater(sync_2_value, sync_1_value)


                            # Verify that all data of the 2nd sync is >= the bookmark from the first sync
                            first_sync_bookmark_created = dt.fromtimestamp(sync_1_value)
                            print(f"*** TEST - 1st sync created: {first_sync_bookmark_created}")

                            first_sync_bookmark_updated = dt.fromtimestamp(sync_1_updated_value)
                            print(f"*** TEST - 1st sync updated: {first_sync_bookmark_updated}")

                            # BUG - Remove following 2 code lines after bug fix
                            #       https://jira.talendforge.org/browse/TDL-21007
                            first_sync_bookmark_created = min(first_sync_bookmark_created, first_sync_bookmark_updated)
                            first_sync_bookmark_updated = min(first_sync_bookmark_created, first_sync_bookmark_updated)

                            # This assertion would fail for the child streams as it is replicated based on the parent i.e. it would fetch the parents based on
                            # the bookmark and retrieve all the child records for th parent.
                            # Hence skipping this assertion for child streams.
                            if stream not in self.child_streams().union({'payout_transactions'}):
                                for record in second_sync_created_data:
                                    print("2nd Sync Created Record Data")
                                    print(f" updated: {record['updated']}\n {replication_key}: {record[replication_key]}")
                                    date_value = record["updated"]
                                    self.assertGreaterEqual(date_value,
                                                            dt.strftime(first_sync_bookmark_created, self.TS_COMPARISON_FORMAT),
                                                            msg="A 2nd sync record has a replication-key that is less than or equal to the 1st sync bookmark.")

                            if stream not in self.child_streams().union({'payout_transactions'}):
                                for record in second_sync_updated_data:
                                    print("2nd Sync Updated Record Data")
                                    print(f" updated: {record['updated']}\n {replication_key}: {record[replication_key]}")
                                    date_value = record["updated"]
                                    self.assertGreaterEqual(date_value,
                                                            dt.strftime(first_sync_bookmark_updated, self.TS_COMPARISON_FORMAT),
                                                            msg="A 2nd sync record has a replication-key that is less than or equal to the 1st sync bookmark.")

                    else:
                        # TODO created streams that connot be updated tested here.
                        pass


                elif stream in self.expected_full_table_streams():
                    raise Exception("Expectations changed, but this test was not updated to reflect them.")
                else:
                    raise Exception("Replication method changed, but this test was not updated to reflect.")

                # TESTING APPLICABLE TO ALL STREAMS

                # Verify that the expected records are replicated in the 2nd sync
                # For incremental streams we should see at least 2 records (a new record and an updated record)
                # but we may see more as the bookmmark is inclusive and there are hidden creates/updates due to
                # dependencies between streams.
                # For full table streams we should see 1 more record than the first sync
                expected_records = expected_records_second_sync.get(stream)
                stream_primary_keys = tap_primary_keys.get(stream)

                updated_pk_values = {tuple([record.get(pk) for pk in stream_primary_keys])
                                     for record in updated_records[stream]}
                self.assertLessEqual(
                    len(expected_records), len(second_sync_data),
                    msg="Expected number of records are not less than or equal to actual for 2nd sync.\n" +
                    "Expected: {}\nActual: {}".format(len(expected_records), len(second_sync_data))
                )
                if (len(second_sync_data) - len(expected_records)) > 0:
                    LOGGER.warn('Second sync replicated %s records more than our create and update for %s',
                                len(second_sync_data), stream)

                if not stream_primary_keys:
                    raise NotImplementedError("PKs are needed for comparing records")

                # Verify that the inserted and updated records are replicated by the 2nd sync
                for expected_record in expected_records:
                    expected_pk_value = expected_record.get('id')
                    sync_pk_values = [sync_record.get('id')
                                      for sync_record in second_sync_data
                                      if sync_record.get('id') == expected_pk_value]
                    if stream != 'invoice_items':
                        self.assertTrue(len(sync_pk_values) > 0,
                                        msg = ("A record is missing from module import symbol "
                                               "our sync: \nSTREAM: {}\tPK: {}".format(
                                                   stream, expected_pk_value))
                        )
                        self.assertIn(expected_pk_value, sync_pk_values)
                    else:
                        is_done = base.JIRA_CLIENT.get_status_category("TDL-24065") == 'done'
                        assert_message = ("JIRA ticket has moved to done, remove the "
                                          "if stream != 'invoice_items' line above.")
                        assert is_done == False, assert_message

                # Verify updated fields are replicated as expected
                if stream == "payment_intents":
                    # payment_intents stream does not generate any event on the update of the metadata field.
                    # payment_intent can be confirmed by updating the payment_method field and it generates succeeds event.
                    # So, for the payment_intents stream, we are verifying updates for the payment_method field.
                    for updated_record in updated_records[stream]:
                        updated_pk_value = updated_record.get('id')
                        sync_records_payment_method = [sync_record.get('payment_method')
                                                for sync_record in second_sync_data
                                                if sync_record.get('id') == updated_pk_value]
                        self.assertIsNotNone(sync_records_payment_method[0])
                else:
                    for updated_record in updated_records[stream]:
                        if stream == 'invoice_items':
                            is_done = base.JIRA_CLIENT.get_status_category("TDL-24065") == 'done'
                            assert_message = ("JIRA ticket has moved to done, remove the "
                                              "if stream != 'invoice_items' line above.")
                            assert is_done == False, assert_message

                            continue

                        expected_updated_key = 'metadata'
                        expected_updated_value_substring = 'bob'
                        updated_pk_value = updated_record.get('id')
                        sync_records_metadata = [sync_record.get('metadata')
                                                for sync_record in second_sync_data
                                                if sync_record.get('id') == updated_pk_value]
                        self.assertTrue(len(sync_records_metadata) == 1)

                        if base.JIRA_CLIENT.get_status_category("TDL-24065") == 'done':
                            assert_message = ("JIRA ticket has moved to done, uncomment the "
                                              "assertion below.")
                            assert True == False, assert_message

                        # uncomment when TDL-24065 is completed and updates test is stable
                        # self.assertIn(expected_updated_value_substring,
                        #             sync_records_metadata[0].get('test_value'))
