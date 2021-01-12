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
        return "tap_tester_tap_stripe_bookmark_revised_test"

    def parse_bookmark_to_date(self, value):
        if value:
            if isinstance(value, str):
                return self.local_to_utc(parse(value))
            if isinstance(value, int):
                return self.local_to_utc(dt.utcfromtimestamp(value))
        return value

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
        untested_streams = self.child_streams().union({
            'transfers',
            'payout_transactions',  # BUG see create test
            'balance_transactions',  # join stream, can't be updated
            'disputes',
        })

        # Ensure tested streams have existing records
        expected_records_first_sync = {stream: [] for stream in self.streams_to_create}
        for _ in range(2): # create 3 records for each stream but only expect the 3rd
            for stream in self.streams_to_create:
                self.new_objects[stream].append(create_object(stream))
        for stream in self.streams_to_create:
            self.new_objects[stream].append(create_object(stream))
            expected_records_first_sync[stream].append({"id": self.new_objects[stream][-1]['id']})

        self.START_DATE = self.get_properties().get('start_date')

        # Instantiate connection with default start
        conn_id = self.create_connection()

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Select all testable streams and all fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        streams_to_select = self.expected_incremental_streams().difference(untested_streams)
        our_catalogs = [catalog for catalog in found_catalogs
                        if catalog.get('tap_stream_id') in
                        streams_to_select]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)

        # Run a sync job using orchestrator
        first_sync_start = self.local_to_utc(dt.utcnow())
        first_sync_record_count = self.run_sync(conn_id)
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
        for stream in self.streams_to_create:
            # There needs to be some test data for each stream, otherwise this will break
            record = expected_records_first_sync[stream][0]
            updated_record = update_object(stream, record["id"])
            updated_records[stream].append(updated_record)
            expected_records_second_sync[stream].append({"id": updated_record['id']})

        # Ensure different times between udpates and inserts
        sleep(2)

        # Insert (create) one record for each stream prior to 2nd sync
        for stream in self.streams_to_create:
            created_record = create_object(stream)
            self.new_objects[stream].append(created_record)
            created_records[stream].append(created_record)
            expected_records_second_sync[stream].append({"id": created_record['id']})

        # ensure validity of expected_records_second_sync
        for stream in self.streams_to_create:
            if stream in self.expected_incremental_streams():
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

            # created_records[stream] = self.records_data_type_conversions(created_records.get(stream))
            # updated_records[stream] = self.records_data_type_conversions(updated_records.get(stream))


        # Run a second sync job using orchestrator
        second_sync_start = self.local_to_utc(dt.utcnow())
        second_sync_record_count = self.run_sync(conn_id)
        second_sync_end = self.local_to_utc(dt.utcnow())

        second_sync_state = menagerie.get_state(conn_id)

        # Get the set of records from a second sync
        second_sync_records = runner.get_records_from_target_output()
        second_sync_created, second_sync_updated = self.split_records_into_created_and_updated(second_sync_records)

        # Loop first_sync_records and compare against second_sync_records
        for stream in self.streams_to_create.difference(untested_streams):
            with self.subTest(stream=stream):

                second_sync_data = [record.get("data") for record
                                    in second_sync_records.get(stream, {}).get("messages", [])]
                stream_replication_keys = self.expected_replication_keys()
                stream_primary_keys = self.expected_primary_keys()

                # TESTING INCREMENTAL STREAMS
                if stream in self.expected_incremental_streams():

                    replication_keys = stream_replication_keys.get(stream)

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

                    # BUG https://stitchdata.atlassian.net/browse/SUP-1316
                    if stream in self.streams_to_create and not stream.startswith('invoice'):
                        for replication_key in replication_keys:
                            updates_replication_key = "updates_" + replication_key
                            updates_stream = stream + "_events"

                            # Verify second sync's bookmarks move past the first sync's
                            self.assertGreater(
                                second_sync_state.get('bookmarks', {updates_stream: {}}).get(
                                    updates_stream, {replication_key: -1}).get(updates_replication_key),
                                first_sync_state.get('bookmarks', {updates_stream: {}}).get(
                                    updates_stream, {updates_replication_key: -1}).get(updates_replication_key)
                            )

                            # Verify that all data of the 2nd sync is >= the bookmark from the first sync
                            first_sync_bookmark = dt.fromtimestamp(
                                first_sync_state.get('bookmarks').get(updates_stream).get(updates_replication_key)
                            )
                            for record in second_sync_data:
                                date_value = record["updated"]
                                self.assertGreaterEqual(date_value,
                                                        dt.strftime(first_sync_bookmark, self.COMPARISON_FORMAT),
                                                        msg="A 2nd sync record has a replication-key that is less than or equal to the 1st sync bookmark.")

                elif stream in self.expected_full_table_streams():
                    raise Exception("Expectations changed, but this test was not updated to reflect them.")

                # TESTING APPLICABLE TO ALL STREAMS

                # Verify that the expected records are replicated in the 2nd sync
                # For incremental streams we should see at least 2 records (a new record and an updated record)
                # but we may see more as the bookmmark is inclusive and there are hidden creates/updates due to
                # dependencies between streams.
                # For full table streams we should see 1 more record than the first sync
                expected_records = expected_records_second_sync.get(stream)
                primary_keys = stream_primary_keys.get(stream)

                updated_pk_values = {tuple([record.get(pk) for pk in primary_keys])
                                     for record in updated_records[stream]}
                self.assertLessEqual(
                    len(expected_records), len(second_sync_data),
                    msg="Expected number of records are not less than or equal to actual for 2nd sync.\n" +
                    "Expected: {}\nActual: {}".format(len(expected_records), len(second_sync_data))
                )
                if (len(second_sync_data) - len(expected_records)) > 0:
                    logging.warn('Second sync replicated %s records more than our create and update for %s',
                                 len(second_sync_data), stream)

                if not primary_keys:
                    raise NotImplementedError("PKs are needed for comparing records")

                # Verify that the inserted and updated records are replicated by the 2nd sync
                for expected_record in expected_records:
                    expected_pk_value = expected_record.get('id')
                    sync_pk_values = [sync_record.get('id')
                                      for sync_record in second_sync_data
                                      if sync_record.get('id') == expected_pk_value]
                    self.assertTrue(
                        len(sync_pk_values) > 0,
                        msg="A record is missing from our sync: \nSTREAM: {}\tPK: {}".format(stream, expected_pk_value)
                    )
                    self.assertIn(expected_pk_value, sync_pk_values)

                # TODO verify updated fields are replicated as expected
