"""
Test tap pagination of streams
"""
import time
from tap_tester import menagerie, runner, connections, LOGGER
from base import BaseTapTest
from utils import create_object, update_object, \
    delete_object, list_all_object, get_catalogs, get_schema


class PaginationTest(BaseTapTest):
    """ Test the tap pagination to get multiple pages of data """

    @staticmethod
    def name():
        return "tt_stripe_pagination"

    def test_run(self):
        """
        Verify that for each stream you can get multiple pages of data
        and that when all fields are selected more than the automatic fields are replicated.

        PREREQUISITE
        For EACH stream add enough data that you surpass the limit of a single
        fetch of data.  For instance if you have a limit of 250 records ensure
        that 251 (or more) records have been posted for that stream.
        """
        conn_id = connections.ensure_connection(self)
        self.conn_id = conn_id

        incremental_streams = {key for key, value in self.expected_replication_method().items()
                               if value == self.INCREMENTAL}
        # We cannot determine if the child stream is
        # returning a page of data due to the duplicacy in the data due to normal parent records
        # as well as event updates of the parents. And hence the ticket: https://jira.talendforge.org/browse/TDL-10005
        # is a blocker. Hence skipping the child streams from this test.
        direct_streams = self.child_streams().union({
            # Data is generated automatically for 'balance_transactions' when 'charges' is created
            'balance_transactions',
            # 'charges',
            # 'coupons',
            # 'customers',
            # 'disputes',
            # 'invoice_items',
            # Data is generated automatically for 'invoice_line_items' when 'invoice_items' is created
            'invoice_line_items',
            # 'invoices',
            'payout_transactions',
            # 'payouts',
            # 'plans',
            # 'products',
            # 'subscription_items',
            # 'subscriptions',
            # 'transfers',
        })
        tested_streams = incremental_streams.difference(direct_streams)

        # Select all streams and all fields within streams
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        our_catalogs = get_catalogs(conn_id, incremental_streams)
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)

        # Ensure tested streams have a record count which exceeds the API LIMIT
        LOGGER.info("Checking record counts for tested streams...")
        streams_to_create = {}
        for stream in tested_streams:
            records = list_all_object(stream)
            record_count = len(records)
            # To not append the Streams having record_count >100 which don't results in index out of range error for "new_objects[stream][0].keys()"
            if record_count <= self.API_LIMIT:
                streams_to_create[stream] = record_count
                LOGGER.info("Stream %s has %s records created today", stream, record_count)

        LOGGER.info("Creating records for tested streams...")
        new_objects = {stream: [] for stream in streams_to_create}
        for stream in streams_to_create:
            if stream != "events" and streams_to_create[stream] <= self.API_LIMIT:
                while streams_to_create[stream] <= self.API_LIMIT:
                    LOGGER.info("Creating a record for %s | %s records created today ",
                                stream, streams_to_create[stream])
                    new_objects[stream].append(create_object(stream))
                    streams_to_create[stream] += 1
                records = list_all_object(stream)
                self.assertEqual(100, len(records))
                LOGGER.info("Stream %s has at least %s records created today", stream, len(records) + 1)

        # Run a sync job using orchestrator
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        actual_fields_by_stream = runner.examine_target_output_for_fields()
        stream_primary_keys = self.expected_primary_keys()

        for stream in tested_streams:
            with self.subTest(stream=stream):

                # verify that we can paginate with all fields selected
                self.assertGreater(
                    record_count_by_stream.get(stream, -1),
                    self.expected_metadata().get(stream, {}).get(self.API_LIMIT, 0),
                    msg="The number of records is not over the stream max limit")

                # verify that the automatic fields are sent to the target
                actual = actual_fields_by_stream.get(stream) or set()
                expected = self.expected_automatic_fields().get(stream, set())
                self.assertTrue(actual.issuperset(expected),
                                msg="The fields sent to the target don't include all automatic fields. "
                                "Expected: {}, Actual: {}". format(expected, actual)
                )

                # verify we have more fields sent to the target than just automatic fields
                # SKIP THIS ASSERTION IF ALL FIELDS ARE INTENTIONALLY AUTOMATIC FOR THIS STREAM
                actual = actual_fields_by_stream.get(stream) or set()
                expected = self.expected_automatic_fields().get(stream, set())
                self.assertTrue(actual.symmetric_difference(expected),
                                msg="The fields sent to the target don't include any non-automatic fields"
                )

                actual_record_message = synced_records.get(stream).get('messages')

                # Primary keys list of the actual stream records which would have `updated_by_event_type` as None
                non_events_primary_keys_list = [tuple([message.get('data').get(expected_pk) for expected_pk in stream_primary_keys[stream]])
                                        for message in actual_record_message
                                        if message.get('action') == 'upsert' and not message.get('data').get('updated_by_event_type', None)]


                primary_keys_list_1 = non_events_primary_keys_list[:self.API_LIMIT]
                primary_keys_list_2 = non_events_primary_keys_list[self.API_LIMIT:2*self.API_LIMIT]

                # Verify by primary keys that data is unique for page
                self.assertTrue(
                    set(primary_keys_list_1).isdisjoint(set(primary_keys_list_2)))

                # Verify we did not duplicate any records across pages
                self.assertCountEqual(set(non_events_primary_keys_list), non_events_primary_keys_list,
                                      msg=f"We have duplicate records for {stream}")

                # updated condition here because for some streams Data is being generated directly when create call for Parent stream is held
                if stream != "events" and stream in streams_to_create:
                    actual = actual_fields_by_stream.get(stream, set())
                    expected = set(new_objects[stream][0].keys())
                    # TODO uncomment when feature is added (https://stitchdata.atlassian.net/browse/SRCE-2466)
                    # verify the target receives all possible fields for a given stream
                    # self.assertEqual(
                    #    actual, expected, msg="The fields sent to the target have an extra or missing field"
                    # )

                # Primary keys list of the event based stream records which would have `updated_by_event_type` as a string
                events_based_primary_keys_list =[tuple([message.get('data').get(expected_pk) for expected_pk in stream_primary_keys[stream]])
                                        for message in actual_record_message
                                        if message.get('action') == 'upsert' and message.get('data').get('updated_by_event_type', None)]

                primary_keys_list_1 = events_based_primary_keys_list[:self.API_LIMIT]
                primary_keys_list_2 = events_based_primary_keys_list[self.API_LIMIT:2*self.API_LIMIT]

                # Verify by primary keys that data is unique for page
                self.assertTrue(
                    set(primary_keys_list_1).isdisjoint(set(primary_keys_list_2)))

                # Verify we did not duplicate any records across pages
                self.assertCountEqual(set(events_based_primary_keys_list), events_based_primary_keys_list,
                                      msg=f"We have duplicate records for {stream}")
