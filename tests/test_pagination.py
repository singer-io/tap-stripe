"""
Test tap pagination of streams
"""
import logging

from tap_tester import menagerie, runner
from tap_tester.scenario import SCENARIOS
from base import BaseTapTest
from util_stripe import create_object, update_object, \
    delete_object, list_all_object, get_catalogs, get_schema


class PaginationTest(BaseTapTest):
    """ Test the tap pagination to get multiple pages of data """

    def name(self):
        return "tap_tester_tap_stripe_pagination_test"

    def do_test(self, conn_id):
        """
        Verify that for each stream you can get multiple pages of data
        and that when all fields are selected more than the automatic fields are replicated.

        PREREQUISITE
        For EACH stream add enough data that you surpass the limit of a single
        fetch of data.  For instance if you have a limit of 250 records ensure
        that 251 (or more) records have been posted for that stream.
        """
        incremental_streams = {key for key, value in self.expected_replication_method().items()
                               if value == self.INCREMENTAL}
        untested_streams = self.child_streams().union({
            'balance_transactions',
            # 'charges',
            # 'coupons',
            # 'customers',
            'disputes',
            # 'invoice_items',
            'invoice_line_items',
            # 'invoices',
            'payout_transactions',
            # 'payouts',
            # 'plans',
            # 'products',
            'subscription_items',
            # 'subscriptions',
            'transfers',
        })
        tested_streams = incremental_streams.difference(untested_streams)
        
        # Select all streams and all fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        our_catalogs = get_catalogs(conn_id, tested_streams)
        # our_catalogs = [catalog for catalog in found_catalogs if
        #                 catalog.get('tap_stream_id') in tested_streams]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)

        # Ensure tested streams have a record count which exceeds the API LIMIT
        logging.info("Checking record counts for tested streams...")
        streams_to_create = {}
        for stream in tested_streams:
            records = list_all_object(stream)
            record_count = len(records['data'])

            streams_to_create[stream] = record_count
            logging.info("   Stream {} has {} records created today".format(stream, record_count))

        logging.info("Creating records for tested streams...")
        new_objects = {stream: [] for stream in streams_to_create}
        for stream in streams_to_create:
            if stream != "events" and streams_to_create[stream] <= self.API_LIMIT:
                while streams_to_create[stream] <= self.API_LIMIT:
                    logging.info("Creating a record for {} | {} records created today ".format(stream,
                                                                                        streams_to_create[stream]))
                    new_objects[stream].append(create_object(stream))
                    streams_to_create[stream] += 1
                records = list_all_object(stream)
                self.assertEqual(100, len(records['data']))
                self.assertTrue(records['has_more'])
                logging.info("   Stream {} has {} records created today".format(stream, len(records['data']) + 1))

        # Run a sync job using orchestrator
        record_count_by_stream = self.run_sync(conn_id)

        actual_fields_by_stream = runner.examine_target_output_for_fields()

        for stream in incremental_streams.difference(untested_streams):
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

                if stream != "events":
                    actual = actual_fields_by_stream.get(stream, set())
                    expected = set(new_objects[stream][0].keys())
                    # TODO uncomment when feature is added (https://stitchdata.atlassian.net/browse/SRCE-2466)
                    # verify the target recieves all possible fields for a given stream
                    # self.assertEqual(
                    #    actual, expected, msg="The fields sent to the target have an extra or missing field"
                    # )

                    ##########################################################################
                    ### Clean up records iff there are multiple pages of records
                    ##########################################################################

                    # logging.info("Ensuring record count does not exceed multiple pages")
                    # records = list_all_object(stream)
                    # count = 1
                    # while records['has_more']:
                    #     for record in records['data']:
                    #         delete_object(stream, record['id'])
                    #         logging.info("Deleting records for stream {} | {} records remaining".format(stream, count))
                    #         count += 1
                    #     records = list_all_object(stream)


SCENARIOS.add(PaginationTest)
