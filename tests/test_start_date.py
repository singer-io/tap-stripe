"""
Test that the start_date configuration is respected
"""

from functools import reduce
from datetime import datetime as dt
from datetime import timedelta
from dateutil.parser import parse

from tap_tester import menagerie, runner
from tap_tester.scenario import SCENARIOS

from base import BaseTapTest
from utils import create_object, update_object, delete_object, get_catalogs


class StartDateTest(BaseTapTest):
    """
    Test that the start_date configuration is respected

    • verify that a sync with a later start date has at least one record synced
      and less records than the 1st sync with a previous start date
    • verify that each stream has less records than the earlier start date sync
    • verify all data from later start data has bookmark values >= start_date
    • verify that the minimum bookmark sent to the target for the later start_date sync
      is greater than or equal to the start date
    """

    def name(self):
        return "tap_tester_tap_stripe_start_date_test"

    def do_test(self, conn_id):
        """Test we get a lot of data back based on the start date configured in base"""

        # Select all streams and all fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        incremental_streams = {key for key, value in self.expected_replication_method().items()
                               if value == self.INCREMENTAL}

        # IF THERE ARE STREAMS THAT SHOULD NOT BE TESTED
        # REPLACE THE EMPTY SET BELOW WITH THOSE STREAMS
        untested_streams = self.child_streams().union({
            'disputes',
            'events',
            'transfers',
            'payout_transactions'
        })
        our_catalogs = get_catalogs(conn_id, incremental_streams.difference(untested_streams))

        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)

        # Create a record for each stream under test prior to the first sync
        new_objects = {
            stream: create_object(stream)
            for stream in incremental_streams.difference(untested_streams)
        }

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_sync(conn_id)
        first_total_records = reduce(lambda a, b: a + b, first_sync_record_count.values())

        # Count actual rows synced
        first_sync_records = runner.get_records_from_target_output()

        # set the start date for a new connection based off bookmarks largest value
        first_max_bookmarks = self.max_bookmarks_by_stream(first_sync_records)

        bookmark_list = [next(iter(book.values())) for stream, book in first_max_bookmarks.items()]
        bookmark_dates = []
        for bookmark in bookmark_list:
            try:
                bookmark_dates.append(parse(bookmark))
            except (ValueError, OverflowError, TypeError):
                pass

        if not bookmark_dates:
            # THERE WERE NO BOOKMARKS THAT ARE DATES.
            # REMOVE CODE TO FIND A START DATE AND ENTER ONE MANUALLY
            raise ValueError

        # largest_bookmark = reduce(lambda a, b: a if a > b else b, bookmark_dates)
        # self.start_date = self.local_to_utc(largest_bookmark).strftime(self.START_DATE_FORMAT)

        self.start_date = dt.strftime(dt.today() - timedelta(days=1), self.START_DATE_FORMAT)

        # create a new connection with the new start_date
        conn_id = self.create_connection(original_properties=False)

        # Select all streams and all fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in incremental_streams.difference(
                            untested_streams)]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)

        # TODO remove the updates, this is unnecessary. Verify with Harvest
        # Update a record for each stream under test prior to the 2nd sync
        first_sync_created, _ = self.split_records_into_created_and_updated(first_sync_records)
        updated = {}  # holds id for updated objects in each stream
        for stream in new_objects:
            # There needs to be some test data for each stream, otherwise this will break
            record = first_sync_created[stream]["messages"][0]["data"]
            update_object(stream, record["id"])
            updated[stream] = record["id"]
        
        # Run a sync job using orchestrator
        second_sync_record_count = self.run_sync(conn_id)

        # tap-stripe uses events for updates, so these need filtered to validate bookmark
        second_sync_records = runner.get_records_from_target_output()
        second_sync_created, second_sync_updated = self.split_records_into_created_and_updated(second_sync_records)
        second_total_records = reduce(lambda a, b: a + b, second_sync_record_count.values(), 0)

        # Only examine bookmarks for "created" objects, not updates
        second_min_bookmarks = self.min_bookmarks_by_stream(second_sync_created)

        # verify that at least one record synced and less records synced than the 1st connection
        self.assertGreater(second_total_records, 0)
        self.assertLess(first_total_records, second_total_records)

        # validate that all newly created records are greater than the start_date
        for stream in incremental_streams.difference(untested_streams):
            with self.subTest(stream=stream):

                # verify that each stream has less records in the first sync than the second
                self.assertGreater(
                    second_sync_record_count.get(stream, 0),
                    first_sync_record_count.get(stream, 0),
                    msg="first had more records, start_date usage not verified")

                # verify all data from 2nd sync >= start_date
                target_mark = second_min_bookmarks.get(stream, {"mark": None})
                target_value = next(iter(target_mark.values()))  # there should be only one

                if target_value:

                    # it's okay if there isn't target data for a stream
                    try:
                        target_value = self.local_to_utc(parse(target_value))
                        expected_value = self.local_to_utc(parse(self.start_date))
                        # verify that the minimum bookmark sent to the target for the second sync
                        # is greater than or equal to the start date
                        self.assertGreaterEqual(target_value, expected_value)

                    except (OverflowError, ValueError, TypeError):
                        print("bookmarks cannot be converted to dates, "
                              "can't test start_date for {}".format(stream))

                if stream in updated:
                    delete_object(stream, updated[stream])


SCENARIOS.add(StartDateTest)
