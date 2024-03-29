"""
Test tap gets all records for streams with full replication
"""
import json

from tap_tester import menagerie, runner, connections

from base import BaseTapTest


class FullReplicationTest(BaseTapTest):
    """Test tap gets all records for streams with full replication"""

    @staticmethod
    def name():
        return "tt_stripe_full_table"

    def test_run(self):
        """
        Verify that a bookmark doesn't exist for the stream
        Verify that the second sync includes the same number or more records than the first sync
        Verify that all records in the first sync are included in the second sync
        Verify that the sync only sent records to the target for selected streams (catalogs)

        PREREQUISITE
        For EACH stream that is fully replicated there are multiple rows of data with
            different values for the replication key
        """
        conn_id = connections.ensure_connection(self)
        self.conn_id = conn_id

        # Select all streams and no fields within streams
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        full_streams = {key for key, value in self.expected_replication_method().items()
                        if value == self.FULL}
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in full_streams]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)

        # verify that the sync only sent records to the target for selected streams (catalogs)
        self.assertEqual(set(first_sync_record_count.keys()), full_streams)

        first_sync_state = menagerie.get_state(conn_id)

        # Get the set of records from a first sync
        first_sync_records = runner.get_records_from_target_output()

        # Run a second sync job using orchestrator
        second_sync_record_count = self.run_and_verify_sync(conn_id)

        # Get the set of records from a second sync
        second_sync_records = runner.get_records_from_target_output()

        # THIS MAKES AN ASSUMPTION THAT CHILD STREAMS DO NOT NEED TESTING.
        # ADJUST IF NECESSARY
        for stream in full_streams.difference(self.child_streams()):
            with self.subTest(stream=stream):

                # verify there is no bookmark values from state
                state_value = first_sync_state.get("bookmarks", {}).get(stream)
                self.assertIsNone(state_value)

                # verify that there is more than 1 record of data - setup necessary
                self.assertGreater(first_sync_record_count.get(stream, 0), 1,
                                   msg="Data isn't set up to be able to test full sync")

                # verify that you get the same or more data the 2nd time around
                self.assertGreaterEqual(
                    second_sync_record_count.get(stream, 0),
                    first_sync_record_count.get(stream, 0),
                    msg="second syc didn't have more records, full sync not verified")

                # verify all data from 1st sync included in 2nd sync
                first_data = [record["data"] for record
                              in first_sync_records.get(stream, {}).get("messages", {"data": {}})]
                second_data = [record["data"] for record
                               in second_sync_records.get(stream, {}).get("messages", {"data": {}})]

                same_records = 0
                for first_record in first_data:
                    first_value = json.dumps(first_record, sort_keys=True)

                    for compare_record in second_data:
                        compare_value = json.dumps(compare_record, sort_keys=True)

                        if first_value == compare_value:
                            second_data.remove(compare_record)
                            same_records += 1
                            break

                self.assertEqual(len(first_data), same_records,
                                 msg="Not all data from the first sync was in the second sync")


