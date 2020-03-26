## Instructions to make a tap-tester test

### General
The tap-tester template is the basic set of tests for SaaS type taps.  These tests inlude discovery,
bookmarks, start_date usage, replication methods, pagination, and stream field selection

These tests do not cover specific difficulties of a stream and should be added to when you run into
a situation that is not typical.  For instance, if there is logic for stream selection or field
selection where if you pick stream A you cannot pick stream B. It also does not currently test 
parent child relationships, streams that you can select the replication method, etc.  

These tests are the starting point for tap-tester and not a comprehesive test.  Each tap should be 
reviewed to determine if supplemental testing should be completed 

### How to use this template

In general all you will need to do is fill out specifics for the properties and credentials
for the tap and create appropriate test data in the dev account used for the testing.

If there are situations which a test is not appropriate for a stream you can update the catalogs
for that test.  An example from the bookmarks test is below

        found_catalogs = menagerie.get_catalogs(conn_id)
        incremental_streams = {key for key, value in self.expected_replication_method().items()
                               if value == self.INCREMENTAL}

        # IF THERE ARE STREAMS THAT SHOULD NOT BE TESTED
        # REPLACE THE EMPTY SET BELOW WITH THOSE STREAMS
        untested_streams = self.child_streams().union(set())
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in incremental_streams.difference(
                            untested_streams)]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=False)

make sure that if the test does not have an untested streams section that you use it everywhere
catalogs are selected and in the subTests so you are not testing streams that are not selected.

####base.py

Fill out the the following methods to customize the test

The name of the tap

    def tap_name(self):
        return "tap-<tap name>"

The extension of the URL for the tap

    def get_type(self):
        """Return the expected url route ending"""
        return "platform.<tap-name>"

The configuration properties required for the tap

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            'start_date': '2017-07-01 00:00:00',
            'shop': 'stitchdatawearhouse'
        }

The credentials required if any. These should be in the 
environments repo

    def get_credentials(self):
        return {
            'api_key': os.getenv('TAP_<TAP-NAME>_API_KEY'),
            'password': os.getenv('TAP_<TAP-NAME>_PASSWORD')
        }
        
    def setUp(self):
        """Verify that you have set the prerequisites to run the tap (creds, etc.)"""
        missing_envs = [x for x in [os.getenv('TAP_SHOPIFY_API_KEY')] if x is None]
        if missing_envs:
            raise Exception("set environment variables")

The expected streams and associated metadata to test for.  You can either explicitly put in the
metadata for each stream, or can set default metadata for streams and update exceptions.  Examples
of both methods are below.

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""

        default = {
                self.REPLICATION_KEYS: {"updated_at"},
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.API_LIMIT: 250}

        meta = default.copy()
        meta.update({self.FOREIGN_KEYS: {"owner_id", "owner_resource"}})

        return {
            "orders": default,
            "metafields": meta,
            "transactions": {
                self.REPLICATION_KEYS: {"created_at"},
                self.PRIMARY_KEYS: {"id"},
                self.FOREIGN_KEYS: {"order_id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.API_LIMIT: 250}
        }