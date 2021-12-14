import os
import logging
from pathlib import Path
from random import random
from time import sleep, perf_counter
from datetime import datetime as dt
from dateutil.parser import parse

from collections import namedtuple

from tap_tester import menagerie, runner, connections
from base import BaseTapTest
from utils import \
    create_object, delete_object, list_all_object, stripe_obj_to_dict


# BUG_12478 | https://jira.talendforge.org/browse/TDL-12478
#             Fields that are consistently missing during replication
#             Original Ticket [https://stitchdata.atlassian.net/browse/SRCE-4736]
KNOWN_MISSING_FIELDS = {
    'customers':{
        'tax_ids',
    },
    'subscriptions':{
        'payment_settings',
        'default_tax_rates',
        'pending_update',
        'automatic_tax',
    },
    'products':{
        'skus',
        'tax_code',
    },
    'invoice_items':{
        'price',
    },
    'payouts':{
        'application_fee',
        'reversals',
        'reversed',
    },
    'charges': set(),
    'subscription_items':{
        'tax_rates',
        'price',
        'updated',
    },
    'invoices':{
        'payment_settings',
        'on_behalf_of',
        'custom_fields',
        'automatic_tax',
        'quote',
    },
    'plans': set(),
    'invoice_line_items': {
        'tax_rates',
        'unique_id',
        'updated',
        'price',
    },
}

KNOWN_FAILING_FIELDS = {
    'coupons': {
        'percent_off', # BUG_9720 | Decimal('67') != Decimal('66.6') (value is changing in duplicate records)
    },
    'customers': {
        'discount',  # BUG_12478 | missing subfields in coupon where coupon is subfield within discount
    },
    'subscriptions': {
        # BUG_12478 | missing subfields in coupon where coupon is subfield within discount
        # BUG_12478 | missing subfields on discount ['checkout_session', 'id', 'invoice', 'invoice_item', 'promotion_code']
        'discount',
        # BUG_12478 | missing subfields on plan ['statement_description', 'statement_descriptor', 'name', 'amount_decimal']
        'plan',
    },
    'products': set(),
    'invoice_items': {
        'plan', # BUG_12478 | missing subfields
    },
    'payouts': set(),
    'charges': set(),
    'subscription_items': {
        # BUG_12478 | missing subfields on plan ['statement_description', 'statement_descriptor', 'name']
        # BUG_13711 | https://jira.talendforge.org/browse/TDL-13711
        #             Schema wrong for subfield 'transform_usage', should be object not string
        'plan',
    },
    'invoices': {
        'discount', # BUG_12478 | missing subfields
        'plans', # BUG_12478 | missing subfields
        'finalized_at', # BUG_13711 | schema missing datetime format
        'created', # BUG_13711 | schema missing datetime format
    },
    'plans': {
        'transform_usage' # BUG_13711 schema is wrong, should be an object not string
    },
    # 'invoice_line_items': { # TODO This is a test issue that prevents us from consistently passing
    #     'unique_line_item_id',
    #     'invoice_item',
    # }
}

# NB | The following sets not to be confused with the sets above documenting BUGs.
#      These are testing issues/limitations which we have implemented long-term
#      workarounds for.

# fields with changing values, which make it hard to compare values directly
FICKLE_FIELDS = {
    'coupons': {
        'times_redeemed' # expect 0, get 1
    },
    'customers': set(),
    'subscriptions': set(),
    'products': set(),
    'invoice_items': set(),
    'payouts': {
        'object', # expect 'transfer', get 'payout'
    },
    'charges': {
        'status', # expect 'paid', get 'succeeded'
    },
    'subscription_items': set(),
    'invoices': set(),
    'plans': set(),
    'invoice_line_items': set()
}

FIELDS_ADDED_BY_TAP = {
    'coupons': {'updated'},
    'customers': {'updated'},
    'subscriptions': {'updated'},
    'products': {'updated'},
    'invoice_items': {
        'updated',
        # BUG_13666 | [https://jira.talendforge.org/browse/TDL-13666]
        #             Deterimine what we do when creating records that
        #             cuases the presence of this value to be inconsistent
        'subscription_item',
    },
    'payouts': {'updated'},
    'charges': {'updated'},
    'subscription_items': {'updated'},
    'invoices': {'updated'},
    'plans': {'updated'},
    'invoice_line_items': {
        'updated',
        'invoice',
        'subscription_item'
    },
}


class ALlFieldsTest(BaseTapTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream"""

    @staticmethod
    def name():
        return "tt_stripe_all_fields"

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
        cls.streams_to_test = {
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

        cls.expected_objects = {stream: [] for stream in cls.streams_to_test}

        cls.existing_objects = {stream: [] for stream in cls.streams_to_test}
        cls.new_objects = {stream: [] for stream in cls.streams_to_test}

        for stream in cls.streams_to_test:

            # create new records
            stripe_obj = create_object(stream)
            cls.new_objects[stream] = [stripe_obj_to_dict(stripe_obj)]
            cls.expected_objects[stream] = cls.new_objects[stream]


    @classmethod
    def tearDownClass(cls):
        logging.info("Start Teardown")
        for stream in cls.streams_to_test:
            for record in cls.new_objects[stream]:
                delete_object(stream, record["id"])


    def getPKsToRecordsDict(self, stream, records, duplicates=False):  # BUG_9720
        """Return dict object of tupled pk values to record"""
        primary_keys = list(self.expected_primary_keys().get(stream))

        if not duplicates: # just send back a dictionary comprehension of tupled pks to records
            pks_to_record_dict = {tuple(record.get(pk) for pk in primary_keys): record for record in records}
            return pks_to_record_dict, dict()

        # if duplicates are present we must track them in a separate dictionary
        pks_to_record_dict_1 = dict()
        pks_to_record_dict_2 = dict()
        for record in records:
            primary_key_values = tuple(record.get(pk) for pk in primary_keys)

            if pks_to_record_dict_1.get(primary_key_values):
                pks_to_record_dict_2[primary_key_values] = record
                continue

            pks_to_record_dict_1[primary_key_values] = record
        return pks_to_record_dict_1, pks_to_record_dict_2


    def test_run(self):

        # first just run the test against customers
        streams_to_test_1 = {'customers'}

        # then run against all streams under test (except customers)
        streams_to_test_2 = self.streams_to_test.difference(streams_to_test_1)

        for streams_to_test in ['invoices']:
            with self.subTest(streams_to_test=streams_to_test):

                # get existing records and add them to our expectations
                for stream in streams_to_test:
                    stripe_obj = list_all_object(stream)
                    self.existing_objects[stream] = stripe_obj
                    self.expected_objects[stream] = self.existing_objects[stream]

                # run the test
                self.all_fields_test(streams_to_test)


    def all_fields_test(self, streams_to_test):
        """
        Verify that for each stream data is synced when all fields are selected.

        Verify the synced data matches our expectations based off of the applied schema
        and results from the test client utils.
        """

        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        self.perform_and_verify_table_and_field_selection(
            conn_id, found_catalogs, streams_to_test, select_all_fields=True
        )

        # run initial sync
        first_record_count_by_stream = self.run_and_verify_sync(conn_id)

        replicated_row_count = sum(first_record_count_by_stream.values())
        synced_records = runner.get_records_from_target_output()

        # Verify target has records for all synced streams
        for stream, count in first_record_count_by_stream.items():
            assert stream in self.expected_streams()
            self.assertGreater(count, 0, msg="failed to replicate any data for: {}".format(stream))
        print("total replicated row count: {}".format(replicated_row_count))


        # Test by Stream
        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # set expectattions
                primary_keys = list(self.expected_primary_keys().get(stream))
                expected_records = self.records_data_type_conversions(
                    self.expected_objects[stream]
                )
                expected_records_keys = set()
                for record in expected_records:
                    expected_records_keys.update(set(record.keys()))

                # collect actual values
                actual_records = synced_records.get(stream)
                actual_records_data = [message['data'] for message in actual_records.get('messages')]
                actual_records_keys = set()
                for message in actual_records['messages']:
                    if message['action'] == 'upsert':
                        actual_records_keys.update(set(message['data'].keys()))
                schema_keys = set(self.expected_schema_keys(stream)) # read in from schema files


                # Log the fields that are included in the schema but not in the expectations.
                # These are fields we should strive to get data for in our test data set
                if schema_keys.difference(expected_records_keys):
                    print("WARNING Stream[{}] Fields missing from expectations: [{}]".format(
                        stream, schema_keys.difference(expected_records_keys)
                    ))

                # Verify schema covers all fields
                adjusted_expected_keys = expected_records_keys.union(
                    FIELDS_ADDED_BY_TAP.get(stream, set())
                )
                adjusted_actual_keys = actual_records_keys.union(  # BUG_12478
                    KNOWN_MISSING_FIELDS.get(stream, set())
                )
                if stream == 'invoice_items':
                    adjusted_actual_keys = adjusted_actual_keys.union({'subscription_item'})  # BUG_13666
                self.assertSetEqual(adjusted_expected_keys, adjusted_actual_keys)

                # verify the missing fields from KNOWN_MISSING_FIELDS are always missing (stability check)
                self.assertSetEqual(actual_records_keys.difference(KNOWN_MISSING_FIELDS.get(stream, set())), actual_records_keys)

                # Verify that all fields sent to the target fall into the expected schema
                self.assertTrue(actual_records_keys.issubset(schema_keys))

                # Verify there are no duplicate pks in the target
                actual_pks = [tuple(actual_record.get(pk) for pk in primary_keys) for actual_record in actual_records_data]
                actual_pks_set = set(actual_pks)
                # self.assertEqual(len(actual_pks_set), len(actual_pks))  # BUG_9720
                self.assertLessEqual(len(actual_pks_set), len(actual_pks))

                # Verify there are no duplicate pks in our expectations
                expected_pks = [tuple(expected_record.get(pk) for pk in primary_keys) for expected_record in expected_records]
                expected_pks_set = set(expected_pks)
                self.assertEqual(len(expected_pks_set), len(expected_pks))

                # Verify by pks, that we replicated the expected records
                self.assertTrue(actual_pks_set.issuperset(expected_pks_set))

                # test records by field values...

                expected_pks_to_record_dict, _ = self.getPKsToRecordsDict(stream, expected_records)  # BUG_9720
                actual_pks_to_record_dict, actual_pks_to_record_dict_dupes = self.getPKsToRecordsDict(  # BUG_9720
                    stream, actual_records_data, duplicates=True
                )

                # BUG_9720 | https://jira.talendforge.org/browse/TDL-9720

                # Verify the fields which are replicated, adhere to the expected schemas
                for pks_tuple, expected_record in expected_pks_to_record_dict.items():
                    with self.subTest(record=pks_tuple):

                        actual_record = actual_pks_to_record_dict.get(pks_tuple) or {}

                        # BUG_9720 | uncomment to reproduce a duplicate record with a data discrepancy
                        # actual_record_dupe = actual_pks_to_record_dict_dupes.get(pks_tuple) or {}
                        # if actual_record_dupe != actual_record and \
                        #    actual_record_dupe['created'] == actual_record['created'] and \
                        #    actual_record_dupe['updated'] == actual_record['updated']:
                        #     import pdb; pdb.set_trace()
                        #     print(f"Discrepancy {set(actual_record_dupe.keys()).difference(set(actual_record.keys())))}")
                        #     print("created: {actual_record['created']}")
                        #     print("created dupe: {actual_record_dupe['created']}")
                        #     print("updated: {actual_record['updated']}")
                        #     print("updated dupe: {actual_record_dupe['updated']}")

                        field_adjustment_set = FIELDS_ADDED_BY_TAP[stream].union(
                            KNOWN_MISSING_FIELDS.get(stream, set())  # BUG_12478
                        )

                        # NB | THere are many subtleties in the stripe Data Model.

                        #      We have seen multiple cases where Field A in Stream A has an effect on Field B in Stream B.
                        #      Stripe also appears to run frequent background processes which can result in the update of a
                        #      record between the time when we set our expectations and when we run a sync, therefore
                        #      invalidating our expectations.

                        #      To work around these challenges we will attempt to compare fields directly. If that fails
                        #      we will log the inequality and assert that the datatypes at least match.

                        for field in set(actual_record.keys()).difference(field_adjustment_set):  # skip known bugs
                            with self.subTest(field=field):
                                base_err_msg = f"Stream[{stream}] Record[{pks_tuple}] Field[{field}]"

                                expected_field_value = expected_record.get(field, "EXPECTED IS MISSING FIELD")
                                actual_field_value = actual_record.get(field, "ACTUAL IS MISSING FIELD")

                                try:

                                    self.assertEqual(expected_field_value, actual_field_value)

                                except AssertionError as failure_1:

                                    print(f"WARNING {base_err_msg} failed exact comparison.\n"
                                          f"AssertionError({failure_1})")

                                    if field in KNOWN_FAILING_FIELDS[stream]:
                                        continue # skip the following wokaround

                                    elif actual_field_value and field in FICKLE_FIELDS[stream]:
                                        self.assertIsInstance(actual_field_value, type(expected_field_value))

                                    elif actual_field_value:
                                        raise AssertionError(f"{base_err_msg} Unexpected field is being fickle.")

                                    else:
                                        print(f"WARNING {base_err_msg} failed datatype comparison. Field is None.")
