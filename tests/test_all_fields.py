"""
Test tap sets a bookmark and respects it for the next sync of a stream
"""
import os
import json
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
    create_object, delete_object, list_all_object


class ALlFieldsTest(BaseTapTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream"""

    @staticmethod
    def name():
        return "tap_tester_tap_stripe_all_fields_test"

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
            # "invoice_line_items", # TODO
            "invoices",
            "payouts",
            "plans",
            "products",
            "subscription_items", # TODO
            "subscriptions",
        }

        cls.expected_objects = {stream: [] for stream in cls.streams_to_test}

        cls.existing_objects = {stream: [] for stream in cls.streams_to_test}
        cls.new_objects = {stream: [] for stream in cls.streams_to_test}

        for stream in cls.streams_to_test:

            # get existing records
            stripe_obj = list_all_object(stream)
            stripe_json = json.dumps(stripe_obj, sort_keys=True, indent=2)
            dict_obj = json.loads(stripe_json)

            cls.existing_objects[stream] = dict_obj['data']

            # create new records if necessary
            stripe_obj = create_object(stream)
            stripe_json = json.dumps(stripe_obj, sort_keys=True, indent=2)
            dict_obj = json.loads(stripe_json)

           cls.new_objects[stream] = [dict_obj]


        # gather expectations based off existing and new data
        for stream in cls.streams_to_test:
            cls.expected_objects[stream] = cls.existing_objects[stream] + cls.new_objects[stream]


    @classmethod
    def tearDownClass(cls):
        logging.info("Start Teardown")
        for stream in cls.streams_to_test:
            for record in cls.new_objects[stream]:
                delete_object(stream, record["id"])


    def getPKsToRecordsDict(self, stream, records):
        """Return dict object of tupled pk values to record"""
        primary_keys = list(self.expected_primary_keys().get(stream))
        pks_to_record_dict = {tuple(record.get(pk) for pk in primary_keys): record for record in records}
        return pks_to_record_dict

    def assertPKsEqual(self, stream, expected_records, sync_records, assert_pk_count_same=False):
        """
        Compare the values of the primary keys for expected and synced records.
        For this comparison to be valid we also check for duplicate primary keys.

        Parameters:
        arg1 (int): Description of arg1
        """
        primary_keys = list(self.expected_primary_keys().get(stream))

        # This stream is dependent on the 'invoice' stream so it technically has two pks
        # but we add one of them, so just use the one pk to make a comparison
        if stream == "invoice_line_items":
            primary_keys = ["id"]

        # Verify there are no duplicate pks in the target
        sync_pks = [tuple(sync_record.get(pk) for pk in primary_keys) for sync_record in sync_records]
        sync_pks_set = set(sync_pks)

        # Verify there are no duplicate pks in our expectations
        expected_pks = [tuple(expected_record.get(pk) for pk in primary_keys) for expected_record in expected_records]
        expected_pks_set = set(expected_pks)


        # Verify sync pks have all expected records pks in it
        self.assertTrue(sync_pks_set.issuperset(expected_pks_set))

        if assert_pk_count_same:
            self.assertEqual(expected_pks_set, sync_pks_set)


    def test_run(self):
        """
        Verify that for each stream you can get data when no fields are selected
        and only the automatic fields are replicated.
        """

        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        streams_to_select = self.streams_to_test
        self.perform_and_verify_table_and_field_selection(
            conn_id, found_catalogs, streams_to_select, select_all_fields=True
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


        # BUG_1 | https://stitchdata.atlassian.net/browse/SRCE-4736
        KNOWN_MISSING_FIELDS = {
            'customers':{
                'tax_ids',
                'cards',
                'discount',
                'subscriptions',
                'sources',
            },
            'subscriptions':{
                'default_tax_rates',
                'pending_update',
                'billing_cycle_anchor',
                'current_period_end',
                'current_period_start',
                'discount',
                'items',
                'plan',
                'start',
                'start_date',
            },
            'products':{
                'skus',
                'package_dimensions',

            },
            'invoice_items':{
                'price',
                'date',
                'period',
                'unit_amount_decimal',
                'unit_amount',
            },
            'payouts':{
                'application_fee',
                'reversals',
                'reversed',
                'arrival_date',
                'date',
                'status',
            },
            'charges':{
                'refunds',
                'source',
            },
            'subscription_items':{
                'tax_rates',
                'price',
                'plan',
                'subscription',
            },
            'invoices':{
                'payment_settings',
                'on_behalf_of',
                'custom_fields',
                'created',
                'date',
                'lines',
                'period_end',
                'period_start',
                'webhooks_delivered_at',
            },
            'plans':{
                'transform_usage',
            },
            'invoice_line_items':{
                'unique_id',
                'tax_rates',
                'price',
                'amount',
                'currency',
                'description',
                'discount_amounts',
                'discountable',
                'discounts',
                'id',
                'invoice_item',
                'livemode',
                'metadata',
                'object',
                'period',
                'plan',
                'proration',
                'quantity',
                'subscription',
                'tax_amounts',
                'type',
            },
        }

        FIELDS_ADDED_BY_TAP = {'updated'}

        # Test by Stream
        for stream in self.streams_to_test:
            with self.subTest(stream=stream):

                # set expectattions
                expected_records = self.records_data_type_conversions(
                    self.expected_objects[stream]
                )
                expected_records_keys = set()
                for record in expected_records:
                    expected_records_keys.update(set(record.keys()))

                # collect actual values
                actual_records = synced_records.get(stream)
                actual_records_keys = set()
                for message in actual_records['messages']: 
                    if message['action'] == 'upsert':
                        actual_records_keys.update(set(message['data'].keys()))
                schema_keys = set(self.expected_schema_keys(stream)) # read in from schema files


                # Verify schema covers all fields
                adjusted_expected_keys = expected_records_keys.union(FIELDS_ADDED_BY_TAP)
                adjusted_actual_keys = actual_records_keys.union(KNOWN_MISSING_FIELDS.get(stream, set()))  # BUG_1
                self.assertSetEqual(adjusted_expected_keys, adjusted_actual_keys)
                # self.assertTrue(adjusted_actual_keys.issubset(adjusted_expected_keys))


                # Log the fields that are included in the schema but not in the expectations.
                # These are fields we should strive to get data for in our test data set
                if schema_keys.difference(adjusted_expected_keys):
                    print("WARNING Stream: [{}] Fields missing from expectations: [{}]".format(
                        stream, schema_keys.difference(adjusted_expected_keys))
                    )


                # Verify that all fields sent to the target fall into the expected schema
                self.assertTrue(actual_records_keys.issubset(schema_keys))


                # actual_records = [row['data'] for row in data['messages']]

                # # Verify by pks, that we replicated the expected records and only the expected records
                # self.assertPKsEqual(stream, expected_records, actual_records)

                # expected_pks_to_record_dict = self.getPKsToRecordsDict(stream, expected_records)
                # actual_pks_to_record_dict = self.getPKsToRecordsDict(stream, actual_records)

                # for pks_tuple, expected_record in expected_pks_to_record_dict.items():
                #     actual_record = actual_pks_to_record_dict.get(pks_tuple) or {}
                #     for field in fields_to_skip_schema_assertion.get(stream, {}):# BUG_1
                #         if field in expected_record.keys():
                #             expected_record.pop(field)
                #         if field in actual_record.keys():
                #             actual_record.pop(field)
                #     self.assertDictEqual(expected_record, actual_record)
