import unittest
from unittest import mock
from tap_stripe import IMMUTABLE_STREAM_LOOKBACK, Context, sync_stream, datetime

class MockClass():
    lines = "lines"
    def __init__(self):
        return None
    @classmethod
    def to_dict_recursive(cls):
        return "Test Data"

start_date_before_lookback = 1645056000 # epoch start_date time

@mock.patch("tap_stripe.reduce_foreign_keys", return_value = {"date": 16452804585})
@mock.patch("tap_stripe.convert_dict_to_stripe_object", return_value = {"date": "2022-02-17T00:00:00"})
@mock.patch("tap_stripe.paginate", return_value = [MockClass()])
@mock.patch("tap_stripe.Context.get_catalog_entry")
@mock.patch("tap_stripe.singer.metadata.to_map")
@mock.patch("tap_stripe.singer.metadata.get", return_value = ["date"])
@mock.patch("tap_stripe.epoch_to_dt")
@mock.patch("tap_stripe.dt_to_epoch", side_effect = [1645205225, 1647647700]) # epoch timestamp
@mock.patch("tap_stripe.sync_sub_stream")
@mock.patch("tap_stripe.get_bookmark_for_stream", return_value = start_date_before_lookback)
class TestLookbackWindow(unittest.TestCase):

    def test_default_value_lookback(self, mock_get_bookmark_for_stream, mock_sync_substream, mock_dt_to_epoch, mock_epoch_to_dt, mock_get, mock_metadata_map, mock_get_catalog_entry, mock_paginate, mock_convert_dict_to_stripe_object, mock_reduce_foreign_keys):
        '''Verify that the lookback window is 600 by default if nothing is provided in the config.'''
        config = { "client_secret": "test_secret", "account_id": "test_account", "start_date": "test_start_date"}
        Context.config = config
        Context.new_counts['balance_transactions'] = 1
        sync_stream("balance_transactions")
        # expected start_date should be the `start_date` - `lookback`(default lookback)
        expected_start_window = start_date_before_lookback - IMMUTABLE_STREAM_LOOKBACK
        mock_epoch_to_dt.assert_called_with(expected_start_window)

    def test_config_provided_value_lookback(self, mock_get_bookmark_for_stream, mock_sync_substream, mock_dt_to_epoch, mock_epoch_to_dt, mock_get, mock_metadata_map, mock_get_catalog_entry, mock_paginate, mock_convert_dict_to_stripe_object, mock_reduce_foreign_keys):
        '''Verify that the lookback window is correctly passed when mentioned in the config.'''
        config = { "client_secret": "test_secret", "account_id": "test_account", "start_date": "test_start_date", "lookback_window": 300}
        Context.config = config
        Context.new_counts['balance_transactions'] = 1
        sync_stream("balance_transactions")
        # expected start_date should be the `start_date` - `lookback`(lookback passed in the config)
        expected_start_window = start_date_before_lookback - config.get('lookback_window')
        mock_epoch_to_dt.assert_called_with(expected_start_window)