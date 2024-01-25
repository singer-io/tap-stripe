import unittest
from unittest import mock
from datetime import datetime
from tap_stripe import IMMUTABLE_STREAM_LOOKBACK, Context, sync_stream


class MockClass():
    '''The mock class for the Balance Transactions/events object.'''
    lines = "lines"

    def __init__(self):
        return None

    @classmethod
    def to_dict_recursive(cls):
        '''The mocked to_dict_recursive method of the Balance Transactions/Events class.'''
        return "Test Data"


BOOKMARK_TIME = 1645046000  # epoch bookmark time
BOOKMARK_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


@mock.patch("singer.write_record")
@mock.patch('singer.utils.now', return_value=datetime.strptime("2022-01-01T08:30:50Z", BOOKMARK_FORMAT))
@mock.patch("tap_stripe.reduce_foreign_keys", return_value={"created": 16452804585})
@mock.patch("tap_stripe.paginate", return_value=[MockClass()])
@mock.patch("tap_stripe.Context.get_catalog_entry")
@mock.patch("tap_stripe.singer.metadata.to_map")
@mock.patch("tap_stripe.singer.metadata.get", return_value=["created"])
@mock.patch("tap_stripe.epoch_to_dt")
@mock.patch("tap_stripe.dt_to_epoch", side_effect=[1645056000, 1645056000, 1647647700, 1645056000])  # epoch timestamps
@mock.patch("tap_stripe.sync_sub_stream")
@mock.patch("tap_stripe.singer.get_bookmark", side_effect=[BOOKMARK_TIME, BOOKMARK_TIME])
class TestLookbackWindow(unittest.TestCase):

    def test_default_value_lookback(
            self, mock_get_bookmark_for_stream, mock_sync_substream, mock_dt_to_epoch, mock_epoch_to_dt, mock_get,
            mock_metadata_map, mock_get_catalog_entry, mock_paginate,
            mock_reduce_foreign_keys, mock_utils_now, mock_write_record):
        '''Verify that the lookback window is 600 by default if nothing is provided in the config.'''
        config = {"client_secret": "test_secret", "account_id": "test_account", "start_date": "2022-02-17T00:00:00"}
        Context.config = config
        Context.new_counts['balance_transactions'] = 1
        sync_stream("balance_transactions")
        # expected start_date should be the bookmark time - `lookback`(default lookback)
        expected_start_window = BOOKMARK_TIME - IMMUTABLE_STREAM_LOOKBACK
        mock_epoch_to_dt.assert_called_with(expected_start_window)

    def test_config_provided_value_lookback(
            self, mock_get_bookmark_for_stream, mock_sync_substream, mock_dt_to_epoch, mock_epoch_to_dt, mock_get,
            mock_metadata_map, mock_get_catalog_entry, mock_paginate,
            mock_reduce_foreign_keys, mock_utils_now, mock_write_record):
        '''Verify that the lookback window is correctly passed when mentioned in the config.'''
        config = {"client_secret": "test_secret", "account_id": "test_account",
                  "start_date": "2022-02-17T00:00:00", "lookback_window": 300}
        Context.config = config
        Context.new_counts['balance_transactions'] = 1
        sync_stream("balance_transactions")
        # expected start_date should be the bookmark time - `lookback`(lookback passed in the config)
        expected_start_window = BOOKMARK_TIME - config.get('lookback_window')
        mock_epoch_to_dt.assert_called_with(expected_start_window)

    def test_empty_string_in_config_lookback(
            self, mock_get_bookmark_for_stream, mock_sync_substream, mock_dt_to_epoch, mock_epoch_to_dt, mock_get,
            mock_metadata_map, mock_get_catalog_entry, mock_paginate,
            mock_reduce_foreign_keys, mock_utils_now, mock_write_record):
        '''Verify that the lookback window is correctly passed when empty string is passed in the config.'''
        config = {"client_secret": "test_secret", "account_id": "test_account",
                  "start_date": "2022-02-17T00:00:00", "lookback_window": ''}
        Context.config = config
        Context.new_counts['balance_transactions'] = 1
        sync_stream("balance_transactions")
        # expected start_date should be the bookmark time - `lookback`(default lookback)
        expected_start_window = BOOKMARK_TIME - IMMUTABLE_STREAM_LOOKBACK
        mock_epoch_to_dt.assert_called_with(expected_start_window)

    def test_default_value_lookback_events(
            self, mock_get_bookmark_for_stream, mock_sync_substream, mock_dt_to_epoch, mock_epoch_to_dt, mock_get,
            mock_metadata_map, mock_get_catalog_entry, mock_paginate,
            mock_reduce_foreign_keys, mock_utils_now, mock_write_record):
        '''Verify that the lookback window is 600 by default if nothing is provided in the config.'''
        config = {"client_secret": "test_secret", "account_id": "test_account", "start_date": "2022-02-17T00:00:00"}
        Context.config = config
        Context.new_counts['events'] = 1
        sync_stream("events")
        # expected start_date should be the bookmark time - `lookback`(default lookback)
        expected_start_window = BOOKMARK_TIME - IMMUTABLE_STREAM_LOOKBACK
        mock_epoch_to_dt.assert_called_with(expected_start_window)

    def test_config_provided_value_lookback_events(
            self, mock_get_bookmark_for_stream, mock_sync_substream, mock_dt_to_epoch, mock_epoch_to_dt, mock_get,
            mock_metadata_map, mock_get_catalog_entry, mock_paginate,
            mock_reduce_foreign_keys, mock_utils_now, mock_write_record):
        '''Verify that the lookback window is correctly passed when mentioned in the config.'''
        config = {"client_secret": "test_secret", "account_id": "test_account",
                  "start_date": "2022-02-17T00:00:00", "lookback_window": 300}
        Context.config = config
        Context.new_counts['events'] = 1
        sync_stream("events")
        # expected start_date should be the bookmark time - `lookback`(lookback passed in the config)
        expected_start_window = BOOKMARK_TIME - config.get('lookback_window')
        mock_epoch_to_dt.assert_called_with(expected_start_window)

    def test_empty_string_in_config_lookback_events(
            self, mock_get_bookmark_for_stream, mock_sync_substream, mock_dt_to_epoch, mock_epoch_to_dt, mock_get,
            mock_metadata_map, mock_get_catalog_entry, mock_paginate,
            mock_reduce_foreign_keys, mock_utils_now, mock_write_record):
        '''Verify that the lookback window is correctly passed when empty string is passed in the config.'''
        config = {"client_secret": "test_secret", "account_id": "test_account",
                  "start_date": "2022-02-17T00:00:00", "lookback_window": ''}
        Context.config = config
        Context.new_counts['events'] = 1
        sync_stream("events")
        # expected start_date should be the bookmark time - `lookback`(default lookback)
        expected_start_window = BOOKMARK_TIME - IMMUTABLE_STREAM_LOOKBACK
        mock_epoch_to_dt.assert_called_with(expected_start_window)

    def test_invalid_value_string_in_config_lookback_events(
            self, mock_get_bookmark_for_stream, mock_sync_substream, mock_dt_to_epoch, mock_epoch_to_dt, mock_get,
            mock_metadata_map, mock_get_catalog_entry, mock_paginate,
            mock_reduce_foreign_keys, mock_utils_now, mock_write_record):
        '''Verify that the lookback window is correctly passed when empty string is passed in the config.'''
        config = {"client_secret": "test_secret", "account_id": "test_account",
                  "start_date": "2022-02-17T00:00:00", "lookback_window": 'abc'}
        Context.config = config
        Context.new_counts['events'] = 1
        # Check if the tap raises error
        with self.assertRaises(ValueError) as e:
            sync_stream("events")
        # Check if the error message returned is proper
        self.assertEqual(str(e.exception), "Please provide a valid integer value for the lookback_window parameter.")

    def test_invalid_value_string_in_config_lookback_balance_transactions(
            self, mock_get_bookmark_for_stream, mock_sync_substream, mock_dt_to_epoch, mock_epoch_to_dt, mock_get,
            mock_metadata_map, mock_get_catalog_entry, mock_paginate,
            mock_reduce_foreign_keys, mock_utils_now, mock_write_record):
        '''Verify that the lookback window is correctly passed when empty string is passed in the config.'''
        config = {"client_secret": "test_secret", "account_id": "test_account",
                  "start_date": "2022-02-17T00:00:00", "lookback_window": 'abc'}
        Context.config = config
        Context.new_counts['balance_transactions'] = 1
        # Check if the tap raises error
        with self.assertRaises(ValueError) as e:
            sync_stream("balance_transactions")
        # Check if the error message returned is proper
        self.assertEqual(str(e.exception), "Please provide a valid integer value for the lookback_window parameter.")

    def test_0_in_config_lookback_events(
            self, mock_get_bookmark_for_stream, mock_sync_substream, mock_dt_to_epoch, mock_epoch_to_dt, mock_get,
            mock_metadata_map, mock_get_catalog_entry, mock_paginate,
            mock_reduce_foreign_keys, mock_utils_now, mock_write_record):
        '''Verify that the lookback window is correctly passed when empty string is passed in the config.'''
        config = {"client_secret": "test_secret", "account_id": "test_account",
                  "start_date": "2022-02-17T00:00:00", "lookback_window": 0}
        Context.config = config
        Context.new_counts['events'] = 1
        sync_stream("events")
        # expected start_date should be the bookmark time - `lookback`(default lookback)
        expected_start_window = BOOKMARK_TIME - config.get('lookback_window')
        mock_epoch_to_dt.assert_called_with(expected_start_window)

    def test_0_in_config_lookback_balance_transactions(
            self, mock_get_bookmark_for_stream, mock_sync_substream, mock_dt_to_epoch, mock_epoch_to_dt, mock_get,
            mock_metadata_map, mock_get_catalog_entry, mock_paginate,
            mock_reduce_foreign_keys, mock_utils_now, mock_write_record):
        '''Verify that the lookback window is correctly passed when empty string is passed in the config.'''
        config = {"client_secret": "test_secret", "account_id": "test_account",
                  "start_date": "2022-02-17T00:00:00", "lookback_window": 0}
        Context.config = config
        Context.new_counts['balance_transactions'] = 1
        sync_stream("balance_transactions")
        # expected start_date should be the bookmark time - `lookback`(default lookback)
        expected_start_window = BOOKMARK_TIME - config.get('lookback_window')
        mock_epoch_to_dt.assert_called_with(expected_start_window)

    def test_string_0_in_config_lookback_balance_transactions(
            self, mock_get_bookmark_for_stream, mock_sync_substream, mock_dt_to_epoch, mock_epoch_to_dt, mock_get,
            mock_metadata_map, mock_get_catalog_entry, mock_paginate,
            mock_reduce_foreign_keys, mock_utils_now, mock_write_record):
        '''Verify that the lookback window is correctly passed when '0' is passed in the config.'''
        config = {"client_secret": "test_secret", "account_id": "test_account",
                  "start_date": "2022-02-17T00:00:00", "lookback_window": "0"}
        Context.config = config
        Context.new_counts['balance_transactions'] = 1
        sync_stream("balance_transactions")
        # expected start_date should be the bookmark time - `lookback`(default lookback)
        expected_start_window = BOOKMARK_TIME - int(config.get('lookback_window'))
        mock_epoch_to_dt.assert_called_with(expected_start_window)

    def test_string_0_in_config_lookback_events(
            self, mock_get_bookmark_for_stream, mock_sync_substream, mock_dt_to_epoch, mock_epoch_to_dt, mock_get,
            mock_metadata_map, mock_get_catalog_entry, mock_paginate,
            mock_reduce_foreign_keys, mock_utils_now, mock_write_record):
        '''Verify that the lookback window is correctly passed when '0' is passed in the config.'''
        config = {"client_secret": "test_secret", "account_id": "test_account",
                  "start_date": "2022-02-17T00:00:00", "lookback_window": "0"}
        Context.config = config
        Context.new_counts['events'] = 1
        sync_stream("events")
        # expected start_date should be the bookmark time - `lookback`(default lookback)
        expected_start_window = BOOKMARK_TIME - int(config.get('lookback_window'))
        mock_epoch_to_dt.assert_called_with(expected_start_window)
