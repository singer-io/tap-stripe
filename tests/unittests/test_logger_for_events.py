import unittest
from unittest import mock
from datetime import datetime
from tap_stripe import Context, sync_stream

class MockClass():
    '''The mock class for the Balance Transactions/events object.'''
    lines = "lines"
    def __init__(self):
        return None

    @classmethod
    def to_dict_recursive(cls):
        '''The mocked to_dict_recursive method of the Balance Transactions/Events class.'''
        return "Test Data"

BOOKMARK_TIME = 1645046000 # epoch bookmark time
BOOKMARK_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

@mock.patch("tap_stripe.LOGGER.warning")
@mock.patch("singer.write_record")
@mock.patch('singer.utils.now', return_value = datetime.strptime("2022-05-01T08:30:50Z", BOOKMARK_FORMAT))
@mock.patch("tap_stripe.reduce_foreign_keys", return_value = {"created": 16452804585})
@mock.patch("tap_stripe.paginate", return_value = [MockClass()])
@mock.patch("tap_stripe.Context.get_catalog_entry")
@mock.patch("tap_stripe.singer.metadata.to_map")
@mock.patch("tap_stripe.singer.metadata.get", return_value = ["created"])
@mock.patch("tap_stripe.epoch_to_dt")
@mock.patch("tap_stripe.dt_to_epoch", side_effect = [1645056000, 1645056000, 1647647700, 1645056000]) # epoch timestamps
@mock.patch("tap_stripe.sync_sub_stream")
@mock.patch("tap_stripe.singer.get_bookmark", side_effect = [BOOKMARK_TIME, BOOKMARK_TIME])
class TestLoggerWarningForEvents(unittest.TestCase):

    def test_date_window_logger(self, mock_get_bookmark_for_stream, mock_sync_substream, mock_dt_to_epoch, mock_epoch_to_dt, mock_get, mock_metadata_map,
                                mock_get_catalog_entry, mock_paginate, mock_reduce_foreign_keys,
                                mock_utils_now, mock_write_record, mock_logger):
        """
        Test that tap prints expected warning message when bookmark value of before 30 days is passed in the state.
        """
        config = {"client_secret": "test_secret", "account_id": "test_account", "start_date": "2022-02-17T00:00:00", "lookback_window": "0"}
        Context.config = config
        Context.state = {}
        Context.new_counts['events'] = 1
        sync_stream("events")

        expected_logger_warning = [
            mock.call("Provided start_date or current bookmark for newly created event records is older than 30 days."),
            mock.call("The Stripe Event API returns data for the last 30 days only. So, syncing event data from 30 days only.")
        ]
        # Verify warning message for bookmark of less than last 30 days.
        self.assertEqual(mock_logger.mock_calls, expected_logger_warning)
