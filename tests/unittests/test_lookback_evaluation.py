import unittest
from unittest import mock
from tap_stripe import evaluate_start_time_based_on_lookback, IMMUTABLE_STREAM_LOOKBACK, Context, utils

class TestLookbackEvaluation(unittest.TestCase):

    def test_lookback_evaluation_for_no_bookmark(self):
        config = { "client_secret": "test_secret", "account_id": "test_account", "start_date": "2022-03-30T00:00:00"}
        Context.config = config
        Context.state = {}
        start_window = evaluate_start_time_based_on_lookback('events', 'created')
        self.assertEqual(start_window, utils.strptime_to_utc(Context.config['start_date']).timestamp())

    def test_lookback_evaluation_when_not_recent_bookmark(self):
        stream_name = "events"
        replication_key = "created"
        state = {'bookmarks': {'events': {'created': 1648739554}}} # 2022-03-31T08:42:34
        config = { "client_secret": "test_secret", "account_id": "test_account", "start_date": "2022-03-30T00:00:00"}
        Context.config = config
        Context.state = state
        start_window = evaluate_start_time_based_on_lookback(stream_name, replication_key)
        self.assertEqual(start_window, Context.state['bookmarks'][stream_name][replication_key])

    @mock.patch("tap_stripe.dt_to_epoch")
    def test_lookback_evaluation_when_recent_bookmark(self, mock_now):
        stream_name = "events"
        replication_key = "created"
        now_time = 1648739354
        mock_now.return_value = now_time # 2022-03-31T08:39:34
        state = {'bookmarks': {'events': {'created': 1648739554}}} # 2022-03-31T08:42:34
        config = { "client_secret": "test_secret", "account_id": "test_account", "start_date": "2022-03-30T00:00:00"}
        Context.config = config
        Context.state = state
        start_window = evaluate_start_time_based_on_lookback(stream_name, replication_key)
        self.assertEqual(start_window, now_time - IMMUTABLE_STREAM_LOOKBACK)
