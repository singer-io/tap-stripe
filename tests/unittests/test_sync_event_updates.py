import unittest
from parameterized import parameterized
from unittest import mock
import datetime
from tap_stripe import Context, sync_event_updates, write_bookmark_for_event_updates

MOCK_DATE_TIME = datetime.datetime.strptime("2021-01-01T08:30:50Z", "%Y-%m-%dT%H:%M:%SZ")
MOCK_CURRENT_TIME = datetime.datetime.strptime("2022-04-01T08:30:50Z", "%Y-%m-%dT%H:%M:%SZ")

class MockContext():
    """
    Mock class of Context
    """
    state = {}

    @classmethod
    def is_selected(self, stream_name):
        return True
    

class TestSyncEventUpdates(unittest.TestCase):
    """
    Verify bookmark logic and logger message of sync_event_updates.
    """
    @mock.patch('stripe.Event.list')
    @mock.patch('singer.utils.now', side_effect = [MOCK_DATE_TIME, MOCK_DATE_TIME, MOCK_DATE_TIME])
    @mock.patch('tap_stripe.write_bookmark_for_event_updates')
    def test_sync_event_updates_bookmark_in_last_7_days(self, mock_write_bookmark, mock_stripe_event, mock_utils_now):
        """
        Test that sync_event_updates write the maximum bookmark value in the state when its value is with in last 
        events_date_window_size(7 days default) days.
        """
        config = {"client_secret": "test_secret", "account_id": "test_account", "start_date": "2022-02-17T00:00:00"}
        Context.config = config
        Context.state = {'bookmarks': {'charges_events': {'created': 1698739554}}}

        mock_stripe_event.return_value = ""
        sync_event_updates('charges', False)

        # Verify that tap writes bookmark/start_date value in the state.
        mock_write_bookmark.assert_called_with(False, 'charges', None, 1645056000)

    @mock.patch('stripe.Event.list')
    @mock.patch('singer.utils.now', side_effect = [MOCK_CURRENT_TIME, MOCK_CURRENT_TIME, MOCK_DATE_TIME])
    @mock.patch('tap_stripe.write_bookmark_for_event_updates')
    @mock.patch('tap_stripe.LOGGER.warning')
    def test_sync_event_updates_bookmark_before_last_7_days(self, mock_logger, mock_write_bookmark, mock_stripe_event, mock_utils_now):
        """
        Test that sync_event_updates write the expected bookmark value(events_date_window_size days less than the current date) in the state when maximum
        bookmark value is before the last events_date_window_size(7 days default) days of current date.
        """
        config = {"client_secret": "test_secret", "account_id": "test_account", "start_date": "2022-02-17T00:00:00"}
        Context.config = config
        Context.state = {'bookmarks': {'charges_events': {'created': 1698739554}}}

        mock_stripe_event.return_value = ""
        sync_event_updates('charges', False)

        # Verify that tap writes maximum of bookmark/start_date value and sync_start_time.
        mock_write_bookmark.assert_called_with(False, 'charges', None, 1648197050)

        expected_logger_warning = [
            mock.call("Provided start_date or current bookmark for event updates is older than 30 days."),
            mock.call("The Stripe Event API returns data for the last 30 days only. So, syncing event data from 30 days only.")
        ]
        # Verify warning message for bookmark of less than last 30 days.
        self.assertEqual(mock_logger.mock_calls, expected_logger_warning)

    @mock.patch("singer.write_state")
    def test_write_bookmark_event_updates_for_non_sub_streams(self, mock_state):
        """
        Test that tap writes expected bookmark for non sub streams.
        """
        Context.state = {'bookmarks': {}}
        write_bookmark_for_event_updates(False, 'charges', None, 1648177250)
        
        # Verify expected bookmark value
        mock_state.assert_called_with({'bookmarks': {'charges_events': {'updates_created': 1648177250}}})

    @mock.patch('tap_stripe.Context', return_value = Context)
    @mock.patch("singer.write_state")
    def test_write_bookmark_event_updates_for_non_sub_streams(self, mock_state, mock_context):
        """
        Test that tap writes expected bookmark for sub streams.
        """
        Context.state = {'bookmarks': {}}
        write_bookmark_for_event_updates(True, 'invoices', 'invoice_line_items', 1648177250)
        
        # Verify expected bookmark value
        mock_state.assert_called_with(mock_context.state)