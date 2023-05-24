import unittest
from parameterized import parameterized
from unittest import mock
import datetime
from tap_stripe import Context, sync_event_updates, write_bookmark_for_event_updates

MOCK_DATE_TIME = datetime.datetime.strptime("2021-01-01T08:30:50Z", "%Y-%m-%dT%H:%M:%SZ")
MOCK_CURRENT_TIME = datetime.datetime.strptime("2022-04-01T08:30:50Z", "%Y-%m-%dT%H:%M:%SZ")


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
    @mock.patch('singer.utils.now', return_value = datetime.datetime.strptime("2023-05-10T08:30:50Z", "%Y-%m-%dT%H:%M:%SZ"))
    def test_sync_event_updates_bookmark_before_last_30_days(self, mock_utils_now, mock_stripe_event):
        """
        Test that sync_event_updates throws the exception if bookmark value is older than 30 days
        """
        config = {"client_secret": "test_secret", "account_id": "test_account", "start_date": "2022-02-17T00:00:00"}
        Context.config = config
        Context.state = {'bookmarks': {'charges_events': {'updates_created': 1675251000}}}
        mock_stripe_event.return_value = ""
        with self.assertRaises(Exception) as e:
            sync_event_updates('charges', False)

    @mock.patch('stripe.Event.list')
    @mock.patch('singer.utils.now', return_value = datetime.datetime.strptime("2023-05-10T08:30:50Z", "%Y-%m-%dT%H:%M:%SZ"))
    def test_sync_event_updates_bookmark_before_last_30_days(self, mock_utils_now, mock_stripe_event):
        """
        Test that sync_event_updates throws the exception if bookmark value is older than 30 days
        """
        config = {"client_secret": "test_secret", "account_id": "test_account", "start_date": "2022-02-17T00:00:00"}
        Context.config = config
        Context.state = {"bookmarks": {"charges_events": {"updates_created": 1675251000}}}
        mock_stripe_event.return_value = ""
        with self.assertRaises(Exception) as e:
            sync_event_updates("charges", False)

    @mock.patch('stripe.Event.list')
    @mock.patch('singer.utils.now', return_value = datetime.datetime.strptime("2023-05-10T08:30:50Z", "%Y-%m-%dT%H:%M:%SZ"))
    @mock.patch('tap_stripe.Context.is_selected', return_value= True)
    def test_sync_event_updates_bookmark_before_last_30_days_for_two_streams(self, mock_is_selected, mock_utils_now, mock_stripe_event):
        """
        Test that sync_event_updates throws the exception if bookmark value is older than 30 days, testing for 2 streams
        """
        config = {"client_secret": "test_secret", "account_id": "test_account", "start_date": "2022-02-17T00:00:00"}
        Context.config = config
        Context.state = {"bookmarks": {"subscriptions_events": {"updates_created": 1675251000, "subscription_items_events": {"updates_created": 1675251000}}}}
        mock_stripe_event.return_value = ""
        with self.assertRaises(Exception) as e:
            sync_event_updates("subscriptions", False)

    @mock.patch('stripe.Event.list')
    @mock.patch('singer.utils.now', return_value = datetime.datetime.strptime("2023-05-15T08:30:50Z", "%Y-%m-%dT%H:%M:%SZ"))
    @mock.patch('tap_stripe.reset_bookmark_for_event_updates')
    def test_sync_event_updates_bookmark_call_count(self, mock_reset_func, mock_utils_now, mock_stripe_event):
        """
        Test that sync_event_updates resets the state if bookmark value is older than 30 days
        """
        config = {"client_secret": "test_secret", "account_id": "test_account", "start_date": "2022-02-17T00:00:00"}
        Context.config = config
        Context.state = {"bookmarks": {"charges_events": {"updates_created": 1675251000}}}
        mock_stripe_event.return_value = ""
        with self.assertRaises(Exception) as e:
            sync_event_updates("charges", False)
        self.assertEquals(mock_reset_func.call_count, 1)

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