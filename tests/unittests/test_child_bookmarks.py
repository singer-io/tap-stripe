import unittest
from unittest import mock
from tap_stripe import Context, sync, stripe, dt_to_epoch, utils

class TestParentChildBookmarking(unittest.TestCase):
    @mock.patch('tap_stripe.is_parent_selected', return_value=False)
    @mock.patch('tap_stripe.paginate')
    @mock.patch('tap_stripe.Context.is_sub_stream', return_value=[True])
    @mock.patch('tap_stripe.singer.write_schema')
    @mock.patch('tap_stripe.Context.is_selected', return_value=[True])
    @mock.patch('tap_stripe.metadata.to_map')
    @mock.patch('tap_stripe.sync_event_updates')
    @mock.patch('tap_stripe.Context.get_catalog_entry')
    @mock.patch('tap_stripe.utils.now')
    def test_child_bookmarking(self, mock_now, mock_get_catalog_entry, mock_event_updates, mock_to_map, mock_is_selected, mock_write_schema, mock_is_sub_stream, mock_paginate, mock_is_parent_selected):
        '''
            Verify that the paginate function is called with the child stream bookmark
        '''
        # mocked now time
        now_time = utils.strptime_with_tz('2022-01-31 16:17:40.948019+00:00')
        mock_now.return_value = now_time
        # catalog passed in the context
        Context.catalog = {'streams': [{'tap_stream_id': 'invoice_line_items', 'schema': {}, 'key_properties': [], 'metadata': []}]}
        mock_get_catalog_entry.return_value = {'tap_stream_id': 'invoices', 'schema': {}, 'key_properties': [], 'metadata': [{"valid-replication-keys": ["created"]}]}
        # metadata.to_map return value
        mock_to_map.return_value = {(): {'table-key-properties': ['id'], 'selected': True, 'forced-replication-method': 'INCREMENTAL', 'valid-replication-keys': ['created']}}
        invoice_line_items_ts = 1641137533 # 02-01-2022T03:32:13Z
        Context.state = {"bookmarks": {"invoices": {"date": 1641137533}, "invoice_line_items": {"date": invoice_line_items_ts}}}
        sync()
        stop_window = dt_to_epoch(now_time)
        # Verify that the paginate function is called with the child stream bookmark
        mock_paginate.assert_called_with(stripe.Invoice, 'created', invoice_line_items_ts, stop_window, 'invoices', None)

    @mock.patch('tap_stripe.is_parent_selected', return_value=False)
    @mock.patch('tap_stripe.paginate')
    @mock.patch('tap_stripe.Context.is_sub_stream', return_value=[True])
    @mock.patch('tap_stripe.singer.write_schema')
    @mock.patch('tap_stripe.Context.is_selected', return_value=[True])
    @mock.patch('tap_stripe.metadata.to_map')
    @mock.patch('tap_stripe.Context.get_catalog_entry')
    @mock.patch('tap_stripe.utils.now')
    @mock.patch('tap_stripe.sync_sub_stream')
    @mock.patch('tap_stripe.sync_stream')
    @mock.patch('tap_stripe.sync_event_updates')
    def test_sync_event_updates_when_events_bookmark_present(self, mock_sync_event_updates, mock_sync_stream, mock_sync_sub_stream, mock_now, mock_get_catalog_entry, mock_to_map, mock_is_selected, mock_write_schema, mock_is_sub_stream, mock_paginate, mock_is_parent_selected):
        '''
            Verify that event_updates is called for child stream.
        '''
        # mocked now time
        now_time = utils.strptime_with_tz('2022-01-31 16:17:40.948019+00:00')
        mock_now.return_value = now_time
        # catalog passed in the context
        Context.catalog = {'streams': [{'tap_stream_id': 'invoice_line_items', 'schema': {}, 'key_properties': [], 'metadata': []}]}
        mock_get_catalog_entry.return_value = {'tap_stream_id': 'invoices', 'schema': {}, 'key_properties': [], 'metadata': [{"valid-replication-keys": ["created"]}]}
        # metadata.to_map return value
        mock_to_map.return_value = {(): {'table-key-properties': ['id'], 'selected': True, 'forced-replication-method': 'INCREMENTAL', 'valid-replication-keys': ['created']}}
        invoice_line_items_ts = 1641137533 # 02-01-2022T03:32:13Z
        events_ts = 1645716195
        Context.state = {"bookmarks": {"invoice_line_items_events": {"updates_created": events_ts}, "invoice_line_items": {"date": invoice_line_items_ts}}}
        sync()
        # Verify that the sync_event_updates function is called with the is_subStream parameter True
        mock_sync_event_updates.assert_called_with('invoice_line_items', [True])
