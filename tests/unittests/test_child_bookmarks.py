import unittest
from unittest import mock
from tap_stripe import Context, sync, stripe, dt_to_epoch, utils, is_parent_selected

class TestParentChildBookmarking(unittest.TestCase):
    @mock.patch('tap_stripe.is_parent_selected', return_value=False)
    @mock.patch('tap_stripe.paginate')
    @mock.patch('tap_stripe.Context.is_sub_stream', return_value=[True])
    @mock.patch('tap_stripe.singer.write_schema')
    @mock.patch('tap_stripe.Context.is_selected', return_value=True)
    @mock.patch('tap_stripe.metadata.to_map')
    @mock.patch('tap_stripe.sync_event_updates')
    @mock.patch('tap_stripe.Context.get_catalog_entry')
    @mock.patch('tap_stripe.utils.now')
    def test_child_bookmarking(self, mock_now, mock_get_catalog_entry,
                               mock_event_updates, mock_to_map, mock_is_selected,
                               mock_write_schema, mock_is_sub_stream,
                               mock_paginate, mock_is_parent_selected):
        '''
            Verify that the paginate function is called with the child stream bookmark
        '''
        # mocked now time
        now_time = utils.strptime_with_tz('2022-02-01 15:32:13.000000+00:00')
        mock_now.return_value = now_time
        # catalog passed in the context
        Context.catalog = {'streams': [{'tap_stream_id': 'invoice_line_items', 'schema': {}, 'key_properties': [], 'metadata': []}]}
        mock_get_catalog_entry.return_value = {'tap_stream_id': 'invoices', 'schema': {}, 'key_properties': [], 'metadata': [{"valid-replication-keys": ["created"]}]}
        # metadata.to_map return value
        mock_to_map.return_value = {(): {'table-key-properties': ['id'], 'selected': True, 'forced-replication-method': 'INCREMENTAL', 'valid-replication-keys': ['created']}}
        invoice_line_items_ts = 1641137533 # 02-01-2022T03:32:13Z
        Context.state = {"bookmarks": {"invoices": {"date": 1641137539}, "invoice_line_items": {"date": invoice_line_items_ts}}}
        sync()
        stop_window = invoice_line_items_ts + (30 * 24 * 60 * 60)
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
    def test_sync_event_updates_when_events_bookmark_present(self, mock_sync_event_updates, mock_sync_stream,
                                                             mock_sync_sub_stream, mock_now, mock_get_catalog_entry,
                                                             mock_to_map, mock_is_selected, mock_write_schema,
                                                             mock_is_sub_stream, mock_paginate, mock_is_parent_selected):
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
        # Verify that the sync_event_updates function is called with the is_sub_stream parameter True
        mock_sync_event_updates.assert_called_with('invoice_line_items', [True])

    @mock.patch('tap_stripe.is_parent_selected', return_value=False)
    @mock.patch('tap_stripe.paginate')
    @mock.patch('tap_stripe.Context.is_sub_stream', return_value=[True])
    @mock.patch('tap_stripe.singer.write_schema')
    @mock.patch('tap_stripe.Context.is_selected', return_value=[True])
    @mock.patch('tap_stripe.metadata.to_map')
    @mock.patch('tap_stripe.Context.get_catalog_entry')
    @mock.patch('tap_stripe.utils.now')
    @mock.patch('tap_stripe.sync_sub_stream')
    @mock.patch('tap_stripe.sync_event_updates')
    @mock.patch('tap_stripe.get_bookmark_for_sub_stream')
    def test_sync_event_updates_when_substream_bookmark_present(self, mock_get_bookmark_for_sub_stream, mock_sync_event_updates,
                                                                mock_sync_sub_stream, mock_now, mock_get_catalog_entry,
                                                                mock_to_map, mock_is_selected, mock_write_schema,
                                                                mock_is_sub_stream, mock_paginate, mock_is_parent_selected):
        '''
            Verify that get_bookmark_for_sub_stream() is called only when the child stream is selected.
        '''
        # mocked now time
        now_time = utils.strptime_with_tz('2022-01-31 16:17:40.948019+00:00')
        mock_get_bookmark_for_sub_stream.return_value = 1641137550
        mock_now.return_value = now_time
        # catalog passed in the context
        Context.catalog = {'streams': [{'tap_stream_id': 'invoice_line_items', 'schema': {}, 'key_properties': [], 'metadata': []}]}
        Context.config = {'start_date': "2019-06-20 16:17:40.948019+00:00"}
        mock_get_catalog_entry.return_value = {'tap_stream_id': 'invoices', 'schema': {}, 'key_properties': [], 'metadata': [{"valid-replication-keys": ["created"]}]}
        # metadata.to_map return value
        mock_to_map.return_value = {(): {'table-key-properties': ['id'], 'selected': True, 'forced-replication-method': 'INCREMENTAL', 'valid-replication-keys': ['created']}}
        invoice_line_items_ts = 1641137533 # 02-01-2022T03:32:13Z
        events_ts = 1645716195
        Context.state = {"bookmarks": {"invoice_line_items_events": {"updates_created": events_ts}, "invoice_line_items": {"date": invoice_line_items_ts}}}
        sync()
        # Verify for substream get_bookmark_for_sub_stream is called with stream name
        mock_get_bookmark_for_sub_stream.assert_called_with("invoice_line_items")
    
    @mock.patch('tap_stripe.reduce_foreign_keys', return_value = {"created": 1561047480})
    @mock.patch('tap_stripe.is_parent_selected', return_value=False)
    @mock.patch('tap_stripe.paginate', return_value = [mock.Mock()])
    @mock.patch('tap_stripe.Context.is_sub_stream', return_value=False)
    @mock.patch('tap_stripe.singer.write_schema')
    @mock.patch('tap_stripe.Context.is_selected', return_value=[True])
    @mock.patch('tap_stripe.metadata.to_map')
    @mock.patch('tap_stripe.Context.get_catalog_entry')
    @mock.patch('tap_stripe.utils.now')
    @mock.patch('tap_stripe.sync_sub_stream')
    @mock.patch('tap_stripe.sync_event_updates')
    @mock.patch('tap_stripe.get_bookmark_for_sub_stream')
    @mock.patch('tap_stripe.singer.write_record')
    def test_sync_event_updates_for_parent_stream(self, mock_write_record, mock_get_bookmark, 
                                                                mock_sync_event_updates, mock_sync_sub_stream, 
                                                                mock_now, mock_get_catalog_entry, 
                                                                mock_to_map, mock_is_selected, mock_write_schema, 
                                                                mock_is_sub_stream, mock_paginate, mock_is_parent_selected, 
                                                                mock_reduce_foreign_keys):
        '''
            Verify that when only the parent stream is selected, write_record() is called for the parent stream.
        '''
        # mocked now time
        now_time = utils.strptime_with_tz('2022-01-31 16:17:40.948019+00:00')
        mock_get_bookmark.return_value = 1641137550
        mock_now.return_value = now_time
        # catalog passed in the context
        Context.catalog = {'streams': [{'tap_stream_id': 'invoices', 'schema': {}, 'key_properties': [], 'metadata': []}]}
        Context.config = {'start_date': "2019-06-20 16:17:40.948019+00:00"}
        mock_get_catalog_entry.return_value = {'tap_stream_id': 'invoices', 'schema': {}, 'key_properties': [], 'metadata': [{"valid-replication-keys": ["created"]}]}
        # metadata.to_map return value
        mock_to_map.return_value = {(): {'table-key-properties': ['id'], 'selected': True, 'forced-replication-method': 'INCREMENTAL', 'valid-replication-keys': ['created']}}
        invoice_line_items_ts = 1641137533 # 02-01-2022T03:32:13Z
        events_ts = 1645716195
        Context.state = {"bookmarks": {"invoice_line_items_events": {"updates_created": events_ts}, "invoice_line_items": {"date": invoice_line_items_ts}}}
        sync()

        # Verify that one record is being written
        self.assertEqual(1, mock_write_record.call_count)

    @mock.patch('tap_stripe.metadata.to_map')
    @mock.patch('tap_stripe.Context.get_catalog_entry')
    def test_is_parent_selected_when_child_is_selected(self, mock_get_catalog_entry, mock_to_map):
        """
            Verify that the is_parent_selected() returns True when the parent stream is also selected.
        """
        mock_get_catalog_entry.return_value = {'tap_stream_id': 'subscriptions', 'schema': {}, 'key_properties': [], 'metadata': [{"valid-replication-keys": ["created"],  "selected": True}]}
        mock_to_map.return_value = {(): {'table-key-properties': ['id'], 'selected': True, 'forced-replication-method': 'INCREMENTAL', 'valid-replication-keys': ['created']}}
        parent_selected = is_parent_selected('subscription_items')
        # verify that the parent_selected returns True
        self.assertTrue(parent_selected)

    @mock.patch('tap_stripe.metadata.to_map')
    @mock.patch('tap_stripe.Context.get_catalog_entry')
    def test_is_parent_selected_when_parent_is_not_selected(self, mock_get_catalog_entry, mock_to_map):
        """
            Verify that the is_parent_selected() returns False when the parent stream is not selected.
        """
        mock_get_catalog_entry.return_value = {'tap_stream_id': 'subscriptions', 'schema': {}, 'key_properties': [], 'metadata': [{"valid-replication-keys": ["created"],  "selected": False}]}
        mock_to_map.return_value = {(): {'table-key-properties': ['id'], 'selected': False, 'forced-replication-method': 'INCREMENTAL', 'valid-replication-keys': ['created']}}
        parent_selected = is_parent_selected('subscription_items')
        # verify that the parent_selected returns False
        self.assertFalse(parent_selected)
