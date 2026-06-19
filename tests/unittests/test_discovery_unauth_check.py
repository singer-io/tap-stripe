import unittest
from unittest.mock import patch

from tap_stripe import (
    _apply_access_checks,
    _prune_inaccessible_children,
    PARENT_STREAM_MAP,
    STREAM_SDK_OBJECTS,
    TapPermissionError,
    discover,
)


class TestPruneInaccessibleChildren(unittest.TestCase):
    def test_removes_child_when_parent_missing(self):
        schemas = {
            'invoices': {'schema': {}},
            'invoice_line_items': {'schema': {}},
            'payouts': {'schema': {}},
            'payout_transactions': {'schema': {}},
        }

        schemas.pop('invoices')

        _prune_inaccessible_children(schemas)

        self.assertNotIn('invoice_line_items', schemas)
        self.assertIn('payout_transactions', schemas)


class TestApplyAccessChecks(unittest.TestCase):
    @patch('tap_stripe._check_stream_access', return_value=True)
    def test_all_parent_streams_accessible_no_change(self, mock_check):
        schemas = {
            'charges': {'schema': {}},
            'invoices': {'schema': {}},
            'invoice_line_items': {'schema': {}},
        }

        _apply_access_checks(schemas)

        self.assertIn('charges', schemas)
        self.assertIn('invoices', schemas)
        self.assertIn('invoice_line_items', schemas)
        checked_streams = [call.args[0] for call in mock_check.call_args_list]
        self.assertNotIn('invoice_line_items', checked_streams)

    @patch('tap_stripe._check_stream_access')
    def test_inaccessible_parent_is_removed_and_child_pruned(self, mock_check):
        def access_by_stream(stream_name, _stream_map):
            return stream_name != 'invoices'

        mock_check.side_effect = access_by_stream

        schemas = {
            'charges': {'schema': {}},
            'invoices': {'schema': {}},
            'invoice_line_items': {'schema': {}},
        }

        _apply_access_checks(schemas)

        self.assertIn('charges', schemas)
        self.assertNotIn('invoices', schemas)
        self.assertNotIn('invoice_line_items', schemas)

    @patch('tap_stripe._check_stream_access', return_value=False)
    def test_raises_when_all_parents_inaccessible(self, _mock_check):
        schemas = {stream_name: {'schema': {}} for stream_name in set(PARENT_STREAM_MAP.values())}

        with self.assertRaises(TapPermissionError):
            _apply_access_checks(schemas)

    @patch('tap_stripe._check_stream_access')
    def test_customers_inaccessible_only_removes_customers(self, mock_check):
        def access_by_stream(stream_name, _stream_map):
            return stream_name != 'customers'

        mock_check.side_effect = access_by_stream

        schemas = {stream_name: {'schema': {}} for stream_name in STREAM_SDK_OBJECTS}

        _apply_access_checks(schemas)

        self.assertNotIn('customers', schemas)
        expected_count = len(STREAM_SDK_OBJECTS) - 1
        self.assertEqual(len(schemas), expected_count)


class TestDiscoverUnauthorizedFiltering(unittest.TestCase):
    @patch('tap_stripe._check_stream_access')
    def test_discover_excludes_unauthorized_parent_and_child(self, mock_check):
        def access_by_stream(stream_name, _stream_map):
            return stream_name != 'invoices'

        mock_check.side_effect = access_by_stream

        catalog = discover()
        stream_names = {stream['tap_stream_id'] for stream in catalog['streams']}

        self.assertNotIn('invoices', stream_names)
        self.assertNotIn('invoice_line_items', stream_names)
        self.assertIn('charges', stream_names)