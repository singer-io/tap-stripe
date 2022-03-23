import stripe
import unittest
from tap_stripe import sync_stream
from unittest import mock
from unittest.mock import Mock



@mock.patch('singer.write_record')
@mock.patch('singer.Transformer.transform')
@mock.patch('tap_stripe.reduce_foreign_keys', return_value={'created': 95454545})
@mock.patch('tap_stripe.convert_dict_to_stripe_object')
@mock.patch('tap_stripe.unwrap_data_objects')
@mock.patch('tap_stripe.get_bookmark_for_stream', return_value=82458458)
@mock.patch('singer.metadata.get', return_value=['created'])
@mock.patch('tap_stripe.Context.is_selected', return_value=True)
@mock.patch('tap_stripe.Context')
@mock.patch('json.loads')
class TestBackOffForDeletedInvoiceLineItems(unittest.TestCase):

    @mock.patch('tap_stripe.paginate')
    @mock.patch('tap_stripe.sync_sub_stream', side_effect=stripe.error.InvalidRequestError('Invoice i1 does not have a line item il_1', {}))
    def test_backoff_for_invoices_line_items(self, mocked_sub_stream, mocked_paginate, mocked_json, mocked_context,
                                             mocked_selected, mocked_metadata, mocked_bookmark, mocked_unwrap_data_objects,
                                             mocked_convert_dict_to_stripe_object, mocked_reduce_foreign_keys,
                                             mocked_transform, mocked_write_record):
        """
            Verify sync_stream method backoff's for 5 times for deleted invoice line items error only
            which contain 'does not have a line item' message
        """
        # Mock Context class and it's methods
        mock_context = Mock()
        mock_context.get_catalog_entry = {'metadata': {}}
        mock_context.config = {}
        mocked_context.return_value = mock_context

        # Mock metadata and it's methods
        mock_data = Mock()
        mock_data.to_dict_recursive.return_value = {'created': 'dummy_date'}
        mocked_paginate.return_value = [mock_data]

        try:
            # Call sync_stream method for `invoices` stream
            sync_stream('invoices')
        except stripe.error.InvalidRequestError:
            pass

        # Verify sync_sub_stream is called with expected times(5 times)
        self.assertEqual(mocked_sub_stream.call_count, 5)

    @mock.patch('tap_stripe.paginate', side_effect=stripe.error.InvalidRequestError('Invoice i1 does not exist', {}))
    def test_backoff_for_all_other_streams(self, mocked_paginate, mocked_json, mocked_context, mocked_selected, mocked_metadata,
                                           mocked_bookmark, mocked_unwrap_data_objects, mocked_convert_dict_to_stripe_object,
                                            mocked_reduce_foreign_keys, mocked_transform, mocked_write_record):
        """
            Verify sync_stream method does not backoff for any other error.
        """
        # Mock Context class and it's methods
        mock_context = Mock()
        mock_context.get_catalog_entry = {'metadata': {}}
        mock_context.config = {}
        mocked_context.return_value = mock_context

        # Mock metadata and it's methods
        mock_data = Mock()
        mock_data.to_dict_recursive.return_value = {'created': 'dummy_date'}
        mocked_paginate.return_value = [mock_data]

        try:
            # Call sync_stream method for `invoices` stream
            sync_stream('invoices')
        except stripe.error.InvalidRequestError:
            pass

        # Verify paginate method called only 1 times
        self.assertEqual(mocked_paginate.call_count, 1)
