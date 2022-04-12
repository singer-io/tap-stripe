from unittest import mock
from tap_stripe import new_list
import unittest
from stripe.error import InvalidRequestError
from stripe.api_resources.list_object import ListObject

# raise 'no such invoice item' error
def raise_no_such_invoice_error(*args, **kwargs):
    raise InvalidRequestError('Request req_test123: No such invoice item: \'ii_test123\'', {})

# raise 'not found' error
def raise_not_found_error(*args, **kwargs):
    raise InvalidRequestError('Not Found', {})

# raise other error
def raise_other_error(*args, **kwargs):
    raise Exception('Not Found for URL: https://api.stripe.com/v1/test')

@mock.patch('tap_stripe.ListObject.request')
class DeletedInvoiceLineItem(unittest.TestCase):
    """
        Test cases for verifying we log 'warning' in case of deleted invoice item call
    """

    @mock.patch('tap_stripe.LOGGER.warn')
    def test_deleted_invoice_line_item_API_call(self, mocked_warn, mocked_request):
        """
            Test case for verifying we 
        """

        mocked_request.side_effect = raise_no_such_invoice_error

        list_obj = ListObject()
        list_obj['url'] = 'https://api.stripe.com/'

        resp = new_list(list_obj)

        self.assertEqual(resp, [])
        mocked_warn.assert_called_with('Request req_test123: No such invoice item: \'ii_test123\'. Currently, skipping this invoice line item call.')

    def test_not_found_InvalidRequestError(self, mocked_request):

        mocked_request.side_effect = raise_not_found_error

        list_obj = ListObject()
        list_obj['url'] = 'https://api.stripe.com/'

        with self.assertRaises(InvalidRequestError) as e:
            resp = new_list(list_obj)
        self.assertEqual(str(e.exception), 'Not Found')

    def test_other_than_InvalidRequestError_error(self, mocked_request):

        mocked_request.side_effect = raise_other_error

        list_obj = ListObject()
        list_obj['url'] = 'https://api.stripe.com/'

        with self.assertRaises(Exception) as e:
            resp = new_list(list_obj)
        self.assertEqual(str(e.exception), 'Not Found for URL: https://api.stripe.com/v1/test')
