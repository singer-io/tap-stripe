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

@mock.patch('tap_stripe.ListObject._request')
class DeletedInvoiceLineItem(unittest.TestCase):
    """
        Test cases for verifying we log 'warning' in case of deleted invoice item call
    """

    @mock.patch('tap_stripe.LOGGER.warning')
    def test_deleted_invoice_line_item_API_call(self, mocked_warn, mocked_request):
        """
            Test case for verifying we skip deleted invoice line item API call and log 'warning'
        """

        # mock request and raise 'InvalidRequestError' containing 'No such invoice item' in error message
        mocked_request.side_effect = raise_no_such_invoice_error

        # create 'ListObject' object
        list_obj = ListObject()
        # set dummy url
        list_obj['url'] = 'https://api.stripe.com/'

        # function call
        resp = new_list(list_obj)

        # verify the 'LOGGER.warning' was called with expected message
        mocked_warn.assert_called_with('%s. Currently, skipping this invoice line item call.', 'Request req_test123: No such invoice item: \'ii_test123\'')

    def test_not_found_InvalidRequestError(self, mocked_request):
        """
            Test case for verifying we raise 'InvalidRequestError' not containing 'No such invoice item' in the error message
        """

        # mock request and raise 'InvalidRequestError' containing any error message but not 'No such invoice item'
        mocked_request.side_effect = raise_not_found_error

        # create 'ListObject' object
        list_obj = ListObject()
        # set dummy url
        list_obj['url'] = 'https://api.stripe.com/'

        # verify we raise error when calling 'new_list' funciton
        with self.assertRaises(InvalidRequestError) as e:
            resp = new_list(list_obj)

        # verify error message
        self.assertEqual(str(e.exception), 'Not Found')

    def test_other_than_InvalidRequestError_error(self, mocked_request):
        """
            Test case for verifying we raise any error not containing 'No such invoice item' in the error message
        """

        # mock request and raise 'Exception'
        mocked_request.side_effect = raise_other_error

        # create 'ListObject' object
        list_obj = ListObject()
        # set dummy url
        list_obj['url'] = 'https://api.stripe.com/'

        # verify we raise error when calling 'new_list' funciton
        with self.assertRaises(Exception) as e:
            resp = new_list(list_obj)

        # verify error message
        self.assertEqual(str(e.exception), 'Not Found for URL: https://api.stripe.com/v1/test')
