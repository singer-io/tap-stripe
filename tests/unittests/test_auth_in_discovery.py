from unittest import mock
from tap_stripe import stripe, Context
import tap_stripe
import unittest
import requests
import json

# Mock args
class Args():
    def __init__(self):
        self.discover = True
        self.catalog = False
        self.config = {"client_secret": "test_secret", "account_id": "test_account", "start_date": "test_start_date"}
        self.state = False

# Mock response
def get_mock_http_response(status_code, content={}):
    contents = json.dumps(content)
    response = requests.Response()
    response.status_code = status_code
    response.headers = {}
    response._content = contents.encode()
    return response

@mock.patch('tap_stripe.utils.parse_args')
class TestBasicAuthInDiscoverMode(unittest.TestCase):
    @mock.patch('tap_stripe.discover')
    @mock.patch('stripe.http_client.requests.Session.request')
    def test_basic_auth_no_access_401(self, mock_request, mocked_discover, mocked_args):
        '''
            Verify exception is raised for no access(401) error code for authentication through sdk
            and discover is called zero times for setup Context.
        '''
        mock_request.return_value = get_mock_http_response(401, {'error': {'message': 'Invalid API Key provided: test_secret', 'type': 'invalid_request_error'}})
        mocked_args.return_value = Args()
        try:
            tap_stripe.main()
        except stripe.error.AuthenticationError as e:
            expected_error_message = 'Invalid API Key provided: test_secret'
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
        # Verify that the discover is not called  when incorrect credentials are passed
        self.assertEqual(mocked_discover.call_count, 0)

    @mock.patch('tap_stripe.discover', return_value = {})
    @mock.patch('stripe.http_client.requests.Session.request')
    def test_basic_auth_access_200(self, mock_retrieve, mocked_discover, mocked_args):
        '''
            Verify discover mode is called if credentials are valid by setting up the client and calling the sdk function
            and discover function is called once for setup Context and discover mode.
        '''
        mock_retrieve.return_value = get_mock_http_response(200, {"settings":{"dashboard": {"display_name": "Stitch"}}})
        mocked_args.return_value = Args()
        tap_stripe.main()
        # Verify that the discover is called once when correct credentials are passed
        self.assertEqual(mocked_discover.call_count, 1)